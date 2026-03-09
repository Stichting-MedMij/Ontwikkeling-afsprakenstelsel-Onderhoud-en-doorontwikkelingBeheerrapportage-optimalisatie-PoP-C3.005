# DVP-rapportage SQL (queries_dvp)

Hier staan de dbt SQL-modellen voor de DVP-rapportage, in drie lagen:

- `01_staging`: ruwe events opschonen en omzetten naar “uitwisselpogingen”
- `02_intermediate`: verrijking/aggregatie op gebruiker × maand
- `03_mart`: facts/KPI-tabellen en een laatste snapshot voor export

## Databronnen (staging)

De staging bouwt op twee staging-tabellen in het DWH:

- `s20_staging.stg_actions_spreekuur` (acties/events)
- `s20_staging.stg_sessions_spreekuur` (sessies met `user_id`)

Aannames:

- Sessies zonder `user_id` nemen we niet mee.
- Een dossieruitwisseling (poging) leiden we af uit `session_id` + het cumulatieve aantal `verzamelen.flow-start` events binnen die sessie.

## Run-volgorde

In dbt volgt dit vanzelf uit `ref()`, maar logisch gezien is de keten:

1. `stg_dvp_user_uitwissel_pogingen`
2. `int_dvp_user_maand_verrijkt`
3. `fct_dvp_user_rollend_12m_metrics`
4. `fct_dvp_user_metrics_maandelijks`
5. `fct_dvp_uitwissel_metrics_maandelijks`
6. `mtr_dvp_user_metrics_snapshot` (laatste select voor export)

## Modellen (wat komt eruit?)

### `01_staging/stg_dvp_user_uitwissel_pogingen.sql`

Output: `s20_staging.stg_dvp_user_uitwissel_pogingen`.

Grain: 1 rij = 1 dossieruitwisseling (poging) binnen een sessie.

Belangrijke kolommen:

- `dossieruitwisseling_id` (poging-id; cumulatieve start-count × sessie)
- `gebruiker_id`, `sessie_id`, `sessie_timestamp`, `sessie_maand`
- `apparaat_context`: `App` / `Web Mobile` / `Web Desktop` / `Other`
- Funnel flags (0/1), afgeleid uit events:
  - `dossieruitwisseling_gestart` (`verzamelen.flow-start`)
  - `zorgaanbieder_gekozen` (`verzamelen.zorgaanbieder-gekozen`)
  - `gegevensdienst_gekozen` (`verzamelen.gegevensdienst-gekozen`) (optioneel; niet overal downstream nodig)
  - `ingelogd_bij_dva` (`verzamelen.ingelogd-bij-dva`)
  - `dossieruitwisseling_afgerond` (`verzamelen.flow-end`)
  - `dossieruitwisseling_succesvol` (flow-end met resultaat `success`)

### `02_intermediate/int_dvp_user_maand_verrijkt.sql`

Materialization: `ephemeral` (dus geen fysieke tabel; dbt neemt dit inline op in downstream queries).

Input: `stg_dvp_user_uitwissel_pogingen`.

Grain: 1 rij = 1 gebruiker × maand.

Wat gebeurt hier:

- We aggregeren naar gebruiker×maand (`user_sessie_aantal` + tellingen van funnel-flags).
- We bepalen `laatste_inlog_maand` via `lag` en maken daar `login_categorie` van:
  - `Afgelopen 12 maanden niet`
  - `6-12 maanden geleden`
  - `<6 maanden geleden`
- We leiden segment-flags af:
  - `failed_gebruiker`: (inactief/nieuw) én gestart maar 0 success
  - `frequent_gebruiker`: laatste inlog <6m én ≥1 success
  - `succesvolle_gebruiker`: ≥1 success
- We voegen `sessie_aantal_afgelopen_12m` toe (rolling sum over 12 maanden per gebruiker).

### `03_mart/fct_dvp_user_rollend_12m_metrics.sql`

Output: `s30_modeling.fct_dvp_user_rollend_12m_metrics`.

Window: 12 maanden eindigend op `RapportageMaand` (exclusief `RapportageMaand` zelf).

Levert KPI 1: `GebruikersAfgelopenJaar` (aantal unieke gebruikers in de 12-maands window).

### `03_mart/fct_dvp_user_metrics_maandelijks.sql`

Output: `s30_modeling.fct_dvp_user_metrics_maandelijks`.

Window: afgelopen maand.

Levert o.a.:

- KPI 2: `GebruikersAfgelopenMaand`
- KPI 3: `AantalBezoekers`, `AantalFailed`, `AantalSuccesvol`
- KPI 4: succesvol uitgesplitst op `login_categorie` (`AantalFrequent`, `AantalNonFrequent`, `AantalInactief`)
- KPI 6: buckets voor sessies in 12m-window (`AantalEenmaligGebruik` t/m `AantalTienmaligOfMeerGebruik`)
- KPI 10: buckets voor #starts bij failed gebruikers (`AantalEenmaligFailed` t/m `AantalTienmaligOfMeerFailed`)

### `03_mart/fct_dvp_uitwissel_metrics_maandelijks.sql`

Output: `s30_modeling.fct_dvp_uitwissel_metrics_maandelijks`.

Window: afgelopen maand.

Dit model rekent op poging-niveau (input is `stg_dvp_user_uitwissel_pogingen`) en:

- past precedentie toe (als een latere funnelstap gehaald is, tellen eerdere stappen ook als gehaald)
- levert device-verdeling en uitval per funnelstap

KPI’s/kolommen die hieruit komen:

- KPI 7: `AantalApp`, `AantalPC`, `AantalWeb`
- KPI 8: `SuccesvolApp/PC/Web` en `UitvalApp/PC/Web`
- KPI 9: uitval/succes per processtap (zorgaanbieder zoeken, DigiD, uitwisseling)

### `03_mart/mtr_dvp_user_metrics_snapshot.sql`

Dit is een snapshot-select (dus zonder dbt `config` in dit bestand) die de KPI’s in 1 rij samenvoegt voor export.

Output: 1 rij (laatste `RapportageMaand`) via `order by ... desc limit 1`.

Combineert:

- Uit deze keten:
  - `fct_dvp_user_metrics_maandelijks` (KPI 2/3/4/6/10)
  - `fct_dvp_user_rollend_12m_metrics` (KPI 1)
  - `fct_dvp_uitwissel_metrics_maandelijks` (KPI 7/8/9)
- Externe facts (niet in deze map):
  - `s30_modeling.fct_pgo_langdurige_toestemming_aantal` (KPI 5)
  - `s30_modeling.fct_pgo_succesvol_system_aantal` en `...onsuccesvol...` (system component van KPI 7/8)

Voegt daarnaast metadata toe (`Deelnemer`, `Leverancier`, `TijdstempelUitvoering`).

## KPI-overzicht (1 t/m 10)

Samengevat levert de snapshot:

1. `GebruikersAfgelopenJaar` (rolling 12 maanden)
2. `GebruikersAfgelopenMaand`
3. Bezoekers/failed/succesvol (afgelopen maand)
4. Succesvol uitgesplitst op recency (inactief / non-frequent / frequent)
5. Langdurige toestemmingen (externe fact)
6. Gebruiksfrequentie succesvol (buckets op sessies in 12 maanden)
7. Device gebruik (App/PC/Web/System)
8. Succesvol vs uitval per device (App/PC/Web/System)
9. Uitval per processtap (zorgaanbieder zoeken / DigiD / uitwisseling)
10. Pogingen bij failed (buckets op #pogingen in maand)
