# DVP-rapportage (dbt SQL)

In deze map staat de SQL/dbt-laag voor de DVP-rapportage: modellen die telemetrie-events (zoals ze in het DWH binnenkomen) omzetten naar KPI-tabellen.

Kort gezegd: ingestie → staging-tabellen in het DWH → dbt-modellen uit deze repo → (buiten scope) export.

## Wat je hier wel en niet vindt

Wel:

- SQL-modellen met dbt-dependencies (`ref()`), in de gebruikelijke lagen (staging → intermediate → mart).
- Een voorbeeld model van de uiteindelijke XML. Deze is gemaskeerd in de zin dat de daadwerkelijke aantallen niet kloppen, echter de KPI hierarchie blijft wel in stand. 

Niet:

- De component die telemetrie ophaalt en naar het DWH laadt.
- dbt project/profiles/orchestratie (targets, credentials, scheduling) en de export-runner.

Die scheiding is bewust: de transformatielogica is reviewbaar en herbruikbaar zonder platform-specifieke randzaken.

## Waar staat wat?

De modellen staan in [queries_dvp/](queries_dvp/):

- `01_staging/`: opschonen + afleiden van “uitwisselpogingen” uit events
- `02_intermediate/`: verrijking/aggregatie (o.a. gebruiker × maand)
- `03_mart/`: facts/KPI-tabellen + een snapshot-select voor export

Als je zoekt naar definities (grain, KPI’s, aannames, run-volgorde), begin dan bij [queries_dvp/README.md](queries_dvp/README.md).

