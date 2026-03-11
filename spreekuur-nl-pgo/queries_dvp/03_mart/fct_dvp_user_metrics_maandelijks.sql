{{ config(
    materialized = 'table',
    schema = 's30_modeling',
) }}

-- Gebruikt als date spine om goed mee te kunnen joinen.
with maanden as (
    select distinct sessie_maand as maand
    from {{ ref('int_dvp_user_maand_verrijkt')}}


    -- Expliciet maand toevoegen om te kunnen testen.
    --union
    --select date_trunc('month', '2026-01-01'::date) as month
)

-- Parameters bepalen voor de query window gebaseerd op de maanden die in de dataset zitten. De interval bepaald de grootte van de window.
, parameters as (
    select
        (maand - interval '1 month')::date as rapportage_maand
        , (maand - interval '1 month')::date as maand_van_window
        , (maand - interval '1 month')::date as window_start
        , maand                     as window_eind
    from maanden
)


-- Max sessies in maand window per gebruiker, wordt gebruikt om gebruikers te categoriseren op gebruiksfrequentie.
, succesvol_max_user_aantal as (
    select
        sessie_maand
        , gebruiker_id
        , max(sessie_aantal_afgelopen_12m) as max_sessies_in_maand_window
    from {{ ref('int_dvp_user_maand_verrijkt') }}
    group by 1, 2
    order by 2
)




-- Finale select statement dat de verschillende KPI's berekend op basis van de gedefinieerde parameters en de verrijkte user maand data.
select
    p.rapportage_maand as "RapportageMaand"
    , p.window_start as "WindowStartMaand"
    , p.window_eind as "WindowEindMaand"

    -- KPI 2: Unieke gebruikers afgelopen maand
    , count(distinct um.gebruiker_id)::bigint as "GebruikersAfgelopenMaand"

    -- KPI 3: Unieke gebruikers per categorie; bezoekers, failed gebruikers, succesvolle gebruikers
    , (count(*) - count(*) filter (where dossieruitwisseling_gestart > 0)) as "AantalBezoekers"
    , count(*) filter (where dossieruitwisseling_gestart > 0 and dossieruitwisseling_succesvol = 0) as "AantalFailed"
    , count(*) filter (where dossieruitwisseling_gestart > 0 and dossieruitwisseling_succesvol > 0) as "AantalSuccesvol"


    -- KPI 4: Unieke succesvolle gebruikers per categorie; inactief, non-frequent gebruiker, frequent gebruiker
    , count(*) filter(
        where dossieruitwisseling_gestart > 0
        and dossieruitwisseling_succesvol > 0
        and login_categorie = '<6 maanden geleden'
    ) as "AantalFrequent"
    , count(*) filter(
        where dossieruitwisseling_gestart > 0
        and dossieruitwisseling_succesvol > 0
        and login_categorie = '6-12 maanden geleden'
    ) as "AantalNonFrequent"
    , count(*) filter(
        where dossieruitwisseling_gestart > 0
        and dossieruitwisseling_succesvol > 0
        and login_categorie = 'Afgelopen 12 maanden niet'
    ) as "AantalInactief"

    -- KPI 6: Gebruiksfrequentie succesvol
    , count(*) filter (
        where dossieruitwisseling_gestart > 0
        and dossieruitwisseling_succesvol > 0
        and max_sessies_in_maand_window = 1
    ) as "AantalEenmaligGebruik"
    , count(*) filter (
        where dossieruitwisseling_gestart > 0
        and dossieruitwisseling_succesvol > 0
        and max_sessies_in_maand_window between 2 and 5
    ) as "AantalTweeTotVijfmaligGebruik"
    , count(*) filter (
        where dossieruitwisseling_gestart > 0
        and dossieruitwisseling_succesvol > 0
        and max_sessies_in_maand_window between 6 and 9
    ) as "AantalZesTotNegenmaligGebruik"
    , count(*) filter (
        where dossieruitwisseling_gestart > 0
        and dossieruitwisseling_succesvol > 0
        and max_sessies_in_maand_window >= 10
    ) as "AantalTienmaligOfMeerGebruik"

    -- KPI 10: Pogingen tot Failed
    , count(dossieruitwisseling_gestart) filter (where dossieruitwisseling_succesvol = 0 and dossieruitwisseling_gestart = 1) as "AantalEenmaligFailed"
    , count(dossieruitwisseling_gestart) filter (where dossieruitwisseling_succesvol = 0 and dossieruitwisseling_gestart between 2 and 5) as "AantalTweeTotVijfmaligFailed"
    , count(dossieruitwisseling_gestart) filter (where dossieruitwisseling_succesvol = 0 and dossieruitwisseling_gestart between 6 and 9) as "AantalZesTotNegenmaligFailed"
    , count(dossieruitwisseling_gestart) filter (where dossieruitwisseling_succesvol = 0 and dossieruitwisseling_gestart >= 10) as "AantalTienmaligOfMeerFailed"


from {{ ref('int_dvp_user_maand_verrijkt')}} as um
left join parameters as p
    on um.sessie_maand >= p.window_start
   and um.sessie_maand <  p.window_eind
left join succesvol_max_user_aantal
    on um.sessie_maand = succesvol_max_user_aantal.sessie_maand
    and um.gebruiker_id = succesvol_max_user_aantal.gebruiker_id
where p.rapportage_maand is not null
group by
    p.rapportage_maand
    , p.window_start
    , p.window_eind
order by
    p.window_start;