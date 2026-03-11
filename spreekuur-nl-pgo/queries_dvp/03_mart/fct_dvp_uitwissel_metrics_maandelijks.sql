{{ config(
    materialized = 'table',
    schema = 's30_modeling',
) }}

-- Date spine om goed mee te kunnen joinen.
with maanden as (
    select distinct sessie_maand as maand
    from {{ ref('stg_dvp_user_uitwissel_pogingen') }}


    -- Expliciet toevoegen van een maand om te kunnen testen.
    -- union
    -- select date_trunc('month', '2026-01-01'::date) as month
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

, flow_precedentie_events as (
 
    -- Aangezien er optionele stappen zijn gebruiken we een precedentie logica. Als een latere stap in de funnel heeft plaatsgevonden zorgt deze logica ervoor
    -- dat we eerdere stappen ook als behaald kunnen markeren.

    select
        dossieruitwisseling_id
        , gebruiker_id
        , sessie_id
        , sessie_maand
        , apparaat_context
        , case
            when dossieruitwisseling_gestart = 0 and (
                zorgaanbieder_gekozen > 0
                or ingelogd_bij_dva > 0
                or dossieruitwisseling_succesvol > 0
            ) then 1
            else dossieruitwisseling_gestart
        end as dossieruitwisseling_gestart

        , case
            when zorgaanbieder_gekozen = 0 and (
                ingelogd_bij_dva > 0
                or dossieruitwisseling_succesvol > 0
            ) then 1
            else zorgaanbieder_gekozen
        end as zorgaanbieder_gekozen

        , case
            when ingelogd_bij_dva = 0 and dossieruitwisseling_succesvol > 0 then 1
            else ingelogd_bij_dva
        end as ingelogd_bij_dva

        , dossieruitwisseling_succesvol
    from {{ ref('stg_dvp_user_uitwissel_pogingen') }}
)

select
    p.rapportage_maand as "RapportageMaand"
    , p.window_start as "WindowStartMaand"
    , p.window_eind as "WindowEindMaand"

    -- KPI 7 - Verdeling uitwisselingen per apparaat_context
    , sum(dossieruitwisseling_gestart) filter (where dossieruitwisseling_gestart > 0) as "AantalUitwisselingpogingenControle"
    , sum(dossieruitwisseling_gestart) filter ( where apparaat_context = 'App' ) as "AantalApp"
    , sum(dossieruitwisseling_gestart) filter ( where apparaat_context = 'Web Mobile' ) as "AantalPC"
    , sum(dossieruitwisseling_gestart) filter ( where apparaat_context = 'Web Desktop' ) as "AantalWeb"

    -- KPI 8 - Verdeling succesvol VS niet succesvol per apparaattype
    , count(*) filter ( where apparaat_context = 'App' and dossieruitwisseling_gestart > 0 and dossieruitwisseling_succesvol > 0) as "SuccesvolApp"
    , count(*) filter ( where apparaat_context = 'Web Mobile' and dossieruitwisseling_gestart > 0 and dossieruitwisseling_succesvol > 0) as "SuccesvolPC"
    , count(*) filter ( where apparaat_context = 'Web Desktop' and dossieruitwisseling_gestart > 0 and dossieruitwisseling_succesvol > 0) as "SuccesvolWeb"
    , count(*) filter ( where apparaat_context = 'App' and dossieruitwisseling_gestart > 0  and dossieruitwisseling_succesvol = 0) as "UitvalApp"
    , count(*) filter ( where apparaat_context = 'Web Mobile' and dossieruitwisseling_gestart > 0 and dossieruitwisseling_succesvol = 0) as "UitvalPC"
    , count(*) filter ( where apparaat_context = 'Web Desktop' and dossieruitwisseling_gestart > 0 and dossieruitwisseling_succesvol = 0) as "UitvalWeb"


    -- KPI 9 - Uitval per processtap
    , count(*) filter ( where dossieruitwisseling_gestart > 0 ) as "SuccesvolDossieruitwisselingGestart"

        -- Zorgaanbiederzoeken
        , count(*) filter (
            where dossieruitwisseling_gestart > 0
            and zorgaanbieder_gekozen > 0
        ) as "SuccesvolZorgaanbiederZoeken"
        , count(*) filter (
            where dossieruitwisseling_gestart > 0
            and zorgaanbieder_gekozen = 0
        ) as "UitvalZorgaanbiederZoeken"

        -- DigID
        , count(*) filter (
            where zorgaanbieder_gekozen > 0
            and ingelogd_bij_dva > 0
        ) as "SuccesvolDigID"
        , count(*) filter (
            where zorgaanbieder_gekozen > 0
            and ingelogd_bij_dva = 0
        ) as "UitvalDigID"

        -- Succesvol Uitwisselen
        , count(*) filter (
            where ingelogd_bij_dva > 0
            and dossieruitwisseling_succesvol > 0
        ) as "SuccesvolUitwisseling"
        , count(*) filter (
            where ingelogd_bij_dva > 0
            and dossieruitwisseling_succesvol = 0
        ) as "UitvalUitwisseling"


from parameters as p
left join flow_precedentie_events as um
    on um.sessie_maand >= p.window_start
   and um.sessie_maand <  p.window_eind
group by
    p.rapportage_maand
    , p.window_start
    , p.window_eind
order by
    p.window_start;
