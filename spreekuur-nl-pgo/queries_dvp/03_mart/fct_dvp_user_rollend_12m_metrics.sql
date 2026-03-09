{{ config(
    materialized = 'table',
    schema = 's30_modeling',
) }}


-- Gebruikt als date spine om goed mee te kunnen joinen.
with maanden as (
    select distinct sessie_maand as maand
    from {{ ref('int_dvp_user_maand_verrijkt')}}

    -- Expliciet maand toevoegen om te kunnen testen.
    -- union
    -- select date_trunc('month', '2026-01-01'::date) as month
),

-- Parameters bepalen voor de query window gebaseerd op de maanden die in de dataset zitten. De interval bepaald de grootte van de window.

parameters as (
    select
        maand as rapportage_maand
        , (maand - interval '1 month')::date as maand_van_window
        , (maand - interval '13 month')::date as window_start
        , maand as window_eind
    from maanden
)

select
    p.rapportage_maand as "RapportageMaand"
    , p.maand_van_window as "MaandVanWindow"
    , p.window_start as "WindowStartTwaalfMaand"
    , p.window_eind as "WindowEindTwaalfMaand"

    -- KPI 1: Aantal unieke accounts dat heeft ingelogd in het MedMij-portaal.
    , count(distinct umv.gebruiker_id) as "GebruikersAfgelopenJaar"
from {{ ref('int_dvp_user_maand_verrijkt')}} as umv
join parameters as p
      on umv.sessie_maand >= p.window_start
     and umv.sessie_maand <  p.window_eind
where p.rapportage_maand is not null
group by 1,2,3,4
