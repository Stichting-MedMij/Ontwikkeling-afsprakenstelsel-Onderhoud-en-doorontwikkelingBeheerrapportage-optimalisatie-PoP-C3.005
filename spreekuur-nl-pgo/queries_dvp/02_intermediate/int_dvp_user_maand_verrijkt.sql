{{ config(
    materialized = 'ephemeral',
) }}

with user_maand as (
    select
        gebruiker_id
        , sessie_maand
        , count(distinct sessie_id) as user_sessie_aantal

         -- Funnel Events
        , sum(dossieruitwisseling_gestart) as dossieruitwisseling_gestart
        , sum(zorgaanbieder_gekozen) as zorgaanbieder_gekozen
        , sum(gegevensdienst_gekozen) as gegevensdienst_gekozen
        , sum(ingelogd_bij_dva) as ingelogd_bij_dva
        , sum(dossieruitwisseling_afgerond) as dossieruitwisseling_afgerond
        , sum(dossieruitwisseling_succesvol) as dossieruitwisseling_succesvol

        -- Bepalen van de laatste inlog maand per gebruiker om gebruikers te kunnen categoriseren op basis van inloggedrag.
        , lag(sessie_maand) over (
            partition by gebruiker_id
            order by sessie_maand
        ) as laatste_inlog_maand

    from {{ ref('stg_dvp_user_uitwissel_pogingen') }}
    group by gebruiker_id, sessie_maand
)

, user_maand_verrijkt as (
    select
        *

        -- Categoriseren van gebruikers op basis van laatste inlog maand
        , case
            when laatste_inlog_maand is null or laatste_inlog_maand < sessie_maand - interval '12 months' then 'Afgelopen 12 maanden niet'
            when laatste_inlog_maand >= sessie_maand - interval '12 months'
                 and laatste_inlog_maand < sessie_maand - interval '6 months' then '6-12 maanden geleden'
            when laatste_inlog_maand >= sessie_maand - interval '6 months' then '<6 maanden geleden'
            else 'Onbekend'
        end as login_categorie

        -- Gebruikerssegment voor gebruikers die gefaald zijn en niet bekend of nieuw. Wordt gebruikt om later te aggregeren.
        , case
            when (laatste_inlog_maand is null or laatste_inlog_maand < sessie_maand - interval '12 months')
                and dossieruitwisseling_gestart > 0
                and dossieruitwisseling_succesvol = 0
            then true
            else false
        end as failed_gebruiker

        -- Gebruikerssegment voor frequente gebruikers die in de laatste 6 maanden hebben ingelogd en succesvol zijn geweest. Wordt gebruikt om later te aggregeren.
        , case
            when laatste_inlog_maand >= sessie_maand - interval '6 months'
                and dossieruitwisseling_succesvol > 0
            then true
            else false
        end as frequent_gebruiker
        , case when dossieruitwisseling_succesvol > 0 then true else false end as succesvolle_gebruiker
        , sum(user_sessie_aantal) over (
            partition by gebruiker_id
            order by sessie_maand
            range between interval '12 months' preceding and current row
        ) as sessie_aantal_afgelopen_12m
    from user_maand
)

select
    *
from user_maand_verrijkt