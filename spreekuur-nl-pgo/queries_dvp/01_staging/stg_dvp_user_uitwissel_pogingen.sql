{{ config(
    materialized = 'table',
    schema = 's20_staging',
) }}

with basis_actie_selectie as (
    select
        acties.session_id as session_id
        , acties.action_at as actie_timestamp
        , date_trunc('month', acties.action_at) as sessie_maand
        , sessies.user_id as gebruiker_id
        , case
            when browser_name in ('Mobile Safari UI/WKWebView', 'Chrome Mobile WebView') then 'App'
            when device_type = 'Mobile' or device_type = 'Tablet' then 'Web Mobile'
            when device_type = 'Desktop' then 'Web Desktop'
            else 'Other'
        end as apparaat_context
        , case when acties.action_name = 'verzamelen.flow-start' then 1 else 0 end as dossieruitwisseling_gestart
        , case when acties.action_name = 'verzamelen.zorgaanbieder-gekozen' then 1 else 0 end as zorgaanbieder_gekozen
        , case when acties.action_name = 'verzamelen.gegevensdienst-gekozen' then 1 else 0 end as gegevensdienst_gekozen
        , case when acties.action_name = 'verzamelen.ingelogd-bij-dva' then 1 else 0 end as ingelogd_bij_dva
        , case when acties.action_name = 'verzamelen.flow-end' then 1 else 0 end as dossierflow_afgerond
        , case when acties.action_name = 'verzamelen.flow-end' then dossier_ophalen_flow_result else null end as dossierflow_resultaat
        , concat(
                    sum(
                        case when acties.action_name = 'verzamelen.flow-start' then 1 else 0 end)
                        over (partition by acties.session_id order by acties.action_at
                    )
                    , '_'
                    , acties.session_id) as dossieruitwisseling_id
    from s20_staging.stg_actions_spreekuur as acties
    left join s20_staging.stg_sessions_spreekuur as sessies
        on acties.session_id = sessies.session_id
    where sessies.user_id is not null
)


, funnel_aggregaat as (
    select
        dossieruitwisseling_id
        , max(gebruiker_id) as gebruiker_id
        , max(sessie_id) as sessie_id
        , min(actie_timestamp) as sessie_timestamp
        , max(sessie_maand) as sessie_maand
        , max(apparaat_context) as apparaat_context
        , max(dossieruitwisseling_gestart) as dossieruitwisseling_gestart
        , max(zorgaanbieder_gekozen) as zorgaanbieder_gekozen
        , max(gegevensdienst_gekozen) as gegevensdienst_gekozen
        , max(ingelogd_bij_dva) as ingelogd_bij_dva
        , max(dossierflow_afgerond) as dossieruitwisseling_afgerond
        , max(
            case when dossierflow_resultaat = 'success' then 1 else 0 end
        ) as dossieruitwisseling_succesvol
    from basis_actie_selectie
    group by dossieruitwisseling_id
)

select * from funnel_aggregaat