with langdurige_toestemmingen as (
    select
        date_trunc('month', timestamp)::date as "WindowStartMaand"
        , aantal_langdurige_toestemmingen as "AantalLangdurigeToestemming"
    from s30_modeling.fct_pgo_langdurige_toestemming_aantal
)

, succesvol_system as (
    select
        date_trunc('month', timestamp)::date as "WindowStartMaand"
        , aantal_succesvol_system as "AantalSuccesvolSystem"
    from s30_modeling.fct_pgo_succesvol_system_aantal
)

, onsuccesvol_system as (
    select
        date_trunc('month', timestamp)::date as "WindowStartMaand"
        , aantal_onsuccesvol_system as "AantalOnSuccesvolSystem"
    from s30_modeling.fct_pgo_onsuccesvol_system_aantal
)

select
    014 as "Deelnemer"
    , 'Topicus' as "Leverancier"
    , now()::timestamp as "TijdstempelUitvoering"
    , maandelijks."RapportageMaand"::date
    , maandelijks."WindowStartMaand"::date
    , maandelijks."WindowEindMaand"::date
    , rollend_12m."WindowStartTwaalfMaand"::date
    , rollend_12m."WindowEindTwaalfMaand"::date

    -- KPI 1
    , rollend_12m."GebruikersAfgelopenJaar"::integer

    -- KPI 2
    , maandelijks."GebruikersAfgelopenMaand"::integer

    -- KPI 3
    , maandelijks."AantalBezoekers"::integer
    , maandelijks."AantalFailed"::integer
    , maandelijks."AantalSuccesvol"::integer

    -- KPI 4
    , maandelijks."AantalInactief"::integer
    , maandelijks."AantalFrequent"::integer
    , maandelijks."AantalNonFrequent"::integer

    -- KPI 5
    , langdurige_toestemmingen."AantalLangdurigeToestemming"::integer

    -- KPI 6
    , maandelijks."AantalEenmaligGebruik"::integer
    , maandelijks."AantalTweeTotVijfmaligGebruik"::integer
    , maandelijks."AantalZesTotNegenmaligGebruik"::integer
    , maandelijks."AantalTienmaligOfMeerGebruik"::integer

    -- KPI 7 - Device Gebruik
    , uitwissel_maandelijks."AantalApp"::integer
    , uitwissel_maandelijks."AantalPC"::integer
    , uitwissel_maandelijks."AantalWeb"::integer
    , succesvol_system."AantalSuccesvolSystem"::integer + onsuccesvol_system."AantalOnSuccesvolSystem"::integer as "AantalSystem"

    -- KPI 8 - Succesvol vs onsuccesvol device gebruik
    , uitwissel_maandelijks."SuccesvolApp"::integer
    , uitwissel_maandelijks."SuccesvolPC"::integer
    , uitwissel_maandelijks."SuccesvolWeb"::integer
    , succesvol_system."AantalSuccesvolSystem"::integer as "SuccesvolSystem"
    , uitwissel_maandelijks."UitvalApp"::integer
    , uitwissel_maandelijks."UitvalPC"::integer
    , uitwissel_maandelijks."UitvalWeb"::integer
    , onsuccesvol_system."AantalOnSuccesvolSystem"::integer as "UitvalSystem"

    -- KPI 9 - Uitval per processtap
    , uitwissel_maandelijks."UitvalZorgaanbiederZoeken"::integer
    , uitwissel_maandelijks."UitvalDigID"::integer
    , uitwissel_maandelijks."UitvalUitwisseling"::integer
    , uitwissel_maandelijks."SuccesvolZorgaanbiederZoeken"::integer
    , uitwissel_maandelijks."SuccesvolDigID"::integer
    , uitwissel_maandelijks."SuccesvolUitwisseling"::integer


    -- KPI 10 - Pogingen tot failed
    , maandelijks."AantalEenmaligFailed"::integer
    , maandelijks."AantalTweeTotVijfmaligFailed"::integer
    , maandelijks."AantalZesTotNegenmaligFailed"::integer
    , maandelijks."AantalTienmaligOfMeerFailed"::integer

from {{ ref('fct_dvp_user_metrics_maandelijks') }} as maandelijks
left join {{ ref('fct_dvp_user_rollend_12m_metrics') }} as rollend_12m
    on maandelijks."RapportageMaand" = rollend_12m."RapportageMaand"
left join langdurige_toestemmingen
    on maandelijks."WindowStartMaand" = langdurige_toestemmingen."WindowStartMaand"
left join succesvol_system
    on maandelijks."WindowStartMaand" = succesvol_system."WindowStartMaand"
left join onsuccesvol_system
    on maandelijks."WindowStartMaand" = onsuccesvol_system."WindowStartMaand"
left join {{ ref('fct_dvp_uitwissel_metrics_maandelijks') }} as uitwissel_maandelijks
    on uitwissel_maandelijks."RapportageMaand" = maandelijks."RapportageMaand"
order by maandelijks."RapportageMaand" DESC
limit 1