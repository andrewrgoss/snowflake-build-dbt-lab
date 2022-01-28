{{ 
config(
    materialized='view', 
      tags=["hourly"]
    ) 
}}
 
select * from {{ ref('stg_knoema_fx_rates') }} 
 where indicator_name = 'Close' 
   and frequency = 'D' 
   and exchange_date > '2016-01-01'