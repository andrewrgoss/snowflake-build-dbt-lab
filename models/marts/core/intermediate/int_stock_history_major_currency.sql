with
stock_history as (
    select * from {{ ref('int_knoema_stock_history')}}
),
 
fx_rates as (
    select * from {{ ref('int_fx_rates') }}
),
 
fx_rates_gdp as (
    select * from fx_rates
        where currency = 'USD/GBP'   
),
 
fx_rates_eur as (
    select * from fx_rates
        where currency = 'USD/EUR' 
),
 
joined as (
    select 
        stock_history.*,
        fx_rates_gdp.exchange_value * stock_history."Open" as gbp_open,       
        fx_rates_gdp.exchange_value * stock_history."High" as gbp_high,     
        fx_rates_gdp.exchange_value * stock_history."Low" as gbp_low,   
        fx_rates_gdp.exchange_value * stock_history."Close" as gbp_close,     
        fx_rates_eur.exchange_value * stock_history."Open" as eur_open,       
        fx_rates_eur.exchange_value * stock_history."High" as eur_high,     
        fx_rates_eur.exchange_value *stock_history."Low" as eur_low,
        fx_rates_eur.exchange_value * stock_history."Close" as eur_close    
    from stock_history
    left join fx_rates_gdp on stock_history.stock_date = fx_rates_gdp.exchange_date
    left join fx_rates_eur on stock_history.stock_date = fx_rates_eur.exchange_date
)
 
select * from joined