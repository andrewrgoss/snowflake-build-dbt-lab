with stock_history as (

    select * from {{ ref('stg_knoema_stock_history') }}
        where indicator_name in ('Close', 'Open','High','Low', 'Volume', 'Change %') 

),

pivoted as (

    select 
        company_symbol, 
        company_name, 
        stock_exchange_name, 
        stock_date, 
        data_source_name,
        {{ dbt_utils.pivot(
      column = 'indicator_name',
      values = dbt_utils.get_column_values(ref('stg_knoema_stock_history'), 'indicator_name'),
      then_value = 'stock_value'
            ) }}
    
    from stock_history
    group by company_symbol, company_name, stock_exchange_name, stock_date, data_source_name

)

select * from pivoted