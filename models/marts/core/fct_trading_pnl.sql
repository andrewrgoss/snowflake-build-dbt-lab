{{ 
config(
      tags = 'core'
      ) 
}}


with
daily_positions as (
    select * from {{ ref('int_daily_position_with_trades' )}}

),

stock_history as (
    select * from {{ ref('int_stock_history_major_currency') }}

),

joined as (
    select 
        daily_positions.instrument, 
        daily_positions.stock_exchange_name, 
        daily_positions.book_date, 
        daily_positions.trader, 
        daily_positions.volume,
        daily_positions.cost, 
        daily_positions.cost_per_share,
        daily_positions.currency,
        sum(cost) over(
                partition by 
                    daily_positions.instrument, 
                    daily_positions.stock_exchange_name, 
                    trader 
                order by
                    daily_positions.book_date rows unbounded preceding 
                    )
                as cash_cumulative,
       case when daily_positions.currency = 'GBP' then gbp_close
            when daily_positions.currency = 'EUR' then eur_close
            else 'Close'
       end AS close_price_matching_ccy, 
       daily_positions.total_shares  * close_price_matching_ccy as market_value, 
       daily_positions.total_shares  * close_price_matching_ccy + cash_cumulative as PnL
   from daily_positions
   inner join stock_history 
      on daily_positions.instrument = stock_history.company_symbol 
     and stock_history.stock_date = daily_positions.book_date 
     and daily_positions.stock_exchange_name = stock_history.stock_exchange_name
)

select * from joined