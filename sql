"""
Example of SQL in DBT, to search load Ranging Stocks Daily
"""

{{ config(materialized='table') }}

with vol_stock as (select ticker, avg(close) as ac ,avg(volume) as avg_vol from {{source('SD', 'All_Data') }}
group by ticker
having avg_vol > 1000000 and ac > 15),
rs as (select * from {{source('Screeners', 'range_scanner') }})

select rs.* , vol_stock.ac, vol_stock.avg_vol from vol_stock
left join rs
on rs.ticker = vol_stock.ticker
order by rs.currentstreakdays desc 

"""
This example query is to identify the latest stocks with the highest volume
"""

select * from `Stock_Data.All_Data` 
left join `Stock_Data.Con_Stock_Info` 
on `Stock_Data.All_Data`.Ticker = `Stock_Data.Con_Stock_Info`.Stock
where industry is not null
order by date desc, Volume desc

"""
Example windows function to calculate day over day percent change 
"""
WITH grp_stocks AS (SELECT date, ticker, close, LAG(close) OVER (PARTITION BY ticker ORDER BY date asc) AS prev_close 
FROM `Stock_Data.All_Data`)

SELECT date, ticker, close, prev_close, close - prev_close AS price_change, (close - prev_close) / prev_close AS pct_change
FROM grp_stocks
ORDER BY ticker, date desc;

  
"""
This is an exammple of a windows function sql used to get the latest data per stock
"""
SELECT 
  ksi.*,fsl.string_field_1 AS Type
FROM `Stock_Data.Full_Stock_List` AS fsl
LEFT JOIN (
  SELECT * FROM (
    SELECT 
      asi.*,
      ad.Close,
      ad.Volume,
      ad.Open,
      ad.High,
      ad.Low,
      ad.Date,
      csi.industry,
      csi.sectorKey,
      ROW_NUMBER() OVER(PARTITION BY ad.Ticker ORDER BY ad.Date DESC) AS rn
    FROM `Stock_Data.All_Signals` AS asi
    LEFT JOIN `Stock_Data.Con_Stock_Info` AS csi
      ON asi.Stock = csi.Stock
    LEFT JOIN `Stock_Data.All_Data` AS ad
      ON ad.Ticker = asi.Stock
  )
  WHERE rn = 1  
) AS ksi

ON fsl.string_field_0 = ksi.Stock;

"""
This is an example of a CTE to calculate total volume and put call ratio
"""

with sep_volume as (select stock, type, sum(volume) as Volume from `Option_Data.daily_top_volume_option_stream`
group by stock, Type),
pc_ratio as  (select stock, sum(case when type = 'Put' then volume else 0 end) / NULLIF(sum(case when type = 'Call' then volume else 0 end), 0) as put_call_ratio
from `Option_Data.daily_top_volume_option_stream`
group by Stock),
total_vol as (select stock, sum(sep_volume.Volume) as Total_volume from sep_volume
group by stock
order by Total_volume desc)

select total_vol.stock, total_vol.Total_volume, pc_ratio.put_call_ratio from total_vol
left join pc_ratio
on pc_ratio.stock = total_vol.stock
order by total_vol.Total_volume desc



"""
Example of using CASE, WHEN to find 3 consecutive days of positive stock gains
"""
with t1 as (select date, ticker, open, close,
case
  when close > open then 1
  when close < open then 0
  else 0
end as up_down
from `Stock_Data.All_Data`)


select t.date, t.ticker, t.open, t.close, t.up_down, sum(t.up_down) over (partition by t.ticker order by date rows between 2 preceding and current row) as up3_sum from t1 as t
order by t.ticker, t.date



