"""
This example query is to identify the latest stocks with the highest volume
"""

select * from `Stock_Data.All_Data` 
left join `Stock_Data.Con_Stock_Info` 
on `Stock_Data.All_Data`.Ticker = `Stock_Data.Con_Stock_Info`.Stock
where industry is not null
order by date desc, Volume desc


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
This is ann example of a CTE to calculate total volume and put call ratio
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




