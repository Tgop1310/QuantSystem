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
  WHERE rn = 1  -- Only keep the most recent date per stock
) AS ksi

ON fsl.string_field_0 = ksi.Stock;

