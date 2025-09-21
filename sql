"""
This example query is to identify the latest stocks with the highest volume
"""

select * from `Stock_Data.All_Data` 
left join `Stock_Data.Con_Stock_Info` 
on `Stock_Data.All_Data`.Ticker = `Stock_Data.Con_Stock_Info`.Stock
where industry is not null
order by date desc, Volume desc
