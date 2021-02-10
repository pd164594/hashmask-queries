with received as
(
select to_address, sum(cast(value as float64)/POWER(10,18)) as NCT_Rec
from `bigquery-public-data.crypto_ethereum.token_transfers`
where token_address = '0x8a9c4dfe8b9d8962b31e4e16f8321c44d48e246e'
Group by to_address
), 

sent as (
select from_address, sum(cast(value as float64)/POWER(10,18)) as NCT_send
from `bigquery-public-data.crypto_ethereum.token_transfers`
where token_address = '0x8a9c4dfe8b9d8962b31e4e16f8321c44d48e246e'
Group by from_address
)

select to_address as address, NCT_Rec - NCT_send as NCT_Balanace
from received a
join sent b
on a.to_address = b.from_address
order by NCT_Balanace desc