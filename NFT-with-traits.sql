with NFT_sold as (
select block_timestamp, transaction_hash, from_address as seller, to_address as buyer, value as NFT_ID
from `bigquery-public-data.crypto_ethereum.token_transfers`
where token_address = "0xc2c747e0f7004f9e8817db2ca4997657a7746928"
and cast(block_timestamp as date) > "2021-02-01"
),

weth_sales as(
select block_timestamp, transaction_hash, to_address as seller,  cast(value as float64)/POWER(10,18) as sold_for
from `bigquery-public-data.crypto_ethereum.token_transfers` 
where token_address="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
and to_address != "0x5b3256965e7c3cf26e11fcaf296dfc8807c01073"
and cast(block_timestamp as date) > "2021-02-01"
),


weth_data as (
select a.*, b.sold_for, "weth" as currency
from NFT_sold a
join weth_sales b
on a.transaction_hash = b.transaction_hash
),

eth_tx as (
select a.hash, cast(value as float64)/POWER(10,18) as eth,
from `bigquery-public-data.crypto_ethereum.transactions` a
where cast(block_timestamp as date) > "2021-02-01" and value > 0
),

eth_sales as (
select a.*, b.eth as sold_for, "eth" as currency
from NFT_sold a
join eth_tx b
on a.transaction_hash = b.hash
),

all_data as (
select * from eth_sales
union all 
select * from weth_data
),

final_data as(
select * from all_data 
where seller != "0x0000000000000000000000000000000000000000"
order by sold_for desc
),
hashmask_traits as (
SELECT 
cast(ID as STRING) as ID, skin, Character, Eyes, Mask, Item, Rarity
FROM `big-264521.hashmask.hashmask_traits` 
)

select 
*,
avg(sold_for) over (partition by transaction_hash) / count(*) over (partition by transaction_hash) as price_paid
from final_data a
join hashmask_traits b
on a.NFT_ID = b.ID
order by block_timestamp