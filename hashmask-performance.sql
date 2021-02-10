--The Hashed Mask Token Analysis 
with hashmask as (
select *, 1 as counter
from `bigquery-public-data.crypto_ethereum.token_transfers` 
where token_address = "0xc2c747e0f7004f9e8817db2ca4997657a7746928"
and from_address = "0x0000000000000000000000000000000000000000"
),

eth_contribute as (
select from_address , cast(value as float64)/POWER(10,18) as eth,  ((cast(value as float64)/POWER(10,18)) * 1600) as dollar_value
from `bigquery-public-data.crypto_ethereum.transactions`
where to_address = "0xc2c747e0f7004f9e8817db2ca4997657a7746928"
and value <> 0
), 
nft_yield as(
select to_address, sum(counter) as total_hash_masks, (sum(counter)) * 3600 * .08  as NFT_Yield
from hashmask
group by to_address
order by total_hash_masks desc),

user_contributions as (
select from_address, sum(eth) as total_eth_contributed, sum(dollar_value) as dollars_spent
from eth_contribute
group by from_address
)

select from_address, total_hash_masks, total_eth_contributed, total_eth_contributed/total_hash_masks as avg_nft_price, dollars_spent,  NFT_Yield, (3660 * .08) as annual_yield, NFT_Yield - dollars_spent as profit_from_NFT_sale
from user_contributions a
join nft_yield b
on a.from_address = b.to_address
order by total_hash_masks desc
