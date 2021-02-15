--Hash in. 
with hash_in as (
select * 
from `bigquery-public-data.crypto_ethereum.token_transfers`
where to_address = '0xaf93fcce0548d3124a5fc3045adaf1dde4e8bf7e'
and token_address = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
), 


hash_out as (
select * 
from `bigquery-public-data.crypto_ethereum.token_transfers`
where from_address = '0xaf93fcce0548d3124a5fc3045adaf1dde4e8bf7e'
and token_address = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
),

all_hash as (
select * 
from hash_in
union all
select * 
from hash_out
),

in_out as (
select block_timestamp, value as NFT_ID, from_address, to_address, transaction_hash,
case 
  when from_address = "0xaf93fcce0548d3124a5fc3045adaf1dde4e8bf7e" then "out"
  when to_address = "0xaf93fcce0548d3124a5fc3045adaf1dde4e8bf7e" then "in"
end as direction
from all_hash
order by value, block_timestamp
),
max_date as 
(
select max(block_timestamp) as  date,
NFT_ID as id
from in_out 
group by id
), 

final as (
select  
block_timestamp, 
transaction_hash,
NFT_ID,
direction
from in_out a
join max_date b
on a.block_timestamp = b.date
and a.NFT_ID = b.id
where direction = "in"
order by cast(NFT_ID as float64) asc
), 

traits_detailed as (
select * from `big-264521.hashmask.new_hashmask_traits` 
),
traits_generic as (
select ID, Rarity
 from `big-264521.hashmask.hashmask_traits` 
)

select * from final a
join traits_detailed b 
on a.NFT_ID = cast(b.number as string)
join traits_generic c
on a.NFT_ID = cast(c.ID as string)

