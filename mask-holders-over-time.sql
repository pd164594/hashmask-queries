with dates as (
select *
from UNNEST(GENERATE_TIMESTAMP_ARRAY('2021-01-01', cast(CURRENT_DATE() as TIMESTAMP), INTERVAL 1 DAY)) AS date
), 

distinct_prep as (
SELECT distinct(to_address) as address
FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
WHERE token_address = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
union all
SELECT distinct(from_address) as address
FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
WHERE token_address = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
), 

all_addresses as (
select distinct(address) as address
from distinct_prep
), 


reference_table as (
select cast(date as date) as date, address from dates
cross join all_addresses
), 

recieved_mask as (
SELECT to_address, cast(block_timestamp as date) as date, count(value) as rec_mask, 
FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
WHERE lower(token_address) = '0xc2c747e0f7004f9e8817db2ca4997657a7746928'
group by to_address, date
), 


sent_mask as (
SELECT from_address, cast(block_timestamp as date) as date, count(value) as sent_mask, 
FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
WHERE lower(token_address) = lower('0xc2c747e0f7004f9e8817db2ca4997657a7746928')
group by from_address, date
),

final_prep as (
select a.*, case when b.rec_mask is null then 0 else b.rec_mask  end as rec_mask, case when c.sent_mask is null then 0 else c.sent_mask end as sent_mask
from reference_table a
left join recieved_mask b
on a.date = b.date and lower(a.address) = lower(b.to_address)
left join sent_mask c
on a.date = c.date and lower(a.address) = c.from_address
order by date asc
), 

final as (
select 
date, 
address, 
rec_mask,
sent_mask,
sum(rec_mask) OVER (PARTITION BY address ORDER BY date) as running_rec,
sum(sent_mask) OVER (PARTITION BY address ORDER BY date) as running_sent
from final_prep), 

last_one as (
select date, address, (running_rec-running_sent) as balance
from final
-- where (running_rec-running_sent) is not null or (running_rec-running_sent) > 0
)

select date, count(address)
from last_one
where balance > 0
group by date
order by date
