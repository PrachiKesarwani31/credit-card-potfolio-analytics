use credit_db;

LOAD DATA LOCAL INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/featured_credit_card_transaction.csv"
INTO TABLE featured_credit_card_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 rows;

alter table featured_credit_card_transactions
modify column trans_date_time datetime;


create index f_cc_idx on featured_credit_card_transactions(cc_num_key);
create index f_merch_idx on featured_credit_card_transactions(merchant(255),merch_zipcode);
create index f_time_idx on featured_credit_card_transactions(trans_date,hr);
create index f_loc_idx on featured_credit_card_transactions(state(100),city(100),trunc_zip);

#Grain is 1 row per card containing information about every card holder
create table if not exists dim_card as
select 
row_number() over(order by cc_num_key) as card_id,
cc_num_key,
max(gender),
max(job),
max(age),
max(age_bucket)
from featured_credit_card_transactions
group by cc_num_key;

#Grain is 1 row per merchant (so for unique merchant = merchant_merch_zipcode is used)
create table if not exists dim_merchant as 
select
row_number() over(order by merchant) as merchant_id,
merchant,
max(category)
from featured_credit_card_transactions
group by merchant;


#Grain is distinct time
create table if not exists dim_time as
select
row_number() over(order by trans_date,hr) as time_id,
trans_date,
hr,
dayweek,
mon,
weekend
from
(select
distinct 
trans_date,
hr,
dayweek,
mon,
weekend
from featured_credit_card_transactions) s1;

#Grain is every distinct location
create table if not exists dim_card_location as 
select
row_number() over(order by state,city,trunc_zip) as location_id,
city,
state,
trunc_zip,
city_buckets
from
(select
distinct 
state,
city,
trunc_zip,
city_buckets
from featured_credit_card_transactions) s2;

ALTER TABLE dim_merchant
ADD CONSTRAINT uq_dim_merchant UNIQUE (merchant);

select
merchant, count(*)
from dim_merchant
group by merchant
having count(*)>1;

alter table dim_card add unique (cc_num_key);
alter table dim_merchant add unique (merchant(255));
alter table dim_time add unique (trans_date,hr);
alter table dim_location add unique (state(100),city(100),trunc_zip);

create index d_cc_idx on dim_card(cc_num_key);
create index d_merch_idx on dim_merchant(merchant(255));
create index d_time_idx on dim_time(trans_date,hr);
create index d_loc_idx on dim_location(state(100),city(100),trunc_zip);

SHOW FULL COLUMNS FROM dim_time;
SHOW INDEX FROM dim_merchant;

ALTER TABLE dim_merchant
MODIFY merchant VARCHAR(255) 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_0900_ai_ci 
NOT NULL;


#Grain is 1 row every transaction (contains attributes which changes with every transaction)
create table if not exists fact_credit_transactions(
trans_num varchar(255) primary key,
card_id int,
merchant_id int,
time_id int,
location_id int,
amt float,
distance float,
is_fraud int
);

insert into fact_credit_transactions
(trans_num,
card_id,
merchant_id,
time_id,
location_id,
amt,
distance,
is_fraud)
select
f.trans_num,
c.card_id,
m.merchant_id,
t.time_id,
l.location_id,
f.amt,
f.distance,
f.is_fraud
from featured_credit_card_transactions f
left join dim_card c
on f.cc_num_key=c.cc_num_key
left join dim_merchant m
on f.merchant=m.merchant
left join dim_time t
on f.trans_date=t.trans_date
and f.hr=t.hr
left join dim_location l
on f.state=l.state
and f.city=l.city
and f.trunc_zip=l.trunc_zip;

 

create table if not exists customer_summary as
select
cc_num_key,
count(*) as total_transactions,
sum(amt) as total_spend,
avg(amt) as avg_txn_amt,
max(amt) as max_txn_amount,
max(gender) as gender,
max(city) as latest_city,
max(state) as latest_state,
max(job) as latest_occupation,
max(age) as latest_age,
sum(is_fraud) as total_fraud, #0 is no fraud where as 1 is fraud
cast(sum(is_fraud)/count(*) as decimal) as fraud_rate, 
sum(weekend) as weekend_txn_count,
sum(case when hr between 0 and 5 then 1 else 0 end) as night_txn_count 
from featured_credit_card_transactions
group by cc_num_key;

create table if not exists merchant_summary as
select
merchant,
count(*) as total_txn_count,
sum(amt) as total_merchant_txn,
avg(amt) as avg_merchant_txn,
max(amt) as max_merchant_txn,
sum(is_fraud) as total_fraud_count,
cast(sum(is_fraud)/count(*) as decimal) as fraud_rate,
sum(weekend) as count_weekend_txn,
sum(case when hr between 0 and 5 then 1 else 0 end) as night_txn_count
from featured_credit_card_transactions 
group by merchant;

select count(*) from cc_num_masking;
select count(*) from dim_card;
select count(*) from dim_merchant;
select count(*) from dim_time;
select count(*) from dim_location;


select 
trans_num,
count(*) 
from featured_credit_card_transactions
group by trans_num
having count(*)>1;

select count(*)
from featured_credit_card_transactions;

create table temp_featured_credit_card_transactions like featured_credit_card_transactions;

insert into temp_featured_credit_card_transactions
select distinct * from featured_credit_card_transactions;

select count(*) from featured_credit_card_transactions;

drop table fact_credit_transactions;

alter table temp_featured_credit_card_transactions
rename to featured_credit_card_transactions;

select * from fact_credit_transactions;

select sum(amt) from fact_credit_transactions
union all
select sum(amt) from credit_card_transactions;