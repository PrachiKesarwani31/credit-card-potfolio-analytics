create database if not exists credit_db;

use credit_db;

set global local_infile=1;

LOAD DATA LOCAL INFILE "C:\Users\Lenovo\Desktop\Credit_Card_Project\credit_card_transactions.csv"
INTO TABLE credit_card_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 rows;

create table if not exists cc_num_masking as
select
cc_num,
row_number() over(order by cc_num) as cc_num_key
from (select distinct cc_num from credit_card_transactions) s;

create table if not exists sanitized_credit_card_transactions as
select
cast(r.trans_date_trans_time as datetime),
s.cc_num_key,
r.merchant,
r.category,
r.amt,
r.gender,
r.city,
r.state,
substring(cast(r.zip as char),1,3) as trunc_zip,
r.lat,
r.lon,
r.city_pop,
r.job,
abs(datediff(curdate(),cast(r.dob as date))/365) as age,
r.trans_num,
r.merch_lat,
r.merch_long,
r.is_fraud,
r.merch_zipcode
from credit_card_transactions r
join cc_num_masking s
on r.cc_num=s.cc_num;


select count(*) from sanitized_credit_card_transactions;

select * from sanitized_credit_card_transactions;