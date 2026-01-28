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
cc_num as cc_num,
row_number() over(order by cc_num) as cc_num_key
from (select distinct cc_num from credit_card_transactions) s;

create table if not exists sanitized_credit_card_transactions as
select
'trans_date_time','cc_num_key','merchant','category','amt',
'gender','city','state','trunc_zip','lat','lon','city_pop','job',
'age','trans_num','merch_lat','merch_long','is_fraud','merch_zipcode'
union all
select
cast(r.trans_date_trans_time as datetime) as trans_date_time,
s.cc_num_key as cc_num_key,
r.merchant as merchant,
r.category as category,
r.amt as amt,
r.gender as gender,
r.city as city,
r.state as state,
substring(cast(r.zip as char),1,3) as trunc_zip,
r.lat as lat,
r.lon as lon,
r.city_pop as city_pop,
r.job as job,
ceil(datediff(curdate(),cast(r.dob as date))/365) as age,
r.trans_num as trans_num,
r.merch_lat as merch_lat,
r.merch_long as merch_long,
r.is_fraud as is_fraud,
r.merch_zipcode as merch_zipcode
from credit_card_transactions r
join cc_num_masking s
on r.cc_num=s.cc_num;


select count(*) from sanitized_credit_card_transactions;

select * from sanitized_credit_card_transactions;

SELECT *
from sanitized_credit_card_transactions
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sanitized_credit_card_transactions.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';
