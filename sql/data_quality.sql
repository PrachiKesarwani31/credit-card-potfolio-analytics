#-------------------------------------------------------Data Quality------------------------------------------------------------------#
use credit_db;

#Null Checks
select
count(*)
from sanitized_credit_card_transactions
where trans_date_time is null
or cc_num_key is null
or trans_num is null;

#Duplicate checks
select
trans_num
from sanitized_credit_card_transactions
group by trans_num
having count(*)>1;

#Negative amount check
select
trans_num
from sanitized_credit_card_transactions
where amt<0;

#Timestamp validity
select
*
from sanitized_credit_card_transactions
where trans_date_time is null;