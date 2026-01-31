LOAD DATA LOCAL INFILE "C:\Users\Lenovo\Desktop\Credit_Card_Project\credit_card_transactions.csv"
INTO TABLE credit_card_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 rows;
