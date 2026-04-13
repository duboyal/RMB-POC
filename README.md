# RMB POC
Install:

- postgreSQL
- docker, docker-compose
- git 

Run:

docker compose up --build

Test:

echo "order_no,customer_no,customer_name,order_date,invoice_date,warehouse_no,total_cases,total_gross_weight
1,123,ACME,04/10/26,04/10/26,01,10,100" > data/incoming/test.ready