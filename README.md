# RMB POC

ssh onto ubuntu server 

```
ssh muscosql@our.ip.adress
```
 

Install on Ubuntu Server:

- postgreSQL ✅
- docker, docker-compose ✅
- git ✅


cd into Repo 
```RMB-POC```
and perform ```git pull``` to bring down the latest changes.


Run:

```sudo docker compose down -v``` to reboot from scratch

then:

```sudo docker compose up --build```

Make sure to get env file over with scp:

```scp .env muscosql@our_ip:~/RMB-POC/.env```


Test file drop abilities (drop file and test listener) with scp:

```scp cust1.txt muscosql@our_ip:~/RMB-POC/data/incoming/cust1.txt```

or

```scp cust1.txt muscosql@our_ip:~/RMB-POC/data/incoming/test_cust1.txt```


OR Test:

echo "order_no,customer_no,customer_name,order_date,invoice_date,warehouse_no,total_cases,total_gross_weight
1,123,ACME,04/10/26,04/10/26,01,10,100" > data/incoming/test.ready

#### Whenever you want to test the code changes you just pushed to github, make sure to PULL the code changes down when on the UBUNTU server, then run ```docker compose --build``` and ```docker compose up``` 

---------------
### NOTES FOR BEST PRACTICE DEV FLOW:

cd into Repo 
```RMB-POC```
and perform ```git pull``` to bring down the latest changes.

then run 
```
docker compose up --build
```
then drop appropiate file into
```
RMB-POC/data/incoming/
```
---------------
### General dev notes:
Your git repo does not point to the database.

Git tracks:
-	code
-	Docker files
-	SQL init/migrations

Git does not track the live DB.

Best-practice flow

Local
-	write code on your Mac
-	run local Docker
-	use local Postgres for dev/testing

Remote Ubuntu
-	install git
-	clone repo
-	run Docker there
-	remote Postgres is the shared/test DB

So do not “sync DB to code”

Instead:
-	code syncs through git
-	database schema syncs through SQL/migrations
-	data stays in the DB

Best setup for you
1.	Mac
    -	local code
    -	local Docker
    -	local Postgres
2.	Ubuntu
    -	cloned repo
    -	Docker running
    -	remote Postgres
3.	Deploy flow
    -	edit locally
    -	commit + push
    -	ssh into Ubuntu
    -	```git pull```
    -	```docker compose up -d --build```

If you want to test against remote DB from your Mac

Yes, you can.

Your local code/container can connect to remote Postgres by changing DB host/port in env vars.

But best practice is:
-	mostly use local DB
-	only use remote DB for integration testing

Install git on Ubuntu
```
sudo apt update
sudo apt install git -y
```
Then clone repo
```
git clone https://github.com/duboyal/RMB-POC.git
cd RMB-POC
```
Then later install Docker and run it there
```
docker compose up -d --build
```
Important mental model
-	GitHub repo = source of truth for code
-	remote Postgres = runtime data
-	Docker = how app + DB run
-	env vars = what DB your code points at

So yes, you can point your code to the remote DB, but don’t think of git as linking to the DB. It’s just deploying code that knows how to connect to a DB.