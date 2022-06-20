library(RPostgres)
library(DBI)



con=dbConnect(RPostgres::Postgres(),host="192.168.50.41",port="5432",dbname="postgres",user="dbread",password="readonly")

