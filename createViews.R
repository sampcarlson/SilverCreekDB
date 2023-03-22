library(sf)
library(RPostgres)
library(DBI)

source("~/Documents/R Workspace/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect()

dbSendQuery(conn, "DROP MATERIALIZED VIEW locationattributes")


# dbSendQuery(conn, "CREATE MATERIALIZED VIEW locationattributes AS SELECT DISTINCT locations.locationid, STRING_AGG(DISTINCT(data.metric), ',') AS metrics, watersheds.geometry FROM locations 
#                     LEFT JOIN watersheds ON locations.locationid = watersheds.outflowlocationid
#                     LEFT JOIN data ON locations.locationid = data.locationid GROUP BY locations.locationid, watersheds.geometry")


dbSendQuery(conn, "CREATE MATERIALIZED VIEW locationattributes AS SELECT ROW_NUMBER() OVER(), met.locationid, met.name, met.locationgeometry, met.metrics, wsh.wshedareakm, wsh.watershedgeometry FROM
                      (SELECT DISTINCT locations.name, locations.locationid, locations.geometry AS locationgeometry, STRING_AGG(DISTINCT(data.metric), ',') AS metrics FROM locations LEFT JOIN data ON locations.locationid = data.locationid GROUP BY locations.locationid) met
                     LEFT JOIN 
                      (SELECT locations.locationid, watersheds.geometry AS watershedgeometry, ST_AREA(watersheds.geometry)/1000000 AS wshedareakm FROM locations LEFT JOIN watersheds ON locations.locationid = watersheds.outflowlocationid) wsh
                      ON met.locationid = wsh.locationid;")


st_read(conn,query="SELECT * FROM locationattributes LIMIT 10")

