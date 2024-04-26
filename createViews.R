library(sf)
library(RPostgres)
library(DBI)

source("~/Documents/R Workspace/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect()


dbExecute(conn,"CREATE INDEX data_metric_index ON data (metric);")
dbExecute(conn,"CREATE INDEX data_metricid_index ON data (metricid);")
dbExecute(conn,"CREATE INDEX data_date_index ON data (datetime);")
dbExecute(conn,"CREATE INDEX data_location_index ON data (locationid);")
dbExecute(conn,"CREATE INDEX data_qc_index ON data (qcstatus);")
dbExecute(conn,"CREATE INDEX locations_geometry_index ON locations USING GIST(geometry);")
dbExecute(conn,"CREATE INDEX streampoints_geometry_index ON streampoints USING GIST(geometry);")
dbExecute(conn,"CREATE INDEX watershed_outflowlocation_index ON watersheds (outflowlocationid);")
dbExecute(conn,"CREATE INDEX watershed_geometry_index ON watersheds USING GIST(geometry);")
dbExecute(conn,"REFRESH MATERIALIZED VIEW locationattributes;")

#dbExecute(conn, "DROP MATERIALIZED VIEW locationattributes")

# dbExecute(conn, "CREATE MATERIALIZED VIEW locationattributes AS SELECT DISTINCT locations.locationid, STRING_AGG(DISTINCT(data.metric), ',') AS metrics, watersheds.geometry FROM locations 
#                     LEFT JOIN watersheds ON locations.locationid = watersheds.outflowlocationid
#                     LEFT JOIN data ON locations.locationid = data.locationid GROUP BY locations.locationid, watersheds.geometry")


dbExecute(conn, "CREATE MATERIALIZED VIEW locationattributes AS SELECT ROW_NUMBER() OVER(), met.locationid, met.name, met.locationgeometry, met.metrics, wsh.wshedareakm, wsh.watershedgeometry FROM
                      (SELECT DISTINCT locations.name, locations.locationid, locations.geometry AS locationgeometry, STRING_AGG(DISTINCT(data.metric), ',') AS metrics FROM locations LEFT JOIN data ON locations.locationid = data.locationid GROUP BY locations.locationid) met
                     LEFT JOIN 
                      (SELECT locations.locationid, watersheds.geometry AS watershedgeometry, ST_AREA(watersheds.geometry)/1000000 AS wshedareakm FROM locations LEFT JOIN watersheds ON locations.locationid = watersheds.outflowlocationid) wsh
                      ON met.locationid = wsh.locationid;")


dbExecute(conn, "DROP MATERIALIZED VIEW snodasdata;")
dbExecute(conn, "CREATE MATERIALIZED VIEW snodasdata AS SELECT data.metric, data.value, data.datetime, 
          data.metricid, data.locationid, data.qcstatus, data.qcdetails
          FROM data LEFT JOIN batches ON data.batchid = batches.batchid WHERE batches.source = 'snodas' AND qcstatus=TRUE;")


dbExecute(conn,"CREATE OR REPLACE FUNCTION wateryear(datetime timestamp without time zone) RETURNS integer AS $$
              SELECT CASE WHEN ( EXTRACT(month FROM datetime)) >= 10  THEN EXTRACT(year FROM datetime) +1 
                                      ELSE EXTRACT(year FROM datetime) 
                        END
                
                $$
            LANGUAGE SQL;")


dbExecute(conn, "REFRESH MATERIALIZED VIEW snodasdata")

st_read(conn,query="SELECT * FROM locationattributes LIMIT 10")

l33=st_read(conn,query="SELECT name, wshedareakm, watershedgeometry FROM locationattributes WHERE locationid = 33")
library(terra)



