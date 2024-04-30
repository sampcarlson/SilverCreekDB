library(sf)
library(RPostgres)
library(DBI)

source("~/R/projects/SilverCreekDB/dnIntakeTools.R")

conn=scdbConnect()

#write Points:
temperaturePoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_TemperatureLocations.gpkg")

#dbExecute(conn,"INSERT INTO locations (name, geometry, sourcenote, sitenote) VALUES ('Temperature #1', 'SRID=26911;POINT(744775.847791251 4791427.82542968)', 'temperatureLocations_source', 'TestPoint' );")
writePt=dbWritePoints(temperaturePoints)

dbGetQuery(conn, "SELECT * FROM locations;")
#dbExecute(conn,"DELETE FROM locations;")

dbGetQuery(conn,"SELECT locations.sitenote FROM locations WHERE ST_DWithin( 'SRID=26911;POINT(728910 4803082)', locations.geometry, 10000);")

dbIntakePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/'
dbIntakeKey=read.csv(paste0(dbIntakePath,"_siteIndex.csv"))



formFile=parseIntakeFile(dbIntakeKey$fileName[1])


dbGetQuery(conn, "SELECT * FROM batches;")

dbGetQuery(conn, "SELECT b.batchid FROM batches b, batches b2 WHERE b.source = b2.source AND b.batchid > b2.batchid;")

uniqueCols=c("source","include")

dbGetQuery(conn,"SELECT * FROM metrics;")


dbGetQuery(conn,"SELECT metric, value, datetime, locationid, COUNT(*) FROM data 
            GROUP BY metric, value, datetime, locationid HAVING( COUNT(*) > 1);")



dataSummary_loc=dbGetQuery(conn,"SELECT COUNT(value) AS num_records, metric, metricid, locations.name AS location, MIN(datetime) AS first_record, MAX(datetime) AS last_record FROM data
           LEFT JOIN locations ON data.locationid = locations.locationid
           GROUP BY metric, locations.name, metricid;")


dataSummary_metric=dbGetQuery(conn,"SELECT COUNT(value) AS num_records, metric, metricid, MIN(datetime) AS first_record, MAX(datetime) AS last_record FROM data
           GROUP BY metric, metricid;")




dbGetQuery(conn,"SELECT * FROM metrics;")
