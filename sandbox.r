library(sf)
library(RPostgres)
library(DBI)

source("~/R/projects/SilverCreekDB/dnIntakeTools.R")

conn=scdbConnect(readOnly = F)

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

