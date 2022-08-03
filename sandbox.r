library(sf)
library(RPostgres)
library(DBI)


usethis::edit_r_environ()


Sys.getenv("scdb_pass")
Sys.getenv("scdb_readpass")


source("~/R/projects/SilverCreekDB/dnIntakeTools.R")


dbIntakePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/'
dbIntakeKey=read.csv(paste0(dbIntakePath,"_siteIndex.csv"))


formFile=parseIntakeFile(dbIntakeKey$fileName[1],site=1)


spatSource=joinToSpatialObject(formFile,sourceID_name="site")

writePt=prepPointLocations(spatSource)

dbGetQuery(con,"SELECT * FROM pointlocations")

dbGetQuery(con,"SELECT pointlocations.sitenote FROM pointlocations WHERE ST_DWithin( ST_SetSRID(ST_MakePoint(728910, 4803082),26911 ), pointlocations.point, 10000);")
