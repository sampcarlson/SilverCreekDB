#dbintake process, temperature data, 8/2022
library(sf)
library(RPostgres)
library(DBI)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

.db____DROPALLDATA()

#write locations:
temperaturePoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_TemperatureLocations.gpkg")
dbWritePoints(temperaturePoints,sourceNoteCol="source_TemperatureLocations.gpkg")

doPoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_DOLocations.gpkg")
dbWritePoints(doPoints,locationNameCol = "Name", sourceNoteCol = "source_DOLocations.gpkg",siteNoteCol="description")

gwPoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_GWLocations.gpkg")
dbWritePoints(gwPoints,locationNameCol = "SiteName", sourceNoteCol = "source_GWLocations.gpkg",siteNoteCol="WaterUse",source_siteIDCol = "WellNumber")
#write total depth or instrument to data?

swPoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_SWLocations.gpkg")
dbWritePoints(swPoints,locationNameCol = "STANAME", sourceNoteCol = "source_SWLocations.gpkg",siteNoteCol="STAID",source_siteIDCol = "STAID")

#write DO and temperature data
dbIntakePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/'
dbIntakeKey=read.csv(paste0(dbIntakePath,"_siteIndex.csv"))

sapply(dbIntakeKey$fileName, dbWriteIntakeFile_1)

#dbWriteData(metric, value, datetime, locationID, sourceName, units, isPrediction, addMetric)


