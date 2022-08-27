#dbintake process, temperature data, 8/2022
library(sf)
library(RPostgres)
library(DBI)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

#.db____DROPALLDATA()

#write locations:
temperaturePoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_TemperatureLocations.gpkg")
temperaturePoints$ID=paste0("WaterTemp_",temperaturePoints$ID)
dbWritePoints(temperaturePoints,sourceNoteCol="source_TemperatureLocations.gpkg")

doPoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_DOLocations.gpkg")
doPoints$ID=paste0("DO_",doPoints$ID)
dbWritePoints(doPoints,locationNameCol = "Name", sourceNoteCol = "source_DOLocations.gpkg",siteNoteCol="description")

#write DO and temperature data
dbIntakePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/'
dbIntakeKey=read.csv(paste0(dbIntakePath,"_siteIndex.csv"))

#dbWriteIntakeFile_1(dbIntakeKey$fileName[100])

sapply(dbIntakeKey$fileName, dbWriteIntakeFile_1)

#dbWriteData(metric, value, datetime, locationID, sourceName, units, isPrediction, addMetric)


