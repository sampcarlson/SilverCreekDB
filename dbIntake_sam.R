#dbintake process, temperature data, 8/2022
library(sf)
library(RPostgres)
library(DBI)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

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

dbWriteIntakeFile_1=function(fileName){
  formatDF=parseIntakeFile(dbIntakeKey$fileName[1])
  #identify location source
  if("DO" %in% names(formatDF)){ 
    sourceNote="source_DOLocations.gpkg"
  } else if ("WaterTemp" %in% names(formatDF)) { #DO datasets also contain WaterTemp, thus this condition only applys if "DO" is absent
    sourceNote="source_TemperatureLocations.gpkg"
  }
  #get location ids
  locations=data.frame(source_site=unique(formatDF$site))
  locations$locationID=sapply(locations$source_site,dbGetLocationID,sourceNote="source_DOLocations.gpkg")
  
  formatDF=merge(formatDF,locations,by.x="site",by.y="source_site")
  
  #write data
  if("DO" %in% names(formatDF)){
    dbWriteData(metric="dissolved oxygen",
                value=formatDF$DO,
                datetime = formatDF$Date,
                locationID=formatDF$locationID,
                sourceName=fileName,
                units = "mg/l",
                isPrediction=F,
                addMetric=T)
  }
  
  if("WaterTemp" %in% names(formatDF)){
    dbWriteData(metric="water temperature",
                value=formatDF$DO,
                datetime = formatDF$Date,
                locationID=formatDF$locationID,
                sourceName=fileName,
                units = "c",
                isPrediction=F,
                addMetric=T)
  }
  
}

dbWriteIntakeFile_1(dbIntakeKey$fileName[1])

#dbWriteData(metric, value, datetime, locationID, sourceName, units, isPrediction, addMetric )



