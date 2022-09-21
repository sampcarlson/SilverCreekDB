#dbintake process, temperature data, 8/2022
library(sf)
library(RPostgres)
library(DBI)
library(pbapply)

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


dbWriteIntakeFile_1=function(fileName){
  #print(fileName)
  formatDF=parseIntakeFile(fileName)
  #identify location source
  if("DO" %in% names(formatDF)){ 
    sourceNote="source_DOLocations.gpkg"
  } else if ("WaterTemp" %in% names(formatDF)) { #DO datasets also contain WaterTemp, thus this condition only applies if "DO" is absent
    sourceNote="source_TemperatureLocations.gpkg"
  }
  #get location ids
  locations=data.frame(source_site=unique(formatDF$site))
  locationIDs=dbGetQuery(conn,paste0("SELECT locationid, source_site_id FROM locations WHERE locations.source_site_id IN ('",
                                     paste0(locations$source_site,collapse="', '"),"');"))
  
  if(!all(locations$source_site %in% locationIDs$source_site_id)){
    print(paste0("Missing location information for site:",locations$source_site[!locations$source_site %in% locationIDs$source_site_id]))
    
  } else {
    #locations=merge(locations,locationIDs,by.x="site",by.y="source_site_id")
    
    #locations$locationID=sapply(locations$source_site,dbGetLocationID,sourceNote=sourceNote)
    
    formatDF=merge(formatDF,locationIDs,by.x="site",by.y="source_site_id")
    
    
    #write data
    if("DO" %in% names(formatDF)){
      dbWriteData(metric="dissolved oxygen",
                  value=formatDF$DO,
                  datetime = formatDF$Date,
                  locationID=formatDF$locationid,
                  sourceName=fileName,
                  units = "mg/l",
                  isPrediction=F,
                  addMetric=T)
    }
    
    if("WaterTemp" %in% names(formatDF)){
      dbWriteData(metric="water wemperature",
                  value=formatDF$WaterTemp,
                  datetime = formatDF$Date,
                  locationID=formatDF$locationid,
                  sourceName=fileName,
                  units = "c",
                  isPrediction=F,
                  addMetric=T)
    }
  }
  
}


pbsapply(dbIntakeKey$fileName, dbWriteIntakeFile_1)
