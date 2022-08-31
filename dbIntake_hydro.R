library(sf)
library(RPostgres)
library(DBI)

#tidyverse packege for xl:
library(readxl)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

gwPoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_GWLocations.gpkg")
dbWritePoints(gwPoints,locationNameCol = "SiteName", sourceNoteCol = "source_GWLocations.gpkg",siteNoteCol="WaterUse",source_siteIDCol = "WellNumber")
#write total depth or instrument to data?

swPoints=st_read("C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource/source_SWLocations.gpkg")
dbWritePoints(swPoints,locationNameCol = "STANAME", sourceNoteCol = "source_SWLocations.gpkg",siteNoteCol="STAID",source_siteIDCol = "STAID")




basePath="C:\\Users\\sam\\Dropbox\\NIFA Project\\DB_Intake\\Hydrology\\"


######Write Continuous GW level to db -----------

compileContinuousGWSheetsToDF=function(xlFile){
  
  sheets = excel_sheets(xlFile)
  locationsDF=dbGetQuery(conn,"SELECT locationid, name, sourcenote, source_site_id FROM locations;")
  locationSheets=sheets[sheets %in% locationsDF$source_site_id]
  notLocationSheets=sheets[!sheets %in% locationSheets]
  
  print(paste("Sheets not referenced to location:",notLocationSheets))
  
  sheetDataGrabber=function(xlFile,sheetName){
    print(sheetName)
    thisSheet=read_excel(path=xlFile,sheet=sheetName,.name_repair = "minimal")
    #print(head(thisSheet))
    if(identical( names(thisSheet)[1:4], c("Site Number","Date","Continuous  Depth to Groundwater","Measurement Type")) | 
       identical( names(thisSheet)[1:4], c("Site Number","Date","Continuous Depth to Groundwater","Measurement Type"))){
      outDF=thisSheet[,1:4]
      names(outDF)[3] = "Depth to Groundwater"
      #print(head(outDF))
    } 
    if(identical( names(thisSheet)[6:9], c("Site Number","Date","Discrete  Depth to Groundwater","Measurement Type")) | 
       identical( names(thisSheet)[6:9], c("Site Number","Date","Discrete Depth to Groundwater","Measurement Type"))){
      tempDF=thisSheet[,6:9]
      names(tempDF)[3] = "Depth to Groundwater"
      #print(head(tempDF))
      outDF=rbind(outDF,tempDF)
    }
    
    
    locationID=dbGetQuery(conn,paste0("SELECT * FROM locations WHERE locations.source_site_id = '",sheetName,"';"))$locationid
    outDF$locationID=locationID
    return(outDF)
  }
  
  outDF=data.frame(`Site Number`=numeric(),Date=character(),`Depth to Groundwater`=numeric(),`Measurement Type`=character(),locationID=numeric())
  for(s in locationSheets){
    #print(s)
    addDF=sheetDataGrabber(xlFile,s)
    #print(names(outDF))
    outDF=rbind(outDF,addDF)
  }
  outDF=outDF[complete.cases(outDF),]
  return(outDF)
}
xlName="WRV_IDWR_Groundwater_Level_Continuous_Well_data_TABULAR.xlsx"
gw_1=paste0(basePath,xlName)

df_1=compileContinuousGWSheetsToDF(gw_1)



dbWriteData(metric="depth to groundwater",
            value=df_1$`Depth to Groundwater`,
            datetime=df_1$Date,
            locationID=df_1$locationID,
            sourceName="WRV_IDWR_Groundwater_Level_Continuous_Well_data_TABULAR.xlsx",
            units="feet below ground surface",addMetric = T)


##############Write manual gw level to db ---------------

compileManualGWSheetsToDF=function(xlFile){
  
  sheets = excel_sheets(xlFile)
  locationsDF=dbGetQuery(conn,"SELECT locationid, name, sourcenote, source_site_id FROM locations;")
  locationSheets=sheets[sheets %in% locationsDF$source_site_id]
  notLocationSheets=sheets[!sheets %in% locationSheets]
  print(paste("Sheets not referenced to location:",notLocationSheets))
  
  sheetDataGrabber=function(xlFile,sheetName){
    print(sheetName)
    thisSheet=read_excel(path=xlFile,sheet=sheetName,.name_repair = "minimal")
    
    outDF=thisSheet[,c("Site Number", "Date", "Manual Depth to Groundwater", "Measurement Type")]
    names(outDF)[3]="Depth to Groundwater"
    locationID=dbGetQuery(conn,paste0("SELECT * FROM locations WHERE locations.source_site_id = '",sheetName,"';"))$locationid
    outDF$locationID=locationID
    return(outDF)
  }
  
  outDF=data.frame(`Site Number`=numeric(),Date=character(),`Depth to Groundwater`=numeric(),`Measurement Type`=character(),locationID=numeric())
  for(s in locationSheets){
    addDF=sheetDataGrabber(xlFile,s)
    outDF=rbind(outDF,addDF)
  }
  outDF=outDF[complete.cases(outDF),]
  return(outDF)
}

xlName="WRV_IDWR_Groundwater_Level_Manual_Well_data_TABULAR.xlsx"
gw_2=paste0(basePath,xlName)
df_2=compileManualGWSheetsToDF(gw_2)


dbWriteData(metric="depth to groundwater",
            value=df_2$`Depth to Groundwater`,
            datetime=df_2$Date,
            locationID=df_2$locationID,
            sourceName=xlName)



#############Write SVGWD data to db:---------------
compileSVGWDSheetsToDF=function(xlFile){
  
  sheets = excel_sheets(xlFile)
  
  trackTable=read_excel(xlFile,"Tracking",skip=5,.name_repair = "minimal")
  nameDefs=data.frame(ES_ID=trackTable$`Ecosystem Sciences Well ID Number`, SRC_ID=trackTable$`Well Number`)
  
  sheetNamesDf=data.frame(sheetName=sheets)
  sheetNamesDf=merge(sheetNamesDf,nameDefs,by.x="sheetName",by.y="ES_ID",all.x=T)
  
  locationsDF=dbGetQuery(conn,"SELECT locationid, name, sourcenote, source_site_id FROM locations;")
  locationSheets=sheetNamesDf[sheetNamesDf$SRC_ID %in% locationsDF$source_site_id,]
  notLocationSheets=sheetNamesDf[!sheetNamesDf$SRC_ID %in% locationsDF$source_site_id,]
  print(paste("Sheets not referenced to location:",paste(notLocationSheets,collapse=", ")))
  print("Sheets referenced to location:")
  
  sheetDataGrabber=function(xlFile,sheetName,siteID){
    print(sheetName)
    thisSheet=as.data.frame(read_excel(path=xlFile,sheet=sheetName))
    
    thisSheet=rbind(data.frame(WellNumber=thisSheet$WellNumber...1, 
                               Date=thisSheet$Date...3,
                               DepthToGW=thisSheet$DepthToGW...4,
                               MeasureType=thisSheet$MeasurementType...5),
                    data.frame(WellNumber=thisSheet$WellNumber...7, 
                               Date=thisSheet$Date...9,
                               DepthToGW=thisSheet$DepthToGW...10,
                               MeasureType=thisSheet$MeasurementType...11)
    )
    
    outDF=thisSheet[,c("WellNumber", "Date", "DepthToGW", "MeasureType")]
    
    
    outDF$Date=as.Date(outDF$Date,format="%Y-%m-%d")
    
    locationID=dbGetQuery(conn,paste0("SELECT * FROM locations WHERE locations.source_site_id = '",siteID,"';"))$locationid
    outDF$locationID=locationID
    return(outDF)
  }
  
  outDF=data.frame(`WellNumber`=character(),`Date`=character(),`DepthToGW`=numeric(),`locationID`=numeric())
  for(i in 1:nrow(locationSheets)){
    addDF=sheetDataGrabber(xlFile,sheetName=locationSheets$sheetName[i],siteID=locationSheets$SRC_ID[i])
    outDF=rbind(outDF,addDF)
  }
  #outDF=outDF[complete.cases(outDF),]
  return(outDF)
}

xlName="WRV_SVGWD_Groundwater_Level_Well_data_TABULAR.xlsx"
gw_3=paste0(basePath,xlName)
gw_3=compileSVGWDSheetsToDF(gw_3)

dbWriteData(metric="depth to groundwater",
            value=gw_3$DepthToGW,
            datetime=gw_3$Date,
            locationID=gw_3$locationID,
            sourceName=xlName)


########Write surface water data to db:-----------------
compileSWSheetsToDF=function(xlFile){
  
  sheets = excel_sheets(xlFile)
  locationsDF=dbGetQuery(conn,"SELECT locationid, name, sourcenote, source_site_id FROM locations;")
  locationSheets=sheets[sheets %in% locationsDF$source_site_id]
  notLocationSheets=sheets[!sheets %in% locationSheets]
  print(paste("Sheets not referenced to location:",notLocationSheets))
  print("Sheets referenced to location:")
  
  sheetDataGrabber=function(xlFile,sheetName){
    print(sheetName)
    thisSheet=as.data.frame(read_excel(path=xlFile,sheet=sheetName,.name_repair = "minimal"))
    
    outDF=thisSheet[,c("STANAME", "STAID", "Date", "Discharge")]
    names(outDF)[4]="Flow"
    
    outDF$Date=as.Date(outDF$Date,format="%Y-%m-%d")
    
    locationID=dbGetQuery(conn,paste0("SELECT * FROM locations WHERE locations.source_site_id = '",sheetName,"';"))$locationid
    outDF$locationID=locationID
    return(outDF)
  }
  
  outDF=data.frame(`STANAME`=character(),`SATID`=character(),`Date`=character(),`Flow`=numeric(),`locationID`=numeric())
  for(s in locationSheets){
    addDF=sheetDataGrabber(xlFile,s)
    outDF=rbind(outDF,addDF)
  }
  #outDF=outDF[complete.cases(outDF),]
  return(outDF)
}

xlName="WRV_Surface_Hydrology_data_TABULAR.xlsx"
sw_1=paste0(basePath,xlName)
df_3=compileSWSheetsToDF(sw_1)

dbWriteData(metric="flow",
            value=df_3$Flow,
            datetime = df_3$Date,
            locationID=df_3$locationID,
            sourceName = xlName,
            addMetric = T, units="cfs")



#diversions data not yet added
