library(sf)
library(RPostgres)
library(DBI)

#tidyverse packege for xl:
library(readxl)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

basePath="C:\\Users\\sam\\Dropbox\\NIFA Project\\DB_Intake\\Hydrology\\"

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
  
  xlDF=data.frame(`Site Number`=numeric(),Date=character(),`Depth to Groundwater`=numeric(),`Measurement Type`=character(),locationID=numeric())
  for(s in locationSheets){
    #print(s)
    outDF=sheetDataGrabber(xlFile,s)
    #print(names(outDF))
    rbind(xlDF,outDF)
  }
  outDF=outDF[complete.cases(outDF),]
  return(outDF)
}
xlName="WRV_IDWR_Groundwater_Level_Continuous_Well_data_TABULAR.xlsx"
gw_1=paste0(basePath,xlName)

df_1=compileContinuousGWSheetsToDF(gw_1)



dbWriteData(metric="Depth to Groundwater",
            value=df_1$`Depth to Groundwater`,
            datetime=df_1$Date,
            locationID=df_1$locationID,
            sourceName="WRV_IDWR_Groundwater_Level_Continuous_Well_data_TABULAR.xlsx",
            units="Feet below ground surface",addMetric = T)




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
    outDF$locationID=sheetName
    return(outDF)
  }
  
  xlDF=data.frame(`Site Number`=numeric(),Date=character(),`Depth to Groundwater`=numeric(),`Measurement Type`=character(),locationID=numeric())
  for(s in locationSheets){
    outDF=sheetDataGrabber(xlFile,s)
    rbind(xlDF,outDF)
  }
  outDF=outDF[complete.cases(outDF),]
  return(outDF)
}

xlName="WRV_IDWR_Groundwater_Level_Manual_Well_data_TABULAR.xlsx"
gw_2=paste0(basePath,xlName)
df_2=compileManualGWSheetsToDF(gw_2)


dbWriteData(metric="Depth to Groundwater",
            value=df_2$`Depth to Groundwater`,
            datetime=df_2$Date,
            locationID=df_2$locationID,
            sourceName=xlName)

