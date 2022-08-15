library(sf)
library(RPostgres)
library(DBI)

#tidyverse packege for xl:
library(readxl)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

basePath="C:\\Users\\sam\\Dropbox\\NIFA Project\\DB_Intake\\Hydrology\\"

compileGWSheetsToDF=function(xlFile){
  
  sheets = excel_sheets(xlFile)
  locationsDF=dbGetQuery(conn,"SELECT locationid, name, sourcenote, source_site_id FROM locations;")
  locationSheets=sheets[sheets %in% locationsDF$source_site_id]
  
  
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
wkshtName="WRV_IDWR_Groundwater_Level_Continuous_Well_data_TABULAR.xlsx"
gw_1=paste0(basePath,)

df_1=compileGWSheetsToDF(xlFile)


dbWriteData(metric="Depth to Groundwater",
            value=df_1$`Depth to Groundwater`,
            datetime=df_1$Date,
            locationID=df_1$locationID,
            sourceName="WRV_IDWR_Groundwater_Level_Continuous_Well_data_TABULAR.xlsx",
            units="Feet below ground surface",addMetric = T)



