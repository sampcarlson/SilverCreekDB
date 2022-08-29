library(sf)
library(RPostgres)
library(DBI)



#tidyverse packege for xl:
library(readxl)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

basePath="C:\\Users\\sam\\Dropbox\\NIFA Project\\DB_Intake\\QuantityModel\\"



######## write usgs sites from csv-------------
usgsSites=read.csv(paste0(basePath,"usgs_sites.csv"))
usgsSites=st_as_sf(usgsSites, coords = c("dec_long_va","dec_lat_va"),crs=st_crs(4326)) #in 4326.  
#Note that it can be written to db in any crs using srid arg to dbWritePoints, 
#but I'm trying to standardize to 26911
usgsSites=st_transform(usgsSites,st_crs(26911)) #convert to 26911

dbWritePoints(usgsSites, 
              locationNameCol="station_nm", 
              sourceNoteCol = "USGS streamflow gauges", 
              siteNoteCol = "abv", 
              source_siteIDCol = "site_no")


######### write snotel sites from csv-------------
snotelSites=read.csv(paste0(basePath,"snotel_sites.csv"))
snotelSites=st_as_sf(snotelSites,coords=c("long","lat"),crs=st_crs(4326))
snotelSites=st_transform(snotelSites,st_crs(26911))

dbWritePoints(snotelSites,
              locationNameCol = "site_name",
              sourceNoteCol = "Snotel Sites",
              siteNoteCol="abv",
              source_siteIDCol = "id")


##########manually write pici and fafi agrimet locations
#https://www.usbr.gov/pn/agrimet/agrimetmap/picida.html
#Latitude: 43.31166 N
#Longitude: -114.16583 W

#https://www.usbr.gov/pn/agrimet/agrimetmap/fafida.html
#Latitude: 43.31305 N
#Longitude: -114.82222 W

agriMetSites=data.frame(name=c("Picabo AgriMet station","Fairfield AgriMet station"),
                        id=c("pici","fafi"),
                        lat=c(43.31166,43.31305),
                        long=c(-114.16583,-114.82222))
agriMetSites=st_as_sf(agriMetSites,coords=c("long","lat"),crs=st_crs(4326))
agriMetSites=st_transform(agriMetSites,st_crs(26911))


dbWritePoints(agriMetSites,
              locationNameCol="name",
              sourceNoteCol="AgriMet",
              siteNoteCol=" ",
              source_siteIDCol = "id")

######## check if sites in data tracker present in DB
dataDefs=read_excel(paste0(basePath,"data_tracker.xlsx"),"definitions")
allModelLocations=unique(dataDefs[,c("site","source_site_ID")])


###check for missing locations:-----------
allModelLocations[!allModelLocations$source_site_ID %in% dbGetQuery(conn,"SELECT source_site_id FROM locations")$source_site_id,]


#######write streamflow data-------------
streamflowData=read.csv(paste0(basePath,"streamflow_data.csv"))

locationIDs=dbGetQuery(conn,paste0("SELECT locationid, source_site_id FROM locations WHERE locations.source_site_id IN ('",
                       paste0(unique(streamflowData$site_no),collapse="', '"),"');"))
streamflowData=merge(streamflowData,locationIDs,by.x="site_no",by.y="source_site_id")

dbWriteData(metric="Flow",
            value=streamflowData$Flow,
            datetime = streamflowData$Date,
            locationID = streamflowData$locationid,
            sourceName="streamflow_data.csv",
            units="cfs",addMetric=T)  


dbWriteData(metric="Diversion flow",
            value=streamflowData$sc.div,
            datetime = streamflowData$Date,
            locationID = streamflowData$locationid,
            sourceName="streamflow_data.csv",
            units="cfs",addMetric=T)  



###########------------ write agrimet air temperature data--------
agriMetData=read.csv(paste0(basePath,"agri_metT.csv"))
agriMetData=merge(agriMetData,allModelLocations,by.x="site_name",by.y="site",all.x = T)

locationIDs=dbGetQuery(conn,paste0("SELECT locationid, source_site_id FROM locations WHERE locations.source_site_id IN ('",
                                   paste0(unique(agriMetData$source_site_ID),collapse="', '"),"');"))
agriMetData=merge(agriMetData,locationIDs,by.x="source_site_ID",by.y="source_site_id",all.x=T)

convertFtoC=function(f){
  c=5*(f-32)/9
  return(c)
}

agriMetData$airTempC=convertFtoC(agriMetData$t)

dbWriteData(metric="Air Temperature",
            value=agriMetData$airTempC,
            datetime = agriMetData$date_time,
            locationID=agriMetData$locationid,
            sourceName="agri_metT.csv",
            units="C",
            addMetric=T)


## mean April - June predicted temperature
ajSnotelTemps=read.csv(paste0(basePath,"aj_pred_temps_long.csv"))

#use sitename -> siteID definitions from datadefs (data_tracker.xlsx)
siteDefs=unique(dataDefs[,1:3])
names(siteDefs)[1]="site_abbr"

#check for unclear a-j sites:
unique(ajSnotelTemps$site[!ajSnotelTemps$site %in% siteDefs$site_abbr])

ajSnotelTemps=merge(ajSnotelTemps,siteDefs,by.x=("site"),by.y="site_abbr")
locationIDs=dbGetQuery(conn,paste0("SELECT locationid, source_site_id FROM locations WHERE locations.source_site_id IN ('",
                                   paste0(unique(ajSnotelTemps$source_site_ID),collapse="', '"),"');"))
ajSnotelTemps=merge(ajSnotelTemps,locationIDs,by.x="source_site_ID",by.y="source_site_id")

ajSnotelTemps$meanTempC=convertFtoC(ajSnotelTemps$aj.mean.t)

#record date as April 1st of the year the data is written? 
ajRecordDate=paste0(format.Date(Sys.Date(),"%Y"),"-04-01")

dbWriteData(metric="Mean April - June Air Temperature",
            units = "C",
            addMetric = T,
            value=ajSnotelTemps$meanTempC,
            datetime=ajRecordDate,
            locationID=ajSnotelTemps$locationid,
            sourceName="aj_pred_temps_long.csv")



### write allVarsLong to db:------------