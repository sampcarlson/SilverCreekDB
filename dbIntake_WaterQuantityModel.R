`library(sf)
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

dbWriteData(metric="flow",
            value=streamflowData$Flow,
            datetime = streamflowData$Date,
            locationID = streamflowData$locationid,
            sourceName="streamflow_data.csv",
            units="cfs",addMetric=T)  


dbWriteData(metric="diversion flow",
            value=streamflowData$sc.div,
            datetime = streamflowData$Date,
            locationID = streamflowData$locationid,
            sourceName="streamflow_data.csv",
            units="cfs",addMetric=T)  




########write snotel data ---------------------
snowData=read.csv(paste0(basePath,"snotel_data.csv"))
locationIDs=dbGetQuery(conn,paste0("SELECT locationid, source_site_id FROM locations WHERE locations.source_site_id IN ('",
                                   paste0(unique(snowData$site_id),collapse="', '"),"');"))
snowData=merge(snowData,locationIDs,by.x="site_id",by.y="source_site_id")

dbWriteData(metric="swe",units="in",addMetric = T,
            value=snowData$snow_water_equivalent,
            datetime=snowData$date,
            locationID=snowData$locationid,
            sourceName="snotel_data.csv")

dbWriteData(metric="max daily temperature",units="c",addMetric = T,
            value=snowData$temperature_max,
            datetime=snowData$date,
            locationID=snowData$locationid,
            sourceName="snotel_data.csv")

dbWriteData(metric="min daily temperature",units="c",addMetric = T,
            value=snowData$temperature_min,
            datetime=snowData$date,
            locationID=snowData$locationid,
            sourceName="snotel_data.csv")

dbWriteData(metric="mean daily temperature",units="c",addMetric = T,
            value=snowData$temperature_mean,
            datetime=snowData$date,
            locationID=snowData$locationid,
            sourceName="snotel_data.csv")


dbWriteData(metric="daily precip",units="in",addMetric = T,
            value=snowData$precipitation,
            datetime=snowData$date,
            locationID=snowData$locationid,
            sourceName="snotel_data.csv")


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

#agriMetData$airTempC=convertFtoC(agriMetData$t)

dbWriteData(metric="air temperature",
            value=agriMetData$t,
            datetime = agriMetData$date_time,
            locationID=agriMetData$locationid,
            sourceName="agri_metT.csv",
            units="f",
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

#ajSnotelTemps$meanTempC=convertFtoC(ajSnotelTemps$aj.mean.t)

#record date as April 1st of the year the data is written? 
ajRecordDate=paste0(format.Date(Sys.Date(),"%Y"),"-04-01")

dbWriteData(metric="mean april - june air temperature",
            units = "f",
            addMetric = T,
            value=ajSnotelTemps$aj.mean.t,
            datetime=ajRecordDate,
            locationID=ajSnotelTemps$locationid,
            sourceName="aj_pred_temps_long.csv")



### write allVarsLong to db:------------

allVarsLong=read.csv(paste0(basePath,"all_vars_long.csv"))
allVarsLong=merge(allVarsLong,dataDefs,by.x="variable",by.y="...1",all.x = T)

#drop those without source_site or metric
allVarsLong=allVarsLong[complete.cases(allVarsLong[,c("source_site_ID","metric","value")]),]
locationIDs=dbGetQuery(conn,paste0("SELECT locationid, source_site_id FROM locations WHERE locations.source_site_id IN ('",
                                   paste0(unique(allVarsLong$source_site_ID),collapse="', '"),"');"))
allVarsLong=merge(allVarsLong,locationIDs,by.x="source_site_ID",by.y="source_site_id")

getDateDF=data.frame(metric=c("center of mass (april 1 - july 31)",
                              "irrigation season volume (april 1 - september 31)",
                              "mean winter flow (Oct 1 - Jan 31)",
                              "mean april-june air temperature",
                              "mean november-january air temperature"),
                     addToYear=c("-04-01",
                                 "-04-01",
                                 "-10-01",
                                 "-04-01",
                                 "-11-01")
)

#########is nov - jan air temp year wrong?  ----------

allVarsLong=merge(allVarsLong,getDateDF,by.x="metric",by.y="metric")

for(m in unique(allVarsLong$metric)){
  writeMe=allVarsLong[allVarsLong$metric==m,]
  if(length(unique(writeMe$unit))>1){
    print(paste("metric:",m,"associated with multiple units:", unique(writeMe$unit)))
    stop("this is probably bad")
  }
  unit=writeMe$unit[1]
  
  #this assumes date is given as year only - not sure best way to handle this?
  writeMe$date=paste0(writeMe$year,writeMe$addToYear)
  
  dbWriteData(metric=m,
              units=unit,
              addMetric=T,
              value=writeMe$value,
              datetime = writeMe$date,
              locationID=writeMe$locationid,
              isPrediction = F,
              sourceName="all_vars_long.csv")
}


#########write bwh.flow.simLong-------------------
bwhSimFlow=read.csv(paste0(basePath,"bwh.flow.simLong.csv"))
#bwh = big wood hailey = bwb
#check that this looks ok?
thisLocation=dbGetQuery(conn,"SELECT * FROM locations WHERE source_site_id = '13139510';")
thisLocation

bwhSimFlow$simnumber=as.numeric(gsub('X','',bwhSimFlow$simulation))

dbWriteData(metric="predicted daily flow",isPrediction = T, units="cfs",addMetric = T,
            value=bwhSimFlow$dailyFlow,
            datetime=bwhSimFlow$X,
            locationID=thisLocation$locationid,
            simnumber = bwhSimFlow$simnumber,
            sourceName="bwh.flow.simLong.csv")




#########write bws.flow.simLong-------------------
bwsSimFlow=read.csv(paste0(basePath,"bws.flow.simLong.csv"))

#check that this looks ok?
thisLocation=dbGetQuery(conn,"SELECT * FROM locations WHERE source_site_id = '13140800';")
thisLocation

bwsSimFlow$simnumber=as.numeric(gsub('X','',bwsSimFlow$simulation))

dbWriteData(metric="predicted daily flow",isPrediction = T, units="cfs",
            value=bwsSimFlow$dailyFlow,
            datetime=bwsSimFlow$X,
            locationID=thisLocation$locationid,
            simnumber = bwsSimFlow$simnumber,
            sourceName="bws.flow.simLong.csv")




#########write cc.flow.simLong-------------------
ccSimFlow=read.csv(paste0(basePath,"cc.flow.simLong.csv"))

#check that this looks ok?
thisLocation=dbGetQuery(conn,"SELECT * FROM locations WHERE source_site_id = '13141500';")
thisLocation

ccSimFlow$simnumber=as.numeric(gsub('X','',ccSimFlow$simulation))

dbWriteData(metric="predicted daily flow",isPrediction = T, units="cfs",
            value=ccSimFlow$dailyFlow,
            datetime=ccSimFlow$X,
            locationID=thisLocation$locationid,
            simnumber = ccSimFlow$simnumber,
            sourceName="cc.flow.simLong.csv")




#########write cc.flow.simLong-------------------
scSimFlow=read.csv(paste0(basePath,"sc.flow.simLong.csv"))

#check that this looks ok?
thisLocation=dbGetQuery(conn,"SELECT * FROM locations WHERE source_site_id = '13150430';")
thisLocation

scSimFlow$simnumber=as.numeric(gsub('X','',scSimFlow$simulation))
dbWriteData(metric="predicted daily flow",isPrediction = T, units="cfs",
            value=scSimFlow$dailyFlow,
            datetime=scSimFlow$X,
            locationID=thisLocation$locationid,
            simnumber = scSimFlow$simnumber,
            sourceName="sc.flow.simLong.csv")



##### write colume sample long-----------
volSample=read.csv(paste0(basePath,"volumes.sampleLong.csv"))
volSample=merge(volSample,siteDefs,by.x="site_name",by.y="site_abbr")
locationIDs=dbGetQuery(conn,paste0("SELECT locationid, source_site_id FROM locations WHERE locations.source_site_id IN ('",
                                   paste0(unique(volSample$source_site_ID),collapse="', '"),"');"))
volSample=merge(volSample,locationIDs,by.x="source_site_ID",by.y="source_site_id")



##########this inserts arbitrary simulation numbers that may not match up with other uses of simNumber
volSample$simNumber=0

volSample$simNumber[volSample$site_name=="bwb.vol"]=1:5000
volSample$simNumber[volSample$site_name=="bws.vol"]=1:5000
volSample$simNumber[volSample$site_name=="cc.vol"]=1:5000
volSample$simNumber[volSample$site_name=="sc.vol"]=1:5000

thisDateTime=paste0(format.Date(Sys.Date(),"%Y"),"-04-01")


dbWriteData(metric="simulated irrigation season volume (april 1 - september 31)", units = "acre-feet", isPrediction = T, addMetric = T,
            value=volSample$vol_af,
            datetime=thisDateTime,
            locationID=volSample$locationid,
            simnumber = volSample$simNumber,
            sourceName = "volumes.sampleLong.csv")
