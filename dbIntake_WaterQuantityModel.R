library(sf)
library(RPostgres)
library(DBI)

#tidyverse packege for xl:
#library(readxl)

source("~/R/projects/SilverCreekDB/dbIntakeTools.R")
conn=scdbConnect(readOnly = F)

basePath="C:\\Users\\sam\\Dropbox\\NIFA Project\\DB_Intake\\QuantityModel\\"

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

#write streamflow data
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
