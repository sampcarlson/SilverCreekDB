
dbConnect=function(readOnly=T){
  if(readOnly){
    scdb=dbConnect(RPostgres::Postgres(),
                   host="silvercreekdb-do-user-12108041-0.b.db.ondigitalocean.com",
                   port="25060",
                   dbname="silvercreekdb" ,
                   user="dbread",
                   Sys.getenv("scdb_readpass")
    )
  } else {
    scdb=dbConnect(RPostgres::Postgres(),
                   host="silvercreekdb-do-user-12108041-0.b.db.ondigitalocean.com",
                   port="25060",
                   dbname="silvercreekdb" ,
                   user="dbwrite",
                   password=Sys.getenv("scdb_pass")
    )
  }
  return(scdb)
}

writeLog=function(x,basePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/',logFileName='_log.txt'){
  if(!file.exists(paste0(basePath,logFileName))){
    write(paste("Log created on",Sys.time()),file=paste0(basePath,logFileName),append=F)
  }
  write(paste(Sys.time(),">>",x),file=paste0(basePath,logFileName),append=T)
}

writeBatch=function(source,notes=" ",include=T,dbHandle=scdb){
  if(!batch %in% dbGetQuery(dbHandle,"SELECT batchID FROM batches;")$batchID){# not an id, assume it is a name
    datetime=Sys.time()
    dbExecute(dbHandle,paste0("INSERT INTO batches (source, importdatetime, notes, include) VALUES ('",
                              batch,"', '",
                              datetime,"', '",
                              notes,"', '",
                              include,"');"))
    batchid=dbGetQuery(dbHandle,paste0("SELECT batchid FROM batches WHERE batches.source = '",source,"';"))$batchid
  } else(
    batchid=source
  )
  return(batchid)
}

writeData=function(metric,value,datetime,locationID,source,addMetric=F,units="",dbHandle=scdb){
  #first check batches, locations, and metrics
  
  ##############check/write batch:---------------
  batchID=writeBatch(source)
  
  ##############check location:-------------
  if(!locationID %in% dbGetQuery(dbHandle,"SELECT locationid FROM locations;")$locationID){#location not correct
    stop(paste("locationID",locationID,"not present in locations table:"))
  }
  ##########check/write metric:--------------
  metricIsNumeric=suppressWarnings( {!is.na(as.numeric(metric))} ) #is metric numeric?
  
  if(addMetric & ( metricIsNumeric | units=="" )){ #if addMetric, check if required info is given
    stop(paste("named metric and units required to add new metric record"))
  } else if(metricIsNumeric) {#if metric is not numeric
    #look for metric by name, get id
    metricID=dbGetQuery(dbHandle,paste0("SELECT metricid FROM metrics WHERE metrics.name = '",metric,"';"))$metricid
    metricName=metric 
    
    if(identical(metricID,integer(0)) & addMetric){#if no record of metric, and addMetric = T
      #go ahead and add it, and get the new id back
      dbExecute(dbHandle,paste0("INSERT INTO metrics (name, units) VALUES ('",metric,"', '",units,"');"))
      metricID=dbGetQuery(dbHandle,paste0("SELECT metricid FROM metrics WHERE metrics.name = '",metric,"';"))$metricid
    }
    
  } else { #if metric is numeric
    #look for metric by id, get name
    metricName=dbGetQuery(dbHandle,paste0("SELECT name FROM metrics WHERE metrics.metricid = '",metric,"';"))$name
    metricID=metric
  } 
  if(!exists(metricID)){ #if the above lines did not define a metric
    print(paste("Metric", metric, "not found or added.  Known metrics:"))
    print(dbGetQuery(dbHandle,"SELECT * FROM metrics;"))
    stop("correct or add metric")
  } 
  
  
  
  ########write data:--------------
  dbExecute(dbHandle,paste0("INSERT INTO data (metric, value, datetime, metricid, locationid, batchid) VALUES (' ",
                            metricName,"', '",
                            as.numeric(value),"', '",
                            datetime,"', '",
                            metricID,"', '",
                            locationID,"', '",
                            batchID,"');"))
  
}

parseIntakeFile=function(intakeFileName,site,metric='auto',basePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/',logFileName='_log.txt'){
  
  skip=0
  done=F
  while(!done){
    
    rawFile=read.csv(paste0(basePath,intakeFileName),header=F,skip=skip)
    if( all( nchar( rawFile[1,] )>0 ) ){
      rawFile=read.csv(paste0(basePath,intakeFileName),header=T,skip=skip,blank.lines.skip = T)
      done=T
    } else { skip=skip+1 }
    
    if(skip>100){
      writeLog(paste('error reading',intakeFileName))
      done=T
      rawFile=""
    }
  }
  
  writeLog(paste('Read file',intakeFileName,'with names',paste(names(rawFile),collapse = ', ')))
  
  dateColIdx=which(grepl("Date",names(rawFile)))
  names(rawFile)[dateColIdx]="Date"
  
  formFile=data.frame(Date=as.POSIXlt(rawFile$Date,format="%m/%d/%y %I:%M:%S %p",tz="MST"),site=site)
  
  if(metric=='auto'){
    
    if(sum(grepl("Temp",names(rawFile)))==1){
      tempColIDX=which(grepl("Temp",names(rawFile)))
      writeLog(paste("interpreted",names(rawFile)[tempColIDX],'as Water Temperature (c)'))
      names(rawFile)[tempColIDX]="WaterTemp"
      formFile$WaterTemp=rawFile[,tempColIDX]
    }
    
    if(sum(grepl("DO",names(rawFile)))==1){
      DOColIDX=which(grepl("DO",names(rawFile)))
      writeLog(paste("interpreted",names(rawFile)[DOColIDX],'as DO (mg/L)'))
      names(rawFile)[DOColIDX]="DO"
      formFile$DO=rawFile[DOColIDX]
      
    }
  }
  return(formFile)
}


joinToSpatialObject=function(dataDF,sourceID_name,sourceFile="source_TemperatureLocations.gpkg"){
  basePath="C:/Users/sam/Dropbox/NIFA Project/DB_Intake/SpatialSource"
  sourcePath=paste(basePath,sourceFile,sep="/")
  
  spatSource=st_read(sourcePath)
}


writePoints=function(writeDf,locationNameCol="Name",sourceNoteCol="temperatureLocations_source",siteNoteCol="siteNote",srid=26911){ #requires an sf object
  
  #rename or create by name function
  renameCol=function(df,oldName,newName){
    if(oldName %in% names(df)){
      names(df)[names(df)==oldName] = newName
    } else {
      df$temp = oldName
      names(df)[names(df)=="temp"] = newName
    }
    return(df)
  }
  
  writeDf=renameCol(writeDf,locationNameCol,"name")
  writeDf=renameCol(writeDf,sourceNoteCol,"sourcenote")
  writeDf=renameCol(writeDf,siteNoteCol,"sitenote")
  
  writeDf=st_zm(writeDf)
  
  
  #works with a single line only, iterate through...  could use dbWriteTable instead?
  for (i in 1:nrow(writeDf)){
    writeLine=writeDf[i,]
    writeLine$point=paste0(st_coordinates(writeDf$geom[i])[1],", ",st_coordinates(writeDf$geom[i])[2])
    
    
    query=paste0("INSERT INTO locations (name, geometry, sourcenote, sitenote) VALUES ('",
                 writeLine$name,"', '",
                 "ST_SetSRID(ST_MakePoint(",writeLine$point,"), ",srid,"', '",
                 writeLine$sourcenote,"', '",
                 writeLine$sitenote,"');" )
    
    print(query)
    
    dbExecute(con,query)
    
  }
  
  return(writeDf)
}
