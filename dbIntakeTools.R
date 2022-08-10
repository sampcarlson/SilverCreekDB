
scdbConnect=function(readOnly=T){
  if(readOnly){
    conn=dbConnect(RPostgres::Postgres(),
                   host="silvercreekdb-do-user-12108041-0.b.db.ondigitalocean.com",
                   port="25060",
                   dbname="silvercreekdb" ,
                   user="dbread",
                   password=Sys.getenv("scdb_readPass")
    )
  } else {
    conn=dbConnect(RPostgres::Postgres(),
                   host="silvercreekdb-do-user-12108041-0.b.db.ondigitalocean.com",
                   port="25060",
                   dbname="silvercreekdb" ,
                   user="dbwrite",
                   password=Sys.getenv("scdb_pass")
    )
  }
  return(conn)
}

escapeSingleQuote=function(df){
  df[]=lapply(df,function (X) {gsub( "'","''",X)})
  return(df)
}

writeLog=function(x,basePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/',logFileName='_log.txt'){
  if(!file.exists(paste0(basePath,logFileName))){
    write(paste("Log created on",Sys.time()),file=paste0(basePath,logFileName),append=F)
  }
  write(paste(Sys.time(),">>",x),file=paste0(basePath,logFileName),append=T)
}

dbWriteBatch=function(batch,notes=" ",include=T,dbHandle=conn){
  if(!batch %in% dbGetQuery(dbHandle,"SELECT batchID FROM batches;")$batchid){# not an id, assume it's a name
    
    batchRecord=dbGetQuery(conn,paste0("SELECT source FROM batches WHERE batches.source = '",batch,"';")) # get records (if present) for this batch name
    if(nrow(batchRecord)==0){ #batch is not present in table, write new record
      datetime=Sys.time()
      dbExecute(dbHandle,paste0("INSERT INTO batches (source, importdatetime, notes, include) VALUES ('",
                                batch,"', '",
                                datetime,"', '",
                                notes,"', '",
                                include,"');"))
    }
    batchID=dbGetQuery(dbHandle,paste0("SELECT batchid FROM batches WHERE batches.source = '",batch,"';"))$batchid[1]
    
  } else{
    batchid=source
  }
  return(batchID)
}

dbWriteData=function(metric,value,datetime,locationID,sourceName,units="",isPrediction=F,addMetric=F,dbHandle=conn){
  value=value[,1] #strip extra attributes
  
  #first check batches, locations, and metrics
  
  ##############check/write batch:---------------
  batchID=dbWriteBatch(sourceName)
  
  ##############check location:-------------
  if(!locationID %in% dbGetQuery(dbHandle,"SELECT locationid FROM locations;")$locationid){#location not correct
    stop(paste("locationID",locationID,"not present in locations table:"))
  }
  ##########check/write metric:--------------
  metricIsNumeric=suppressWarnings( {!is.na(as.numeric(metric))} ) #is metric numeric?
  
  if(addMetric & ( metricIsNumeric | units=="" )){ #if addMetric, check if required info is given
    stop(paste("named metric and units required to add new metric record"))
  } else if(!metricIsNumeric) {#if metric is not numeric
    #look for metric by name, get id
    metricID=dbGetQuery(dbHandle,paste0("SELECT metricid FROM metrics WHERE metrics.name = '",metric,"';"))$metricid
    metricName=metric 
    
    if(identical(metricID,integer(0)) & addMetric){#if no record of metric, and addMetric = T
      #go ahead and add it, and get the new id back
      dbExecute(dbHandle,paste0("INSERT INTO metrics (name, units, isprediction) VALUES ('",metric,"', '",
                                units,"', '",
                                isPrediction,"');"))
      metricID=dbGetQuery(dbHandle,paste0("SELECT metricid FROM metrics WHERE metrics.name = '",metric,"';"))$metricid
    }
    
  } else { #if metric is numeric
    #look for metric by id, get name
    metricName=dbGetQuery(dbHandle,paste0("SELECT name FROM metrics WHERE metrics.metricid = '",metric,"';"))$name
    metricID=metric
  } 
  if(!exists("metricID")){ #if the above lines did not define a metric
    print(paste("Metric", metric, "not found or added.  Known metrics:"))
    print(dbGetQuery(dbHandle,"SELECT * FROM metrics;"))
    stop("correct or set addMetric=T")
  } 
  
  writeMe=data.frame(value=value,metricName=metricName,datetime=datetime,metricID=metricID,locationID=locationID,batchID=batchID)
  
  ########write data:--------------
  #update to write whole table at once
  for(i in 1:nrow(writeMe)){
    query=paste0("INSERT INTO data (metric, value, datetime, metricid, locationid, batchid) VALUES ('",
                 writeMe$metricName[i],"', '",
                 as.numeric(writeMe$value[i]),"', '",
                 writeMe$datetime[i],"', '",
                 writeMe$metricID[i],"', '",
                 writeMe$locationID[i],"', '",
                 writeMe$batchID[i],"');")
    print(query)
    dbExecute(dbHandle,query)
  }
}

dbGetLocationID=function(source_site_id,sourceNote){
  location=dbGetQuery(conn,paste0("SELECT locationid, name, sitenote FROM locations WHERE locations.sourcenote='",sourceNote,"'
                                  AND locations.source_site_id = '",source_site_id,"';"))
  if(nrow(location)==1){
    return(location$locationid)
  } else {
    print("Location unclear.  Records found:")
    print(location)
    stop("Clarify or add location")
  }
}

parseIntakeFile=function(intakeFileName,metric='auto',basePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/',siteIndexFile="_siteIndex.csv",logFileName='_log.txt'){
  
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
  
  siteIndex=read.csv(paste0(basePath,siteIndexFile))
  
  thisSite=siteIndex$site[siteIndex$fileName==intakeFileName]
  
  formFile=data.frame(Date=as.POSIXlt(rawFile$Date,format="%m/%d/%y %I:%M:%S %p",tz="MST"),site=thisSite)
  
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

dbWritePoints=function(writeDF,locationNameCol="Name",sourceNoteCol="",siteNoteCol="siteNote",source_siteIDCol="ID",srid=26911){ #requires an sf object
  #remove Z coord to simplify coordinate retrieval
  writeDF=st_zm(writeDF)
  
  cleanDF=data.frame(writeDF)#strip sf attribute
  cleanDF$geom=NULL
  
  #rename or create by name function
  renameCol=function(df,oldName,newName){
    if(oldName %in% names(df)){
      df$temp=df[,names(df)==oldName]
    } else {
      df$temp = oldName
    }
    names(df)[names(df)=="temp"] = newName
    return(df)
  }
  
  cleanDF=renameCol(cleanDF,locationNameCol,"name")
  cleanDF=renameCol(cleanDF,sourceNoteCol,"sourcenote")
  cleanDF=renameCol(cleanDF,siteNoteCol,"sitenote")
  cleanDF=renameCol(cleanDF,source_siteIDCol,"source_site_id")
  
  cleanDF=escapeSingleQuote(cleanDF)
  
  cleanDF$point="0 0"
  
  #works with a single line only, iterate through...  could use dbWriteTable instead
  for (i in 1:nrow(cleanDF)){
    writeLine=cleanDF[i,]
    writeLine$point=paste0(st_coordinates(writeDF$geom[i])[1]," ",st_coordinates(writeDF$geom[i])[2])
    
    # print(writeLine)
    
    query=paste0("INSERT INTO locations (name, geometry, sourcenote, sitenote, source_site_id) VALUES ('",
                 writeLine$name,"', ",
                 "'SRID=",srid,";POINT(",writeLine$point,")', '",
                 writeLine$sourcenote,"', '",
                 writeLine$sitenote,"', '",
                 writeLine$source_site_id,"');" )
    
    print(query)
    
    dbExecute(conn,query)
    
  }
  
  #return(writeDF)
}
