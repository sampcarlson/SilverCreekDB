dbIntakePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/'
dbIntakeKey=read.csv(paste0(dbIntakePath,"_siteIndex.csv"))

writeLog=function(x,basePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/',logFileName='_log.txt'){
  if(!file.exists(paste0(basePath,logFileName))){
    write(paste("Log created on",Sys.time()),file=paste0(basePath,logFileName),append=F)
  }
  write(paste(Sys.time(),">>",x),file=paste0(basePath,logFileName),append=T)
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

formFile=parseIntakeFile(dbIntakeKey$fileName[1],site=1)
