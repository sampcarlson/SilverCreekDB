BasePath="C:\\Users\\sam\\Documents\\SilverCreek\\ES_Data_Dump"

list.files(BasePath)

isNumeric=function(x){
  suppressWarnings(
    !is.na(as.numeric(x))
  )
}

nameAndWrite=function(prepend="",path=BasePath,outPath=paste0(BasePath,"\\Out")){
  filesAndDirs=list.files(path)
  isFile=grepl(pattern= '.+\\..{2,4}',filesAndDirs)
  files=filesAndDirs[isFile]
  dirs=filesAndDirs[!isFile]
  
  for(f in files){
    #test if name has site (seems to be a convention here)
    if (grepl('[0-9]{1,2}-[0-9]{1,2}-[0-9]{1,2}-[0-9]{2,4}-',f)){
      thisSite=strsplit(f,split="-")[[1]][1]
      
      print(paste("Move ",paste0(path,"\\",f),"to",paste0(outPath,"\\",paste0(prepend,"site",thisSite,"_"),f)))
      print(" ")
      file.copy(from=paste0(path,"\\",f),to=paste0(outPath,"\\",paste0(prepend,"site",thisSite,"_"),f),overwrite=T)
      
    }
    
    file.copy(from=paste0(path,"\\",f),to=paste0(outPath,"\\",prepend,f),overwrite=T)
    print(paste("Move ",paste0(path,"\\",f),"to",paste0(outPath,"\\",prepend,f)))
    print(" ")
  }
  
  for(d in dirs){
    if(d %in% c("Dissolved_oxygen_files")){
      nameAndWrite(prepend="DO_",path=paste0(path,"\\",d))
    } 
    if(d %in% c("Temperature_files")){
      nameAndWrite(prepend=paste0(prepend,"WaterTemp_"),path=paste0(path,"\\",d))
    }
    if(isNumeric(d)){
      d=as.numeric(d)
      if(d > 2000){  # min year
        nameAndWrite(prepend=paste0(prepend,"year",as.character(d),"_"),path=paste0(path,"\\",d))
      } else {
        nameAndWrite(prepend=paste0(prepend,"site",as.character(d),"_"),path=paste0(path,"\\",d))
      }
    }
  }
}

nameAndWrite()