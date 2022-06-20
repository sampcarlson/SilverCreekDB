DbIntakePath='C:/Users/sam/Dropbox/NIFA Project/DB_Intake/'

#files starting with '_' are ignored
allFiles=list.files(DbIntakePath,pattern='')
isMeta=grepl('^_',allFiles)
isFile=grepl('.+\\..{2,4}',allFiles)
isDbInput=isFile & !isMeta

dbInputKey=data.frame(fileName=allFiles[isDbInput])

getSite=function(fileName){
  thisSite=NA
  if(grepl('_site',fileName)){
    tail=strsplit(fileName,'_site',fixed=T)[[1]][2]
    thisSite=strsplit(tail,'[_-]')[[1]][1]
    
  } else if( length(gregexpr('[_-][0-9]+',fileName)[[1]]) == 1 ) { #only one number following a _ or -
    siteCharIndex=regexpr('[_-][0-9]+',fileName) #get first match
    thisSite=substr(x=fileName,start=siteCharIndex+1,stop=siteCharIndex+attr(siteCharIndex,'match.length')-1)
    
  } else if (grepl('[_-][0-9]+[_-][0-9]+[_-][0-9]+[_-][0-9]+[_-]',fileName)){ #4 numbers separated by [_-] in sequence, first is site
    siteCharIndex=regexpr('[_-][0-9]+',fileName) #get first match
    thisSite=substr(x=fileName,start=siteCharIndex+1,stop=siteCharIndex+attr(siteCharIndex,'match.length')-1)
    
  }#else if(grepl('#',fileName)) {
  #siteCharIndex=regexpr('#[0-9]{1,2}',fileName)
  #
  #}
  return(thisSite)
}


dbInputKey$site=sapply(dbInputKey$fileName,getSite)

file.copy(from=paste0(DbIntakePath,allFiles[isMeta]),to=paste0(DbIntakePath,"_backup_",Sys.Date(),allFiles[isMeta]))

write.csv(dbInputKey,paste0(DbIntakePath,"_siteIndex.csv"))


