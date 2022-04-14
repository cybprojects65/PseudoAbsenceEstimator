############################################################################################################################
############# Absence Generation Script #######
############################################################################################################################
#citation: Coro, G., Magliozzi, C., Berghe, E. V., Bailly, N., Ellenbroek, A., & Pagano, P. (2016). Estimating absence locations of marine species from data of scientific surveys in OBIS. Ecological Modelling, 323, 61-76.

#V11 by G. Coro

####REST API VERSION#####
rm(list=ls(all=TRUE))
graphics.off() 

## loading the libraries
cat("Loading the required libraries\n")
library(raster)
library(maptools)
library(sqldf)
library(RJSONIO)
library(httr)
library(data.table)
library(robis)

## parameters 
species<-"Latimeria chalumnae"
#boundingbox="POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))"
boundingbox<-"POLYGON((34.94 -27.53,34.94 -9.19,53.71 -9.19,53.71 -27.53,34.94 -27.53))"
resolution<-1
#0.001|0.01|0.1|1
yearStart<-0
yearEnd<-0

#enable/disable absence retrieval
presence_only<-F

cat("Selected species:",species,"\nresolution:",resolution,"\nbounding box: ",boundingbox,"\n")

occurrences_file<-paste0("occ_",species,"_.dat")
if (file.exists(occurrences_file)){
  load(occurrences_file)
} else{
  occurrences <- occurrence(scientificname = species,geometry = boundingbox)
  save(occurrences,file=occurrences_file)
}

speciesid<-occurrences$speciesid[1]
if (is.null(speciesid)){
  cat("No species found on OBIS with name ->",species,"<- in the selected area. Please, check the name or change the area!\n")
  stopscript
}

cat("Spp occurrences retrieved for sp id",speciesid,"\n")

if (yearStart>0){
  occurrencesOrig<-occurrences
  occurrences<-occurrences[which(occurrences$year>=yearStart),]
} else if (yearEnd>0){
  occurrencesOrig<-occurrences
  occurrences<-occurrences[which(occurrences$year<=yearEnd),]
}

ndigits = 0
if (resolution==0.001)
  ndigits = 3
if (resolution==0.01)
  ndigits = 2
if (resolution==0.1)
  ndigits = 1

occurrence_records_table="occurrence_records.csv"
write.table(occurrences,file=occurrence_records_table,append=F,row.names=F,quote=F,col.names=T,sep=",")

presence_point_table<-"presence_points.csv"
presencepts<-cbind(occurrences$decimalLongitude,occurrences$decimalLatitude)
presencepts<-as.data.frame(presencepts)
colnames(presencepts) <- c("longAbs","latAbs")
presencepts$longAbs<-round(presencepts$longAbs,ndigits)
presencepts$latAbs<-round(presencepts$latAbs,ndigits)
presencepts<-unique(presencepts)

header<-"longitude,latitude"
write.table(header,file=presence_point_table,append=F,row.names=F,quote=F,col.names=F)
write.table(presencepts,file=presence_point_table,append=T,row.names=F,quote=F,col.names=F,sep=",")

if (presence_only==T){
  cat("Request to produce occurrence records only\n") 
}else{
  
  cat("Selecting valuable datasets\n")
  resources_records<-sqldf("select count(*) counts, dataset_id from occurrences group by dataset_id",drv="SQLite")
  resources_records$counts<-resources_records$counts/sum(resources_records$counts)
  lowerlim<-mean(resources_records$counts)+1.96*sd(resources_records$counts)
  valid_datasets<-resources_records[which(resources_records$counts>=lowerlim),]$dataset_id
  if (length(valid_datasets)==0){
    cat("Revising lower limit\n")
    lowerlim<-max(resources_records$counts)
    valid_datasets<-resources_records[which(resources_records$counts>=lowerlim),]$dataset_id
  }  
  
  if (length(valid_datasets)==0){
    cat("No valid data found in the bounding box and time frame. Please try extending it!\n")
    stopscript
  }
  
  valid_datasets<-as.vector(valid_datasets)
  cat("\nValid datasets found",length(valid_datasets)," over ",length(unique(resources_records$dataset_id)),"\n")
  
  occurrences_valid<-occurrences[which(occurrences$dataset_id %in% valid_datasets),]
  occurrences_valid<-sqldf("select decimalLongitude, decimalLatitude from occurrences_valid",drv="SQLite")
  occurrences_valid$decimalLongitude<-round(occurrences_valid$decimalLongitude,ndigits)
  occurrences_valid$decimalLatitude<-round(occurrences_valid$decimalLatitude,ndigits)
  occurrences_valid<-unique(occurrences_valid)
  
  cat("Valid coordinates",dim(occurrences_valid)[1],"over",dim(occurrences)[1],"\n")
  
  cat("Retrieving all occurrences in the area from valid datasets\n")
  alloccurrences_file<-paste0("all_occ_",speciesid,"_.dat")
  if (file.exists(alloccurrences_file)){
    load(alloccurrences_file)
  } else{
    alloccurrences <- occurrence(geometry = boundingbox, datasetid=valid_datasets)
    save(alloccurrences,file=alloccurrences_file)
  }
  
  if (yearStart>0){
    alloccurrencesOrig<-alloccurrences
    alloccurrences<-alloccurrences[which(alloccurrences$year>=yearStart),]
  } else if (yearEnd>0){
    alloccurrencesOrig<-alloccurrences
    alloccurrences<-alloccurrences[which(alloccurrences$year<=yearEnd),]
  }
  
  cat("Occurrences retrieved\n")
  alloccurrences_coords<-alloccurrences[-which(alloccurrences$speciesid==speciesid),]
  alloccurrences_coords<-sqldf("select decimalLongitude, decimalLatitude from alloccurrences_coords",drv="SQLite")
  alloccurrences_coords$decimalLongitude<-round(alloccurrences_coords$decimalLongitude,ndigits)
  alloccurrences_coords$decimalLatitude<-round(alloccurrences_coords$decimalLatitude,ndigits)
  alloccurrences_coords<-unique(alloccurrences_coords)
  cat("The datasets contain",dim(alloccurrences_coords)[1],"records for other species\n")
  
  if (dim(alloccurrences_coords)[1]==0){
    cat("No other species was observed without observing also ",species,". The algorithm cannot work with the current bounding box and time frame. Please try extending it or change the resolution!\n")
    stopscript
  }
  
  cat("Overlapping the datasets\n")
  speciesoverlap<-sqldf("select a.decimalLongitude,b.decimalLatitude from occurrences_valid as a join alloccurrences_coords as b on a.decimalLongitude=b.decimalLongitude AND a.decimalLatitude=b.decimalLatitude",drv="SQLite")
  speciespresence<-rbind(speciesoverlap,occurrences_valid)
  speciespresence<-unique(speciespresence)
  cat("There were",dim(speciespresence)[1],"overlapping or presence locations\n")
  
  alloccurrence_codes<-paste(alloccurrences_coords$decimalLongitude,";",alloccurrences_coords$decimalLatitude,sep="")
  allpresence_codes<-paste(speciespresence$decimalLongitude,";",speciespresence$decimalLatitude,sep="")
  
  absences<-alloccurrences_coords[-which(alloccurrence_codes %in% allpresence_codes),]
  absences<-unique(absences)
  cat("There were",dim(absences)[1],"non-overlapping locations->absences\n")
  
  if (dim(absences)[1]==0){
    cat("No other species was observed without observing also ",species,". The algorithm cannot work with the current bounding box and time frame. Please try extending it or change the resolution!\n")
    stopscript
  }
}#end management of absence records

  point_distribution<-"presence_absence_map.png"
  absence_point_table<-"absence_points.csv"
  
  cat("Writing image\n")
  
  data(wrld_simpl)
  projection(wrld_simpl) <- CRS("+proj=longlat")
  png(filename=point_distribution, width=1200, height=600)
  
  if (presence_only==F){
    plot(wrld_simpl, xlim=c(min(absences$decimalLongitude,min(presencepts$longAbs))-1, max(absences$decimalLongitude,presencepts$longAbs)+1), 
         ylim=c(min(absences$decimalLatitude,presencepts$latAbs)-1, max(absences$decimalLatitude,presencepts$latAbs)+1), axes=TRUE, col="black")
    box()
    absPoints <- cbind(absences$decimalLongitude, absences$decimalLatitude)
    pts <- SpatialPoints(absPoints,proj4string=CRS(proj4string(wrld_simpl)))
    cat("Excluding absence points that do not fall on land\n")
    pts<-pts[which(is.na(over(pts, wrld_simpl)$FIPS))]
    points(pts, col="red", pch=1, cex=0.50)
    datapts<-as.data.frame(pts)
    colnames(datapts) <- c("longAbs","latAbs")
    header<-"longitude,latitude"
    write.table(header,file=absence_point_table,append=F,row.names=F,quote=F,col.names=F)
    write.table(absPoints,file=absence_point_table,append=T,row.names=F,quote=F,col.names=F,sep=",")
  }else{
    plot(wrld_simpl, xlim=c(min(presencepts$longAbs)-1, max(presencepts$longAbs)+1), 
         ylim=c(min(presencepts$latAbs)-1, max(presencepts$latAbs)+1), axes=TRUE, col="black")
    box()
    file.create(absence_point_table)
  }
  
  presPoints <- cbind(presencepts$longAbs, presencepts$latAbs)
  ppts <- SpatialPoints(presPoints,proj4string=CRS(proj4string(wrld_simpl)))
  cat("Excluding presence points that do not fall on land\n")
  ppts<-ppts[which(is.na(over(ppts, wrld_simpl)$FIPS))]
  points(ppts, col="blue", pch=1, cex=0.50)
  
  cat("Image written\n")
  
  graphics.off()
  cat("File written\n")


cat("All done.")
