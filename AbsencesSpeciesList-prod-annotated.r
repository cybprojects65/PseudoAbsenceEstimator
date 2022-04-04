############################################################################################################################
############# Absence Generation Script #######
############################################################################################################################
#citation: Coro, G., Magliozzi, C., Berghe, E. V., Bailly, N., Ellenbroek, A., & Pagano, P. (2016). Estimating absence locations of marine species from data of scientific surveys in OBIS. Ecological Modelling, 323, 61-76.

####REST API VERSION#####
rm(list=ls(all=TRUE))
graphics.off() 

## charging the libraries
library(raster)
library(maptools)
library(sqldf)
library(RJSONIO)
library(httr)
library(data.table)
library(robis)
# time
t0<-Sys.time()

## parameters 
species<-"Latimeria chalumnae"

res=1;
#boundingbox="POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))"
occ_percentage=0.01 #as a percentage

extent_x=180
extent_y=90
n=extent_y*2/res;
m=extent_x*2/res;

YearStart<-1900;
YearEnd<-2100;

#AUX functions
pos_id<-function(latitude,longitude){
  latitude<-latitude
  longitude<-longitude
  code<-paste(latitude,";",longitude,sep="")
  return(code)
}

downloadOccurrences<-function(scientificname, datasetid){
  cat("getting occurrences\n")
  occurrences <- occurrence(scientificname = scientificname)
  if (is.na(scientificname))
    occurrences <- occurrence(datasetid = datasetid)
  else if (is.na(datasetid))
    occurrences <- occurrence(scientificname = scientificname)
  else
    occurrences <- occurrence(scientificname = scientificname,datasetid = datasetid)
  return (occurrences)    
}

## opening the connection with postgres
cat("REST API VERSION\n")
cat("PROCESS VERSION 10\n")
cat("Opening the connection with the catalog\n")
cat("Analyzing species",species,"\n")

cat("1. Extracting total observations from all contributors\n")

allresfile="allresources.dat"
if (file.exists(allresfile)){
  load(allresfile)
} else{
  ######QUERY 0 - REST CALL
  cat("Q0:querying for resources\n")
  getJsonQ0<-function(){
    limit=1;
    resources_query<-paste("https://api.obis.org/v3/dataset?size=",limit,sep="")
    json_file <- fromJSON(resources_query)
    res_count_json<<-json_file$total
    limit = res_count_json
    cat("Q0: limit",limit,"\n")
    resources_query<-paste("https://api.obis.org/v3/dataset?size=",limit,sep="")
    json_file <- fromJSON(resources_query)
    res_count<-length(json_file$results)
    cat("Q0:declared count vs actual result count",res_count_json,"vs",res_count,"\n",sep=" ")
    allresources1 <- data.frame(resource_id=integer(),allcount=integer())
    for (i in 1:res_count){
      if (is.null(json_file$results[[i]]$records))
        json_file$results[[i]]$records=0
      row<-data.frame(resource_id = json_file$results[[i]]$id, allcount = json_file$results[[i]]$records)
      allresources1 <- rbind(allresources1, row)
    }
    rm(json_file)
    return(allresources1)
  }
  allresources1<-getJsonQ0()
  ######END REST CALL
  save(allresources1,file=allresfile)
}

cat("All resources retrieved\n")

files<-vector()
f<-0
if (!file.exists("./data"))
  dir.create("./data")

cat("About to analyse species",species,"\n")
f<-f+1
t1<-Sys.time()
graphics.off()
grid=matrix(data=0,nrow=n,ncol=m)
outputfileAbs=paste("data/Absences_",species,"_",res,"deg.csv",sep="");
outputimage=paste("data/Absences_",species,"_",res,"deg.png",sep="");

cat("Analyzing species",species,"; Output will be ",outputfileAbs,"and",outputimage,"\n")

######QUERY 1 - REST CALL
## first query: select the species
cat("Extracting species id from the OBIS database...\n")
query1<-paste("select id from obis.tnames where tname='",species,"'", sep="")
cat("Q1:querying for the species",species," \n")
query1<-paste("https://api.obis.org/v3/checklist?scientificname=",URLencode(species),sep="")
cat("Q1:query: ",query1," \n")
result_from_httr1<-GET(query1, timeout(1*3600))
json_obis_taxa_id <- fromJSON(content(result_from_httr1, as="text"))
cat("Q1:query done\n")
res_count_json<-json_obis_taxa_id$total
res_count<-length(json_obis_taxa_id$results)
cat("Q1:json count vs count",res_count_json,"vs",res_count,"\n",sep=" ")
obis_id<-json_obis_taxa_id$results[[1]]$acceptedNameUsageID
obis_id<-data.frame(id=obis_id)
cat("The ID extracted is", obis_id$id, "for species", species, "\n", sep=" ")
if (nrow(obis_id)==0) {
  cat("ERROR: there is no reference code for species ", species,"\n")
  stop
}
######END Q1 REST CALL

## second query: select the contributors
cat("Downloading the contributors having data about the species...\n")

######QUERY 2
occurrences<-downloadOccurrences(species,NA)
cat("Occurrences retrieved from OBIS")
  
names(occurrences)[which(names(occurrences)=="originalScientificName")]<-"originalScientificName2"
names(occurrences)[which(names(occurrences)=="dataset_id")]<-"resource_id"
cat("Selecting all resource ids associated with the occurrences")
posresource<-sqldf("select resource_id from occurrences",drv="SQLite")
tgtresources1<-sqldf("select resource_id, decimalLatitude || ';' || decimalLongitude as tgtcount from occurrences",drv="SQLite")
posresource<-sqldf("select distinct * from posresource",drv="SQLite")
rm(occurrences)
if (nrow(posresource)==0) {
  cat("ERRROR: there are no resources for", species,"\n")
  stop
}
######END Q2 

## Q3 - select resources with enough data about the species
## merge all observations and species observations
merge(allresources1, posresource, by="resource_id")-> res_ids
cat("Extracting # of species obs for each contributor ",obis_id$id," \n")
tgtresources1<-sqldf("select resource_id, count(distinct tgtcount) as tgtcount from tgtresources1 group by resource_id",drv="SQLite")
merge(tgtresources1, posresource, by="resource_id")-> tgtresourcesSpecies 
cat("Extracting the contributors containing more than ",(occ_percentage),"% of observations of ",species,"\n")
tmp <- merge(res_ids, tgtresourcesSpecies, by= "resource_id",all.x=T)
tmp["species_10"] <- NA 
tmp$species_10<-as.numeric(tmp$tgtcount) *100 / tmp$allcount 
viable_res_ids <- subset(tmp,species_10 >= (occ_percentage), select=c("resource_id","allcount","tgtcount", "species_10")) 
if (nrow(viable_res_ids)==0) {
    cat("ERROR: there are no viable resources for", species,"- TRY TO reduce the occurrence percentage parameter\n")
    stop
}
## END Q3

## Q4 - extract 0.1 cells of all observations
cat("Retrieving all-resource occurrences to aggregate\n")
numericselres<-paste("'",paste(as.character((t(viable_res_ids["resource_id"]))),collapse="','"),"'",sep="")
resourcesidq6 = gsub("'", "", numericselres)
occurrences <- downloadOccurrences(NA,resourcesidq6)
names(occurrences)[which(names(occurrences)=="originalScientificName")]<-"originalScientificName2"
names(occurrences)[which(names(occurrences)=="dataset_id")]<-"resource_id"
all_cells_table<-sqldf("select resource_id, decimalLatitude || ';' || decimalLongitude as position, decimalLatitude as latitude , decimalLongitude as longitude from occurrences",drv="SQLite")
rm(occurrences)
all_cells<-sqldf("select position as position_id, latitude, longitude, count(*) as allcount from all_cells_table group by position, latitude, longitude, resource_id",drv="SQLite")
######END Q4

## Q5 - extract 0.1 cells of species observations
cat("Retrieving species occurrences to aggregate\n")
occurrences <- downloadOccurrences(species,resourcesidq6)
names(occurrences)[which(names(occurrences)=="originalScientificName")]<-"originalScientificName2"
names(occurrences)[which(names(occurrences)=="dataset_id")]<-"resource_id"
presence_cells2<-sqldf("select resource_id, decimalLatitude as latitude , decimalLongitude as longitude, decimalLatitude || ';' || decimalLongitude as position from occurrences",drv="SQLite")
rm(occurrences)
presence_cells<-sqldf("select position as position_id, latitude, longitude, count(*) as tgtcount from presence_cells2 group by position_id, latitude, longitude, resource_id",drv="SQLite")
######END REST CALL
positions<-""

######QUERY 6
data.df2<-merge(all_cells, presence_cells, by= "position_id",all.x=T)
data.df2$longitude.y<-NULL 
data.df2$latitude.y<-NULL
data.df2[is.na(data.df2)] <- 0 
rm (all_cells)
pres_abs_cells2 <- subset(data.df2,select=c("latitude.x","longitude.x", "tgtcount","position_id"))
positions2<-paste("'",paste(as.character(as.character(t(pres_abs_cells2["position_id"]))),collapse="','"),"'",sep="")
pres_abs_cells<-sqldf("select * from pres_abs_cells2 order by position_id",drv="SQLite")
######END Q6
  
nrows = nrow(pres_abs_cells)
######## FIRST Loop inside the rows of the dataset
cat("Aggregating the the data\n")
for(i in 1: nrows) {
    lat<-as.numeric(pres_abs_cells[i,1])
    long<-as.numeric(pres_abs_cells[i,2])
    value<-pres_abs_cells[i,3]
    k=round((lat+90)*n/180)
    g=round((long+180)*m/360)
    if (k==0) k=1;
    if (g==0) g=1;
    if (k>n || g>m)
      next;
    if (value>=1){
      if (grid[k,g]==0){
        grid[k,g]=1
      }
      else if (grid[k,g]==-1){
        grid[k,g]=-2
      }
    }
    else if (value==0){
      if (grid[k,g]==0){
        grid[k,g]=-1
      }
      else if (grid[k,g]==1){
        grid[k,g]=-2
      }
    }
  }
cat("End aggregation\n")
  
cat("Generating image\n")
absence_cells<-which(grid==-1,arr.ind=TRUE)
presence_cells_idx<-which(grid==1,arr.ind=TRUE)
latAbs<-((absence_cells[,1]*180)/n)-90
longAbs<-((absence_cells[,2]*360)/m)-180
latPres<-((presence_cells_idx[,1]*180)/n)-90
longPres<-((presence_cells_idx[,2]*360)/m)-180
rm(grid)
absPoints <- cbind(longAbs, latAbs)
if (length(absPoints)==0){
    cat("WARNING no viable point found for ",species," after processing! - TRY TO reduce the spatial resolution parameter\n")
    next;
}
data(wrld_simpl)
projection(wrld_simpl) <- CRS("+proj=longlat")
png(filename=outputimage, width=1200, height=600)
plot(wrld_simpl, xlim=c(-180, 180), ylim=c(-90, 90), axes=TRUE, col="black")
box()
pts <- SpatialPoints(absPoints,proj4string=CRS(proj4string(wrld_simpl)))
cat("Retreiving the poing that do not fall on land\n")
pts<-pts[which(is.na(over(pts, wrld_simpl)$FIPS))]
points(pts, col="green", pch=1, cex=0.50)
datapts<-as.data.frame(pts)
colnames(datapts) <- c("longAbs","latAbs")
abspointstable<-datapts
  
header<-"longitude,latitude"
write.table(header,file=outputfileAbs,append=F,row.names=F,quote=F,col.names=F)
write.table(abspointstable,file=outputfileAbs,append=T,row.names=F,quote=F,col.names=F,sep=",")
cat("Elapsed:  created imaged in ",Sys.time()-t1," sec \n")
graphics.off()

zipOutput<-"absences.zip"
zip(zipOutput, files=c("./data"), flags= "-r9X", extras = "",zip = Sys.getenv("R_ZIPCMD", "zip"))

cat("Elapsed:  overall process finished in ",Sys.time()-t0," min \n")
graphics.off()
