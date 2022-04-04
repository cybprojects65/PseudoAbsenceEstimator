############################################################################################################################
############# Absence Generation Script - Gianpaolo Coro and Chiara Magliozzi, CNR 2015, original version 06-07-2015 #######
############################################################################################################################
#Modified 18-09-2017 - new OBIS 2.0 API
#Modified 25-05-2017
#Modified 07-05-2019
#Modified 16-02-2021

#52North WPS annotations
# wps.des: id = Absence_generation_from_OBIS, title = Absence_generation_from_OBIS, abstract = A script to estimate absence records from OBIS;

####REST API VERSION#####
rm(list=ls(all=TRUE))
graphics.off() 

## charging the libraries
#library(DBI)
#library(RPostgreSQL)
library(raster)
library(maptools)
library("sqldf")
library(RJSONIO)
library(httr)
library(data.table)
library(robis)
# time
t0<-Sys.time()

## parameters 
# wps.in: id = list, type = text/plain, title = list of species beginning with the speciesname header,value="species.txt";
list= "species.txt"
specieslist<-read.table(list,header=T,sep=",") # my short dataset 2 species
#attach(specieslist)
# wps.in: id = res, type = double, title = resolution of the analysis,value=1;
res=1;
extent_x=180
extent_y=90
n=extent_y*2/res;
m=extent_x*2/res;
# wps.in: id = occ_percentage, type = double, title = percentage of observations occurrence of a viable survey,value=0.1;
occ_percentage=0.001 #between 0 and 1
#uncomment for time filtering

#No time filter
TimeStart<-"";
TimeEnd<-"";

TimeStart<-gsub("(^ +)|( +$)", "",TimeStart)
TimeEnd<-gsub("(^ +)|( +$)", "", TimeEnd)

#AUX function
pos_id<-function(latitude,longitude){
  #latitude<-round(latitude, digits = 3)
  #longitude<-round(longitude, digits = 3)
  latitude<-latitude
  longitude<-longitude
  code<-paste(latitude,";",longitude,sep="")
  return(code)
}

## opening the connection with postgres
cat("REST API VERSION\n")
cat("PROCESS VERSION 9 \n")
cat("Opening the connection with the catalog\n")
#drv <- dbDriver("PostgreSQL")
#con <- dbConnect(drv, dbname="obis", host="obisdb-stage.vliz.be", port="5432", user="obisreader", password="0815r3@d3r")

cat("Analyzing the list of species\n")
counter=0;
overall=length(specieslist$scientificname)

cat("Extraction from the different contributors the total number of obs per resource id...\n")

timefilter<-""
if (nchar(TimeStart)>0 && nchar(TimeEnd)>0)
  timefilter<-paste(" where datecollected>'",TimeStart,"' and datecollected<'",TimeEnd,"'",sep="");

queryCache <- paste("select drs.resource_id, count(distinct position_id) as allcount from obis.drs", timefilter, " group by drs.resource_id",sep="")
cat("Resources extraction query:",queryCache,"\n")

allresfile="allresources.dat"
if (file.exists(allresfile)){
  load(allresfile)
} else{
  #allresources1<-dbGetQuery(con,queryCache)
  ######QUERY 0 - REST CALL
  cat("Q0:querying for resources\n")
  
  getJsonQ0<-function(){
    
    limit=1;
    resources_query<-paste("https://api.obis.org/v3/dataset?size=",limit,sep="")
    
    json_file <- fromJSON(resources_query)
    
    #res_count<-json_file$count
    res_count_json<<-json_file$total
    limit = res_count_json
    
    cat("Q0: limit",limit,"\n")
    
    resources_query<-paste("https://api.obis.org/v3/dataset?size=",limit,sep="")
    json_file <- fromJSON(resources_query)
    
    res_count<-length(json_file$results)
    
    cat("Q0:json count vs count",res_count_json,"vs",res_count,"\n",sep=" ")
    
    allresources1 <- data.frame(resource_id=integer(),allcount=integer())
    
    for (i in 1:res_count){
      #cat(i,"\n")
      if (is.null(json_file$results[[i]]$records))
        json_file$results[[i]]$records=0
      row<-data.frame(resource_id = json_file$results[[i]]$id, allcount = json_file$results[[i]]$records)
      allresources1 <- rbind(allresources1, row)
    }
    rm(json_file)
    return(allresources1)
  }
  
  allresources1<-getJsonQ0()
  
  
  ceil<-(res_count_json)
  if (ceil<1000){
    cat("Error: could not retrieve the objects count")
    exitwitherror
    #for (i in 2:ceil){
    #cat(">call n.",i,"\n")
    #allresources1.1<-getJsonQ0(objects,objects*(i-1))
    #allresources1<-rbind(allresources1,allresources1.1)
    #}
  }
  ######END REST CALL
  save(allresources1,file=allresfile)
}


cat("All resources saved\n")

files<-vector()
f<-0
if (!file.exists("./data"))
  dir.create("./data")

cat("About to analyse species\n")

for (sp in specieslist$scientificname){
  f<-f+1
  t1<-Sys.time()
  graphics.off()
  grid=matrix(data=0,nrow=n,ncol=m)
  gridInfo=matrix(data="",nrow=n,ncol=m)
  outputfileAbs=paste("data/Absences_",sp,"_",res,"deg.csv",sep="");
  outputimage=paste("data/Absences_",sp,"_",res,"deg.png",sep="");
  
  counter=counter+1;
  cat("analyzing species",sp,"\n")
  cat("***Species status",counter,"of",overall,"\n")
  
  ## first query: select the species
  cat("Extraction the species id from the OBIS database...\n")
  query1<-paste("select id from obis.tnames where tname='",sp,"'", sep="")
  #obis_id<- dbGetQuery(con,query1)
  
  ######QUERY 1 - REST CALL
  cat("Q1:querying for the species",sp," \n")
  query1<-paste("https://api.obis.org/v3/checklist?scientificname=",URLencode(sp),sep="")
  cat("Q1:query: ",query1," \n")
  result_from_httr1<-GET(query1, timeout(1*3600))
  json_obis_taxa_id <- fromJSON(content(result_from_httr1, as="text"))
  
  #json_obis_taxa_id <- fromJSON(query1)
  cat("Q1:query done\n")
  res_count_json<-json_obis_taxa_id$total
  res_count<-length(json_obis_taxa_id$results)
  cat("Q1:json count vs count",res_count_json,"vs",res_count,"\n",sep=" ")
  obis_id<-json_obis_taxa_id$results[[1]]$acceptedNameUsageID
  obis_id<-data.frame(id=obis_id)
  ######END REST CALL
  
  cat("The ID extracted is", obis_id$id, "for the species", sp, "\n", sep=" ")
  if (nrow(obis_id)==0) {
    cat("WARNING: there is no reference code for", sp,"\n")
    next;
  }
  
  ## second query: select the contributors
  cat("Selection of the contributors in the database having recorded the species...\n")
  #query2<- paste("select distinct resource_id from obis.drs where valid_id='",obis_id$id,"'", sep="")
  #posresource<-dbGetQuery(con,query2)
  
  ######QUERY 2 - REST CALL
  cat("Q2:querying for obisid ",obis_id$id," \n")
  
  downloadOccurrences<-function(scientificname, datasetid){
    
    
    library(robis)
    cat("getting occurrences\n")
    occurrences <- occurrence(scientificname = scientificname)
    
    if (is.na(scientificname))
      occurrences <- occurrence(datasetid = datasetid)
    else if (is.na(datasetid))
      occurrences <- occurrence(scientificname = scientificname)
    else
      occurrences <- occurrence(scientificname = scientificname,datasetid = datasetid)
     # query<-paste("https://api.obis.org/v3/download/occurrence?scientificname=",URLencode(scientificname),"&datasetid=",datasetid,sep="")
    
    
    return (occurrences)    
  }
  
  occurrences<-downloadOccurrences(sp,NA)
  cat("Retrieved occurrences from OBIS")
  
  names(occurrences)[which(names(occurrences)=="originalScientificName")]<-"originalScientificName2"
  names(occurrences)[which(names(occurrences)=="dataset_id")]<-"resource_id"
  
  posresource<-sqldf("select resource_id from occurrences",drv="SQLite")
  tgtresources1<-sqldf("select resource_id, decimalLatitude || ';' || decimalLongitude as tgtcount from occurrences",drv="SQLite")
  posresource<-sqldf("select distinct * from posresource",drv="SQLite")
  rm(occurrences)
  ######END REST CALL
  
  if (nrow(posresource)==0) {
    cat("WARNING: there are no resources for", sp," - TRY TO reduce the occurrence percentage parameter\n")
    next;
  }
  
  ## third query: select from the contributors different observations
  merge(allresources1, posresource, by="resource_id")-> res_ids
  
  ## forth query: how many obs are contained in each contributors for the species
  cat("Extraction from the different contributors the number of obs for the species...\n")
  query4 <- paste("select drs.resource_id, count(distinct position_id) as tgtcount from obis.drs where valid_id='",obis_id$id,"'group by drs.resource_id ",sep="")
  #tgtresources1<-dbGetQuery(con,query4)
  
  ######QUERY 4 - REST CALL
  cat("Q4 (SQL):extracting obs from contributors ",obis_id$id," \n")
  tgtresources1<-sqldf("select resource_id, count(distinct tgtcount) as tgtcount from tgtresources1 group by resource_id",drv="SQLite")
  
  ######END REST CALL
  
  merge(tgtresources1, posresource, by="resource_id")-> tgtresourcesSpecies 
  
  ## fifth query: select contributors that has al least 0.1 observation of the species
  #### we have the table all together: contributors, obs in each contributors for at leat one species and obs of the species in each contributors
  cat("Extracting the contributors containing more than 10% of observations for the species\n")
  cat("Selected occurrence percentage: ",occ_percentage,"\n")
  
  tmp <- merge(res_ids, tgtresourcesSpecies, by= "resource_id",all.x=T)
  tmp["species_10"] <- NA 
  as.numeric(tmp$tgtcount) / tmp$allcount -> tmp$species_10
  
  viable_res_ids <- subset(tmp,species_10 >= occ_percentage, select=c("resource_id","allcount","tgtcount", "species_10")) 
  #cat(viable_res_ids)
  
  if (nrow(viable_res_ids)==0) {
    cat("WARNING: there are no viable points for", sp,"- TRY TO reduce the occurrence percentage parameter\n")
    next;
  }
  
  #numericselres<-paste("'",paste(as.character(as.numeric(t(viable_res_ids["resource_id"]))),collapse="','"),"'",sep="")
  numericselres<-paste("'",paste(as.character((t(viable_res_ids["resource_id"]))),collapse="','"),"'",sep="")
  #selresnumbers<-as.numeric(t(viable_res_ids["resource_id"]))
  
  ## sixth query: select all the cell at 0.1 degrees resolution in the main contributors
  cat("Select the cells at 0.1 degrees resolution for the main contributors\n")
  query6 <- paste("select position_id, positions.latitude, positions.longitude, count(*) as allcount ", 
                  "from obis.drs ", 
                  "inner join obis.tnames on drs.valid_id=tnames.id ",
                  "inner join obis.positions on position_id=positions.id ",
                  "where resource_id in (", numericselres,") ",
                  "group by position_id, positions.latitude, positions.longitude, resource_id")
  #all_cells <- dbGetQuery(con,query6)
  
  
  ######QUERY 6 - REST CALL
  cat("Q6:extracting 0.1 cells from contributors \n")
  
  resourcesidq6 = gsub("'", "", numericselres)
  occurrences <- downloadOccurrences(NA,resourcesidq6)
  cat("Retrieved occurrences from OBIS\n")
  names(occurrences)[which(names(occurrences)=="originalScientificName")]<-"originalScientificName2"
  names(occurrences)[which(names(occurrences)=="dataset_id")]<-"resource_id"
  
  all_cells_table<-sqldf("select resource_id, decimalLatitude || ';' || decimalLongitude as position, decimalLatitude as latitude , decimalLongitude as longitude from occurrences",drv="SQLite")
  rm(occurrences)
  
  cat("All resources:",numericselres,"\n")
  
  all_cells<-sqldf("select position as position_id, latitude, longitude, count(*) as allcount from all_cells_table group by position, latitude, longitude, resource_id",drv="SQLite")
  
  ######END REST CALL
  
  
  
  ## seventh query:  select all the cell at 0.1 degrees resolution in the main contributors for the selected species
  cat("Select the cells at 0.1 degrees resolution for the species in the main contributors\n")
  query7 <- paste("select position_id, positions.latitude, positions.longitude, count(*) as tgtcount ",
                  "from obis.drs",
                  "inner join obis.tnames on drs.valid_id=tnames.id ", 
                  "inner join obis.positions on position_id=positions.id ", 
                  "where resource_id in (", numericselres,") ",
                  "and drs.valid_id='",obis_id$id,"'", 
                  "group by position_id, positions.latitude, positions.longitude")
  #presence_cells<-dbGetQuery(con,query7)
  
  
  ######QUERY 7 - REST CALL
  cat("Q7:extracting 0.1 cells for the species ",obis_id$id,"\n")
  
  occurrences <- downloadOccurrences(sp,resourcesidq6)
  cat("Retrieved occurrences from OBIS\n")
  names(occurrences)[which(names(occurrences)=="originalScientificName")]<-"originalScientificName2"
  names(occurrences)[which(names(occurrences)=="dataset_id")]<-"resource_id"
  
  presence_cells2<-sqldf("select resource_id, decimalLatitude as latitude , decimalLongitude as longitude, decimalLatitude || ';' || decimalLongitude as position from occurrences",drv="SQLite")
  rm(occurrences)
  presence_cells<-sqldf("select position as position_id, latitude, longitude, count(*) as tgtcount from presence_cells2 group by position_id, latitude, longitude, resource_id",drv="SQLite")
  
  ######END REST CALL
  
  ## last query: for every cell in the sixth query if there is a correspondent in the seventh query I can put 1 otherwise 0
  #data.df<-merge(all_cells, presence_cells, by= "position_id",all.x=T)
  #data.df$longitude.y<-NULL 
  #data.df$latitude.y<-NULL
  #data.df[is.na(data.df)] <- 0 
  
  ######### Table resulting from the analysis
  #pres_abs_cells <- subset(data.df,select=c("latitude.x","longitude.x", "tgtcount","position_id"))
  #positions<-paste("'",paste(as.character(as.numeric(t(pres_abs_cells["position_id"]))),collapse="','"),"'",sep="")
  positions<-""
  query8<-paste("select position_id, resfullname,digirname,abstract,temporalscope,date_last_harvested",
                "from ((select distinct position_id,resource_id from obis.drs where position_id IN (", positions,
                ") order by position_id ) as a",
                "inner join (select id,resfullname,digirname,abstract,temporalscope,date_last_harvested from obis.resources where id in (",
                numericselres,")) as b on b.id = a.resource_id) as d")
  
  #resnames<-dbGetQuery(con,query8)
  
  ######QUERY 8 - REST CALL
  cat("Q8:extracting contributors details\n")
  data.df2<-merge(all_cells, presence_cells, by= "position_id",all.x=T)
  data.df2$longitude.y<-NULL 
  data.df2$latitude.y<-NULL
  data.df2[is.na(data.df2)] <- 0 
  rm (all_cells)
  pres_abs_cells2 <- subset(data.df2,select=c("latitude.x","longitude.x", "tgtcount","position_id"))
  positions2<-paste("'",paste(as.character(as.character(t(pres_abs_cells2["position_id"]))),collapse="','"),"'",sep="")
  
  refofpositions<-sqldf(paste("select distinct resource_id from all_cells_table where position in (",positions2,")"),drv="SQLite")
  referencesn<-nrow(refofpositions)
  resnames_res2 <- data.frame(resource_id=integer(),resfullname=character(),digirname=character(),abstract=character(),temporalscope=character(),date_last_harvested=character())
  
  for (i in 1: referencesn){
    query8<-paste("https://api.obis.org/v3/dataset/",refofpositions[i,1],sep="")
    result_from_httr<-GET(query8, timeout(1*3600))
    jsonDoc <- fromJSON(content(result_from_httr, as="text"))$results[[1]]
    
    if (length(jsonDoc$updated)==0){
      jsonDoc$updated=""
      daterecord=""
    }else daterecord<-as.character(jsonDoc$updated)#as.POSIXct(jsonDoc$updated/1000, origin="1970-01-01")#origin="1970-01-01")
    
    if (length(daterecord)==0)
      daterecord=""
    
    abstractst<-jsonDoc$abstract
    
    if (length(jsonDoc$abstract)==0)
      jsonDoc$abstract=""
    
    if (length(jsonDoc$id)==0)
      jsonDoc$id=""
    
    if (length(jsonDoc$title)==0)
      jsonDoc$title=""
    
    if (length(jsonDoc$temporalscope)==0)
      jsonDoc$temporalscope=""
    
    if (length(jsonDoc$digirname)==0)
      jsonDoc$digirname=""
    
    row<-data.frame(resource_id = jsonDoc$id, resfullname=jsonDoc$title, 
                    digirname=jsonDoc$digirname, abstract=jsonDoc$abstract,
                    temporalscope=jsonDoc$temporalscope,
                    date_last_harvested=daterecord)
    
    resnames_res2 <- rbind(resnames_res2, row) 
  }
  
  resnames2<-sqldf(paste("select distinct position as position_id, resfullname, digirname, abstract, temporalscope, date_last_harvested from (select * from all_cells_table where position in (",positions2,")) as a inner join resnames_res2 as b on a.resource_id=b.resource_id"),drv="SQLite")
  resnames<-sqldf("select * from resnames2 order by position_id",drv="SQLite")
  pres_abs_cells<-sqldf("select * from pres_abs_cells2 order by position_id",drv="SQLite")
  rm(all_cells_table)
  ######END REST CALL
  
  #sorting data df
  #  pres_abs_cells<-pres_abs_cells[with(pres_abs_cells, order(position_id)), ]
  nrows = nrow(pres_abs_cells)
  ######## FIRST Loop inside the rows of the dataset
  cat("Looping on the data\n")
  for(i in 1: nrows) {
    lat<-as.numeric(pres_abs_cells[i,1])
    long<-as.numeric(pres_abs_cells[i,2])
    value<-pres_abs_cells[i,3]
    resource_name<-paste("\"",paste(as.character(t(resnames[i,])),collapse="\",\""),"\"",sep="")#resnames[i,2]
    k=round((lat+90)*n/180)
    g=round((long+180)*m/360)
    if (k==0) k=1;
    if (g==0) g=1;
    if (k>n || g>m)
      next;
    if (value>=1){
      if (grid[k,g]==0){
        grid[k,g]=1
        gridInfo[k,g]=resource_name
      }
      else if (grid[k,g]==-1){
        grid[k,g]=-2
        gridInfo[k,g]=resource_name
      }
    }
    else if (value==0){
      if (grid[k,g]==0){
        grid[k,g]=-1
        #cat("resource abs",resource_name,"\n")
        gridInfo[k,g]=resource_name
      }
      else if (grid[k,g]==1){
        grid[k,g]=-2
        gridInfo[k,g]=resource_name
      }
      
    }
  }
  cat("End looping\n")
  
  cat("Generating image\n")
  absence_cells<-which(grid==-1,arr.ind=TRUE)
  presence_cells_idx<-which(grid==1,arr.ind=TRUE)
  latAbs<-((absence_cells[,1]*180)/n)-90
  longAbs<-((absence_cells[,2]*360)/m)-180
  latPres<-((presence_cells_idx[,1]*180)/n)-90
  longPres<-((presence_cells_idx[,2]*360)/m)-180
  resource_abs<-gridInfo[absence_cells]
  rm(gridInfo)
  rm(grid)
  absPoints <- cbind(longAbs, latAbs)
  absPointsData <- cbind(longAbs, latAbs,resource_abs)
  
  if (length(absPoints)==0)
  {
    cat("WARNING no viable point found for ",sp," after processing! - TRY TO reduce the resolution parameter\n")
    next;
  }
  data(wrld_simpl)
  projection(wrld_simpl) <- CRS("+proj=longlat")
  png(filename=outputimage, width=1200, height=600)
  plot(wrld_simpl, xlim=c(-180, 180), ylim=c(-90, 90), axes=TRUE, col="black")
  box()
  pts <- SpatialPoints(absPoints,proj4string=CRS(proj4string(wrld_simpl)))
  
  ## Find which points do not fall over land
  cat("Retreiving the poing that do not fall on land\n")
  pts<-pts[which(is.na(over(pts, wrld_simpl)$FIPS))]
  points(pts, col="green", pch=1, cex=0.50)
  datapts<-as.data.frame(pts)
  colnames(datapts) <- c("longAbs","latAbs")
  
  abspointstable<-merge(datapts, absPointsData, by.x= c("longAbs","latAbs"), by.y=c("longAbs","latAbs"),all.x=F)
  
  
  header<-"longitude,latitude,closest_observation_of_other_species,resource_name,resource_identifier,resource_abstract,resource_temporalscope,resource_last_harvested_date"
  write.table(header,file=outputfileAbs,append=F,row.names=F,quote=F,col.names=F)
  
  write.table(abspointstable,file=outputfileAbs,append=T,row.names=F,quote=F,col.names=F,sep=",")
  files[f]<-outputfileAbs
  cat("Elapsed:  created imaged in ",Sys.time()-t1," sec \n")
  graphics.off()
}

# wps.out: id = zipOutput, type = text/zip, title = zip file containing absence records and images;
zipOutput<-"absences.zip"
#zipOutput<-paste(zipOutput,"_",Sys.time(),".zip",sep="")
#zipOutput<-gsub(" ", "_", zipOutput)
#zipOutput<-gsub(":", "_", zipOutput)

zip(zipOutput, files=c("./data"), flags= "-r9X", extras = "",zip = Sys.getenv("R_ZIPCMD", "zip"))

#unlink(dirzip,recursive=T)

cat("Closing database connection")
cat("Elapsed:  overall process finished in ",Sys.time()-t0," min \n")
#dbDisconnect(con)
graphics.off()
