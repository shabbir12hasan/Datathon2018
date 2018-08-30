#####################################################
# Some R code to get you going for the 
# 2018 Melbourne Datathon
#####################################################

#install and load required libraries
#install.packages('data.table')
library(data.table)
library(R.utils)

# stopping exponential values
options(scipen = 999)



# For running over all samples and converting them from gzip to txt.
for (sample_folder in c(3:9)){
  
  #tell R where it can find the data
  ScanOnFolderMaster <- 'D:/StudyMaterial/DatathonMelb_2018/Samp_x/ScanOnTransaction'
  ScanOffFolderMaster <- 'D:/StudyMaterial/DatathonMelb_2018/Samp_x/ScanOffTransaction'
  
  
  #decide sample folder
  mySamp <- sample_folder
  
  ScanOnFolder <- sub("x",mySamp,ScanOnFolderMaster)
  ScanOffFolder <- sub("x",mySamp,ScanOffFolderMaster)
  
  #list the files
  onFiles <- list.files(ScanOnFolder,recursive = TRUE,full.names = TRUE)
  offFiles <- list.files(ScanOffFolder,recursive = TRUE,full.names = TRUE)
  
  #how many
  allFiles <- union(onFiles,offFiles)
  cat("\nthere are", length(allFiles),'files')
  
  # using gzip to unzip files
  # code to unzip all files at once
  # select offFiles or onFiles or both
  # unzip all files
  for (i in 1:157){
    myFile <- offFiles[i]
    dt <- gunzip(myFile)
    myFile_on <- onFiles[i]
    dt <- gunzip(myFile_on)
  }
  
}