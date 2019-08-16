## code to prepare `DATASET` dataset goes here
library(dplyr, warn.conflicts = FALSE)
library(readr)
library(usethis)

# https://datahub.io/machine-learning/bank-marketing/r/1.html
# install.packages("jsonlite", repos="https://cran.rstudio.com/")
library(jsonlite)

json_file <- "https://datahub.io/machine-learning/bank-marketing/datapackage.json"
json_data <- fromJSON(paste(readLines(json_file), collapse=""))

# get list of all resources:
print(json_data$resources$name)

# print all tabular data(if exists any)
for(i in 1:length(json_data$resources$datahub$type)){
  if(json_data$resources$datahub$type[i]=='derived/csv'){
    path_to_file = json_data$resources$path[i]
    data <- read.csv(url(path_to_file))
    print(data)
  }
}

bank_marketing <- data
write_csv(bank_marketing, "data-raw/bank_marketing.csv")
# usethis::use_data("bank_marketing")

