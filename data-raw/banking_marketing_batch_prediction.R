## create a dataset for testing batch predictions
library(dplyr, warn.conflicts = FALSE)
library(readr)
library(usethis)

# Source https://datahub.io/machine-learning/bank-marketing
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

# sample random rows to create a sample batch prediction input for testing
# and vingnettes
set.seed(123)
data_out <- data[sample(nrow(data), 10), ]

# save file to local directory for uploading to GCS
write_csv(data_out, "data-raw/bank_marketing_batch_01.csv")
# usethis::use_data("bank_marketing")

# load library for loading to GCS
library(googleCloudStorageR)
# Check correct bucket is set in `.Renviron`
# GCS_DEFAULT_BUCKET="bucket-name"
gcs_get_global_bucket()

# execute upload of file from local directory
gcs_upload(file = "data-raw/bank_marketing_batch_01.csv",
           name = "bank_marketing_batch_01.csv",
           type = "text/csv")
