# Store dataset name
# .gcat_env <- new.env(parent = emptyenv())
# TODO - @justjm - consider and add this based on GCS R for more efficient workflow,
# less repitition of parameters in other functions
# source: https://github.com/cloudyr/googleCloudStorageR/blob/5beb3b481b/R/buckets.R#L2

#' Check if objecti is Dataset object
#'
# is.gcat_dataset <- function(x) inherits(x, "gar_Dataset")
# TODO - @justjm - consider adding this

#' Set global dataset name
#'
#' set a dataset name used for this R session
# gcat_global_dataset <- function(dataset){
#
#   dataset <- as.dataset_name(dataset)
#
#   .gcat_env$dataset <- dataset
#   message("Set default dataset name to '", dataset,"'")
#   return(invisible(.gcat_env$dataset))
#
# }
# TODO - @justjm - consider adding this

#' Lists datasets in a project.
#'
#' @param projectId
#' @param location location of GCP resources
#' @param parent The resource name of the project from which to list datasets
#' @param filter An expression for filtering the results of the request
#' @param pageToken A token identifying a page of results for the server to return
#' @param pageSize Requested page size
#' @export
gcat_list_datasets <- function(projectId,
                               locationId,
                               filter = NULL,
                               pageToken = NULL,
                               pageSize = NULL) {

  location_path <- gcat_get_location(projectId = projectId,
                                     locationId = locationId)

  parent <- location_path$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/datasets",
                 parent)

  # automl.projects.locations.datasets.list
  pars = list(filter = filter,
              pageToken = pageToken,
              pageSize = pageSize)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)

  response <- f()

  # TODO @justinjm - format `createTime` with utlity function
  response$datasets[, c("displayName", "createTime", "etag", "name")]

}


#' Gets a dataset
#'
#' @param projectId
#' @param location location of GCP resources
#' @param datasetId
#' @export
gcat_get_dataset <- function(projectId,
                             location,
                             datasetId) {

  # need: https://automl.googleapis.com/v1beta1/projects/{project_id}/locations/{locationId}/datasets/{datasetId}
  # get location path from existing function instead of hard-coding
  location_path <- gcat_location_path(projectId, location)

  # hard-code url since not sure best way to do dymanically/elsewhere
  name <- sprintf("%s/datasets/%s", location_path, datasetId)

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s", name)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = function(x) x)

  out <- f()

  print.gcat_dataset(out)

  out

}




#' Creates a dataset
#'
#' @param projectId
#' @param displayName
#' @param location location of GCP resources
#' @param parent
#' @export
gcat_create_dataset <- function(projectId,
                                location,
                                displayName,
                                parent) {

  location_path <- gcat_location_path(projectId, location)

  # Unboxing of entry into a list
  # https://github.com/justinjm/googleCloudAutoMLTablesR/issues/1#issuecomment-510526353
  jubox <- function(x) jsonlite::unbox(x)

  ds <- structure(
    list(
      displayName = jubox(displayName),
      tablesDatasetMetadata = jubox("{ }")
    ),
    class = c("gar_Dataset", "list")
  )

  gcat_create_dataset_do_call(Dataset = ds,
                              parent = location_path)

}

#' Creates a dataset (Internal API call).
#'
#' @param Dataset
#' @param parent The resource name of the project to create the dataset for
#' @importFrom googleAuthR gar_api_generator
#' @family Dataset functions
#' @keywords internal
#' @noRd
gcat_create_dataset_do_call <- function(Dataset,
                                        parent) {

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/datasets",
                 parent)

  # automl.projects.locations.datasets.create
  f <- googleAuthR::gar_api_generator(url,
                                      "POST",
                                      data_parse_function = function(x) x,
                                      checkTrailingSlash = FALSE)

  stopifnot(inherits(Dataset, "gar_Dataset"))

  f(the_body = Dataset)

}



#' Import data into AutoML Tables
#' https://cloud.google.com/automl-tables/docs/datasets#automl-tables-example-cli-curl
#'
#' @param projectId
#' @param location location of GCP resources
#' @param dataset_display_name
#' @param input_source
#' @param input_url
#' @returns inputConfig object
#' @export
gcat_import_data <- function(projectId,
                             location,
                             dataset_display_name,
                             input_source = c("bq", "gcs"),
                             input_url) {

  message("> Starting data import...")

  input_source <- match.arg(input_source)

  if(input_source == "bq") {

    # BQ
    import_data_request <- structure(
      list(
        inputConfig = list(
          params = list(
            schema_inference_version = "1"
          ),
          bigquerySource = list(
            inputUri = input_url
          )
        )
      ),
      class = c("gar_ImportDataRequest", "list")
    )

  } else if(input_source == "gcs") {

    # GCS
    import_data_request <- structure(
      list(
        inputConfig = list(
          params = list(
            schema_inference_version = "1"
          ),
          gcsSource = list(
            inputUris = input_url
          )
        )
      ),
      class = c("gar_ImportDataRequest", "list")
    )

  } else {
    message("Error. input_source not bq or gcs")
  }

  # get list of datasets
  datasets <- gcat_list_datasets(projectId = projectId,
                                 location = location)

  # extract id of dataset to create url for api call
  ## `projects/{project-id}/locations/us-central1/datasets/{dataset-id}`
  location_dataset_name <- subset(datasets,
                                  displayName == dataset_display_name,
                                  select = c(name))

  message("> Importing data...")

  tryCatch({
    gcat_import_data_do_call(ImportDataRequest = import_data_request,
                             name = location_dataset_name)
  }, error = function(ex) {
    stop("ImportDataRequest error: ", ex$message)
  })
  # TODO @justinjm - add informative messaging for import
  # since data does not successfully imports, the import job is :)
  message("> Import successful")

}

#' Imports data into a dataset. (Internal API call).
#'
#' @param ImportDataRequest Required, inputConfig JSON body request for importing a dataset
#' @param name Required, location_dataset_name
#' @noRd
gcat_import_data_do_call <- function(ImportDataRequest,
                                     name) {

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s:importData",
                 name)

  # automl.projects.locations.datasets.importData
  f <- googleAuthR::gar_api_generator(url,
                                      "POST",
                                      data_parse_function = function(x) x)

  stopifnot(inherits(ImportDataRequest, "gar_ImportDataRequest"))

  f(the_body = ImportDataRequest)

}

#' Lists table specs in a dataset
#' @param projectId
#' @param location
#' @param dataset_display_name
#' @export
gcat_list_table_specs <- function(projectId,
                                  location,
                                  dataset_display_name) {

  # get list of datasets
  datasets <- gcat_list_datasets(projectId = projectId,
                                 location = location)

  # extract id of dataset to create url for api call
  ## `projects/{project-id}/locations/us-central1/datasets/{dataset-id}`
  location_dataset_name <- subset(datasets,
                                  displayName == dataset_display_name,
                                  select = c(name))


  gcat_list_table_specs_do_call(parent = location_dataset_name)

}

#' Lists table specs in a dataset (Internal call )
#'
#'
#' @param parent The resource name of the dataset to list table specs from
#' @param filter Filter expression, see go/filtering
#' @param fieldMask Mask specifying which fields to read
#' @param pageToken A token identifying a page of results for the server to return
#' @param pageSize Requested page size
#' @noRd
gcat_list_table_specs_do_call <- function(parent,
                                          filter = NULL,
                                          fieldMask = NULL,
                                          pageToken = NULL,
                                          pageSize = NULL) {

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/tableSpecs",
                 parent)

  # automl.projects.locations.datasets.tableSpecs.list
  pars = list(filter = filter,
              fieldMask = fieldMask,
              pageToken = pageToken,
              pageSize = pageSize)

  list_table_specs <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)

  # TODO @justinjm - add function to cleanup timsetamp
  ### https://github.com/cloudyr/googleCloudStorageR/blob/f558e5b086c02362af0c027ef958d798b985c07b/R/objects.R#L88
  ### x$timeCreated <- timestamp_to_r(x$timeCreated)

  response <- list_table_specs()

  out <- response$tableSpecs

  print.gcat_table_specs(out)

  out

}


# TODO - @justinm - add this for more efficient functions to print info and
# work with objects
#' Gets a table spec.
#'
#'
#' @param name The resource name of the table spec to retrieve
#' @param fieldMask Mask specifying which fields to read
# gcat_get_table_specs <- function(name,
#                                  fieldMask = NULL) {
#
#     url <- sprintf("https://automl.googleapis.com/v1beta1/%s", name)
#
#       # automl.projects.locations.datasets.tableSpecs.get
#     pars = list(fieldMask = fieldMask)
#
#     f <- googleAuthR::gar_api_generator(url,
#                                         "GET",
#                                         pars_args = rmNullObs(pars),
#                                         data_parse_function = function(x) x)
#     f()
#
# }

















#' Gets a column spec.
#' @param projectId
#' @param location location of GCP resources
#' @param datasetId
#' @param tableSpecId
#'
#' @export
gcat_get_column_specs <- function(projectId,
                                  location,
                                  datasetId,
                                  tableSpecId,
                                  fieldMask = NULL) {

  # get location path from existing function instead of hard-coding
  location_path <- gcat_location_path(projectId, location)

  # hard-code url since not sure best way to do dymanically/elsewhere
  parent <- sprintf("%s/datasets/%s/tableSpecs/%s", location_path, datasetId,
                    tableSpecId)

  # parent <- "projects/736862006196/locations/us-central1/datasets/TBL4800700863335104512/tableSpecs/7338035050660757504/columnSpecs/7181143537470144512"
  url <- sprintf("https://automl.googleapis.com/v1beta1/%s",
                 parent)

  # automl.projects.locations.datasets.tableSpecs.columnSpecs.get
  pars = list(fieldMask = fieldMask)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)
  response <- f()

  out <- response

  out

}

#' @export
#'
gcat_get_column_spec <- function(name,
                                  fieldMask = NULL) {


  # name <- "projects/736862006196/locations/us-central1/datasets/TBL4800700863335104512/tableSpecs/7338035050660757504/columnSpecs/7181143537470144512"
  url <- sprintf("https://automl.googleapis.com/v1beta1/%s",
                 name)

  # automl.projects.locations.datasets.tableSpecs.columnSpecs.get
  pars = list(fieldMask = fieldMask)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)
  response <- f()

  browser()

  out <- response

  out

}





#' Lists column specs in a table spec.
#'
#' @param projectId
#' @param location location of GCP resources
#' @param datasetId
#' @param tableSpecId
#' @param parent The resource name of the table spec to list column specs from
#' @param pageSize Requested page size
#' @param filter Filter expression, see go/filtering
#' @param fieldMask Mask specifying which fields to read
#' @param pageToken A token identifying a page of results for the server to return
#' @export
gcat_list_column_specs <- function(projectId,
                                   location,
                                   datasetId,
                                   tableSpecId,
                                   pageSize = NULL,
                                   filter = NULL,
                                   fieldMask = NULL,
                                   pageToken = NULL) {
  # need:
  # "projects/736862006196/locations/us-central1/datasets/TBL4800700863335104512/tableSpecs/7338035050660757504"

  parent <- gcat_get_column_specs(projectId = projectId,
                                  location = location,
                                  datasetId = datasetId,
                                  tableSpecId = tableSpecId)

  parent <- parent$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/columnSpecs",
                 parent)

  # automl.projects.locations.datasets.tableSpecs.columnSpecs.list
  pars = list(pageSize = pageSize,
              filter = filter,
              fieldMask = fieldMask,
              pageToken = pageToken)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)
  response <- f()

  # TODO - @justinjm - consider adding function for parsing results in form
  # of nested dataframes
  # https://github.com/cloudyr/googleCloudStorageR/blob/master/R/utilities.R
  # out <- my_reduce_rbind(response)
  out <- response$columnSpecs

  out

}


#' Updates a dataset.
#' @param projectId
#' @param location location of GCP resources
#' @param datasetId
#' @param tableSpecId
#' @param label_column_name
#' @param updateMask
#'
#' @export
gcat_set_label <- function(projectId,
                           location,
                           datasetId,
                           tableSpecId,
                           label_column_name,
                           updateMask = NULL) {

  # get dataset based on Dataset parament
  dataset_input <- gcat_get_dataset(projectId = projectId,
                                    location = location,
                                    datasetId = datasetId)

  # set url for API call
  name <- dataset_input$name

  # get dataset display name for API call body
  displayName <- dataset_input$displayName

  # list gcat_list_column_specs to get column spec ID for target column
  column_specs_input <- gcat_list_column_specs(projectId = projectId,
                                               location = location,
                                               datasetId = datasetId,
                                               tableSpecId = tableSpecId)

  # set columnSpec Id of label column to set in AutoML tables
  label_column_specs_input <- subset(column_specs_input,
                                     displayName == label_column_name)

  # use regex since not sure where else to grab `targetColumnSpecId`?
  target_column_spec_id <- gsub(".*/columnSpecs/", "",
                                label_column_specs_input$name)

  # Unboxing of entry into a list
  # https://github.com/justinjm/googleCloudAutoMLTablesR/issues/1#issuecomment-510526353
  jubox <- function(x) jsonlite::unbox(x)

  # build request body last for easier updating code above
  Dataset <- structure(
    list(
      displayName = jubox(displayName),
      tablesDatasetMetadata = list(
        targetColumnSpecId = target_column_spec_id
      )
    ),
    class = c("gar_Dataset", "list")
  )

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s", name)

  # automl.projects.locations.datasets.patch
  pars = list(updateMask = updateMask)

  f <- googleAuthR::gar_api_generator(url,
                                      "PATCH",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)

  stopifnot(inherits(Dataset, "gar_Dataset"))

  response  <- f(the_body = Dataset)

  out <- response

  out

}


## References
# list example urls for ease of reference in developement
# GET columspecs
# https://automl.googleapis.com/v1beta1/projects/736862006196/locations/us-central1/datasets/TBL4800700863335104512/tableSpecs/7338035050660757504/
#
# list columnspecs
# https://automl.googleapis.com/v1beta1/projects/gc-automl-tables-r/locations/us-central1/datasets/TBL4800700863335104512/tableSpecs/7338035050660757504/columnSpecs/?

# PATCH dataset
## set target column
# # projects/736862006196/locations/us-central1/datasets/TBL4800700863335104512/tableSpecs/7338035050660757504/columnSpecs/7181143537470144512
