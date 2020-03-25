#' Check if objecti is Dataset object
#'
#' @noRd
#' @import assertthat
is.gcat_dataset <- function(x) {

  assert_that(is.string(x))
  inherits(x, "gcat_Dataset")
}

# TODO - consider adding a check of some kind?
#' Makes a dataset name
#' @noRd
#' @import assertthat
as.dataset_name <- function(x) {

  out <- x

  assert_that(is.string(out))

  out

}

#' Set global dataset nameto be used for this R session
#'
#' @param dataset dataset name you want this session to use by default or a
#' dataset object
#'
#' @details
#' This sets a dataset to a global environment value so you don't need to
#' supply the dataset argument to other API calls.
#'
#' @return the dataset name (invisvibly)
#'
#' @import assertthat
#' @export
gcat_global_dataset <- function(dataset){

  dataset <- as.dataset_name(dataset)

  .gcat_env$dataset <- dataset
  message("Set default dataset name to '", dataset,"'")
  return(invisible(.gcat_env$dataset))

}

#' Get global dataset name
#'
#' @return a string of the dataset displayName
#'
#' @details
#' Dataset name set this session to use by default. Set the dataset name
#' via \link{gcat_global_dataset}
#'
#' @export
gcat_get_global_dataset <- function(){

  if(!exists("dataset", envir = .gcat_env)){
    stop("Dataset is NULL and couldn't find global dataset name.
         Set it via gcat_global_dataset")
  }

  .gcat_env$dataset

}

#' Lists datasets in a project.
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#'
#' @export
gcat_list_datasets <- function(projectId = gcat_project_get(),
                               locationId = gcat_region_get()) {

  parent <- sprintf("projects/%s/locations/%s",
                    projectId,
                    locationId)

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/datasets",
                 parent)

  parse_ld <- function(x) {
    x <- x$datasets
    x$createTime <- timestamp_to_r(x$createTime)

    x

  }

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = parse_ld)

  response <- f()

  out <- response

  out[, c("displayName", "createTime", "etag", "name")]

}


#' Gets a dataset
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param displayName the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#'
#' @export
gcat_get_dataset <- function(projectId,
                             locationId,
                             displayName = gcat_get_global_dataset()) {

  datasets_list <- gcat_list_datasets(projectId = projectId,
                                      locationId = locationId)

  dataset_display_name <- displayName

  name <- subset(datasets_list,
                 displayName == dataset_display_name,
                 select = c(name))

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s", name)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = function(x) x)

  response <- f()

  out <- response

  structure(out, class = "gcat_dataset")

}

#' Creates a dataset
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param displayName the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#'
#' @import jsonlite
#'
#' @export
gcat_create_dataset <- function(projectId,
                                locationId,
                                displayName) {

  location_path <- gcat_get_location(projectId = projectId,
                                     locationId = locationId)

  parent <- location_path$name

  # Unboxing of entry into a list
  jubox <- function(x) jsonlite::unbox(x)

  ds <- structure(
    list(
      displayName = jubox(displayName),
      tablesDatasetMetadata = jubox("{ }")
    ),
    class = c("gar_Dataset", "list")
  )

  gcat_create_dataset_do_call(Dataset = ds,
                              parent = parent)

}

#' Creates a dataset (Internal API call).
#'
#' @param Dataset a dataset object
#' @param parent the resource name of the project to create the dataset for
#'
#' @family Dataset functions
#'
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

#' Import data into AutoML Tables from BigQuery or Google Cloud Storage
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param dataset_display_name the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#' @param input_source a string "bq" or "gcs" to specific the source data location,
#' BigQuery or Google Cloud Storage
#' @param input_url a string, location of source data up to 2000 characters long
#' 1. BigQuery URI or path to a table e.g. bq://projectId.bqDatasetId.bqTableId
#' 2. Google Cloud Storage URI or full object path, e.g. gs://bucket/directory/object.csv
#'
#' @export
gcat_import_data <- function(projectId,
                             location, # fix to match other functions
                             dataset_display_name, # fix to match other functions
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
  # TODO  @justinjm - update these since changing of args to locationId
  # in other functions
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
#' @param ImportDataRequest Required, inputConfig JSON body request for
#' importing a dataset
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
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param displayName the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#'
#' @export
gcat_list_table_specs <- function(projectId,
                                  locationId,
                                  displayName) {

  # get data set metadata for listing tableSpecs API call
  dataset <- gcat_get_dataset(projectId = projectId,
                              locationId = locationId,
                              displayName = displayName)

  # sent parent name here for easy insertion into boilerplant googleAuthR
  # API call function
  parent <- dataset$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/tableSpecs",
                 parent)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = function(x) x)

  response <- f()

  out <- response$tableSpecs

  out

}

#' Gets a table spec
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param displayName the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#'
#' @export
gcat_get_table_specs <- function(projectId,
                                 locationId,
                                 displayName) {

  table_spec_list <- gcat_list_table_specs(projectId = projectId,
                                           locationId = locationId,
                                           displayName = displayName)

  dataset_display_name <- displayName

  name <- subset(table_spec_list,
                 displayName == dataset_display_name,
                 select = c(name))

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s", name)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = function(x) x)
  response <- f()

  out <- response

  structure(out, class = "gcat_table_specs")

}

#' Lists column specs in a table spec.
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param displayName the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#'
#' @export
gcat_list_column_specs <- function(projectId,
                                   locationId,
                                   displayName) {

  table_spec <- gcat_get_table_specs(projectId = projectId,
                                     locationId = locationId,
                                     displayName = displayName)

  parent <- table_spec$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/columnSpecs",
                 parent)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = function(x) x)
  response <- f()

  # TODO - @justinjm - consider adding function for parsing results in form
  # of nested dataframes
  # https://github.com/cloudyr/googleCloudStorageR/blob/master/R/utilities.R
  # out <- my_reduce_rbind(response)
  out <- response$columnSpecs

  out

}

#' Gets a column spec.
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param displayName the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#' @param columnDisplayName The name of the column to show in the interface.
#' The name can be up to 100 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, ASCII digits 0-9, underscores(_),
#' and forward slashes(/), and must start with a letter or a digit.
#' @param fieldMask Mask specifying which fields to read.
#'
#' @export
gcat_get_column_specs <- function(projectId,
                                  locationId,
                                  displayName,
                                  columnDisplayName,
                                  fieldMask = NULL) {

  column_specs_list <- gcat_list_column_specs(projectId = projectId,
                                              locationId = locationId,
                                              displayName = displayName)

  # rename to aid with debugging and avoid errors in subset()
  dataset_display_name <- displayName
  column_display_name <- columnDisplayName

  name <- subset(column_specs_list,
                 displayName == column_display_name,
                 select = c(name))

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s",
                 name)

  # automl.projects.locations.datasets.tableSpecs.columnSpecs.get
  pars = list(fieldMask = fieldMask)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)
  response <- f()

  out <- response

  structure(out, class = "gcat_column_spec")

}

#' Updates a dataset.
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param displayName the name of the dataset that is shown in the interface.
#' The name can be up to 32 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, underscores (_), and ASCII digits 0-9.
#' @param labelColumnDisplayName The name of the column to show in the interface.
#' The name can be up to 100 characters long and can consist only of ASCII
#' Latin letters A-Z and a-z, ASCII digits 0-9, underscores(_),
#' and forward slashes(/), and must start with a letter or a digit.
#' @param updateMask The update mask applies to the resource.
#'
#' @import jsonlite
#'
#' @export
gcat_set_label <- function(projectId,
                           locationId,
                           displayName,
                           labelColumnDisplayName,
                           updateMask = NULL) {

  # Get Dataset info to ensure dataset exists
  dataset_input <- gcat_get_dataset(projectId = projectId,
                                    locationId = locationId,
                                    displayName = displayName)

  # set url for API call
  name <- dataset_input$name

  # get dataset display name for API call body
  displayName <- dataset_input$displayName

  # list gcat_list_column_specs to get column spec ID for target column
  column_specs_list <- gcat_list_column_specs(projectId = projectId,
                                              locationId = locationId,
                                              displayName = displayName)

  # set columnSpec Id of label column to set in AutoML tables
  label_column <- subset(column_specs_list,
                         displayName == labelColumnDisplayName)

  # TODO @JUSTINJM - ADD ERROR HANDLING IF DISPLAY NAME OF TARGET DOES NOT
  # EXIST IN DATASET
  # use regex since not sure where else to grab `targetColumnSpecId`?
  target_column_spec_id <- gsub(".*/columnSpecs/", "",
                                label_column$name)

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

  structure(out, class = "gcat_dataset")

}

