## Store dataset name
# source: https://github.com/cloudyr/googleCloudStorageR/blob/5beb3b481b/R/buckets.R#L2
.gcat_env <- new.env(parent = emptyenv())

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

#' Set global dataset name
#'
#' set a dataset name used for this R session
#'
#' @param dataset dataset name you want this session to use by default or a
#' dataset object
#' @details
#'   This sets a dataset to a global environment value so you don't need to
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
#' Dataset name set this session to use by default
#'
#' @return Dataset name
#'
#' @details
#'   Set the dataset name via \link{gcat_global_dataset}
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
#' @param projectId
#' @param locationId location of GCP resources
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

  parse_ld <- function(x) {
    x <- x$datasets
    x$createTime <- timestamp_to_r(x$createTime)

    x

  }

  pars = list(filter = filter,
              pageToken = pageToken,
              pageSize = pageSize)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = parse_ld)

  response <- f()

  out <- response

  out[, c("displayName", "createTime", "etag", "name")]

}


#' Gets a dataset
#'
#' @param projectId
#' @param locationId location of GCP resources
#' @param displayName
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
#' @param projectId
#' @param displayName
#' @param location location of GCP resources
#' @param parent
#'
#' @import jsonlite
#'
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
#' @param projectId name of the dataset to list table specs from
#' @param locationId
#' @param displayName
#' @param filter Filter expression, see go/filtering
#' @param fieldMask Mask specifying which fields to read
#' @param pageToken A token identifying a page of results for the server to return
#' @param pageSize Requested page size
#'
#' @export
gcat_list_table_specs <- function(projectId,
                                  locationId,
                                  displayName,
                                  filter = NULL,
                                  fieldMask = NULL,
                                  pageToken = NULL,
                                  pageSize = NULL) {

  # get data set metadata for listing tableSpecs API call
  dataset <- gcat_get_dataset(projectId = projectId,
                              locationId = locationId,
                              displayName = displayName)

  # sent parent name here for easy insertion into boilerplant googleAuthR
  # API call function
  parent <- dataset$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/tableSpecs",
                 parent)

  pars = list(filter = filter,
              fieldMask = fieldMask,
              pageToken = pageToken,
              pageSize = pageSize)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)

  response <- f()

  out <- response$tableSpecs

  out

}

#' Gets a table spec.
#' @param projectId name of the dataset to get a table spec from
#' @param locationId
#' @param displayName
#' @param fieldMask Mask specifying which fields to read
#'
#' @export
gcat_get_table_specs <- function(projectId,
                                 locationId,
                                 displayName,
                                 fieldMask = NULL) {

  table_spec_list <- gcat_list_table_specs(projectId = projectId,
                                           locationId = locationId,
                                           displayName = displayName)

  dataset_display_name <- displayName

  name <- subset(table_spec_list,
                 displayName == dataset_display_name,
                 select = c(name))

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s", name)

  pars = list(fieldMask = fieldMask)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)
  response <- f()

  out <- response

  structure(out, class = "gcat_table_specs")

}

#' Lists column specs in a table spec.
#'
#' @param projectId
#' @param locationId location of GCP resources
#' @param displayName
#' @param parent The resource name of the table spec to list column specs from
#' @param pageSize Requested page size
#' @param filter Filter expression, see go/filtering
#' @param fieldMask Mask specifying which fields to read
#' @param pageToken A token identifying a page of results for the server to return
#' @export
gcat_list_column_specs <- function(projectId,
                                   locationId,
                                   displayName,
                                   pageSize = NULL,
                                   filter = NULL,
                                   fieldMask = NULL,
                                   pageToken = NULL) {

  table_spec <- gcat_get_table_specs(projectId = projectId,
                                     locationId = locationId,
                                     displayName = displayName)

  parent <- table_spec$name

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

#' Gets a column spec.
#'
#' @param projectId
#' @param locationId location of GCP resources
#' @param displayName
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
#' @param projectId
#' @param location location of GCP resources
#' @param displayName
#' @param tableSpecId
#' @param labelColumnDisplayName
#' @param updateMask
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
