#' Lists datasets in a project.
#'
#' @param projectId
#' @param location
#' @export
gcat_list_datasets <- function(projectId,
                               location) {

  location_path <- gcat_location_path(projectId, location)

  gcat_list_datasets_do_call(parent = location_path)

}

#' Lists datasets in a project. (internal API call)
#'
#' @param parent The resource name of the project from which to list datasets
#' @param filter An expression for filtering the results of the request
#' @param pageToken A token identifying a page of results for the server to return
#' @param pageSize Requested page size
#' @keywords internal
#' @noRd
gcat_list_datasets_do_call <- function(parent,
                                       filter = NULL,
                                       pageToken = NULL,
                                       pageSize = NULL) {

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

  f()$datasets[, c("displayName", "createTime", "etag", "name")]

}

#' Check if objecti is Dataset object
#'
# is.gcat_dataset <- function(x) inherits(x, "gar_Dataset")

#' Creates a dataset
#'
#' @param projectId
#' @param displayName
#' @param location
#' @param parent
#' @export
gcat_create_dataset <- function(projectId,
                                location,
                                displayName,
                                parent) {

  location_path <- gcat_location_path(projectId, location)


  # added unboxing of entry into a list
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
#' @param location
#' @param dataset_display_name
#' @param input_source
#' @param input_url
#' @returns inputConfig object
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

  message("> Import successful")

}



#' Imports data into a dataset. (Internal API call).
#'
#' @param ImportDataRequest The \link{ImportDataRequest} object to pass to this method
#' @param name Required, location_dataset_name
#' @importFrom googleAuthR gar_api_generator
#' @family ImportDataRequest functions
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

