
#' List Models
#'
#' @param projectId
#' @param locationId location of GCP resources
#' @param filter An expression for filtering the results of the request
#' @param pageToken A token identifying a page of results for the server to return
#' @param pageSize Requested page size
#' @export
gcat_list_models <- function(projectId,
                             locationId,
                             filter = NULL,
                             pageToken = NULL,
                             pageSize = NULL) {

  location_path <- gcat_get_location(projectId = projectId,
                                     locationId = locationId)

  parent <- location_path$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/models", parent)

  # automl.projects.locations.models.list
  pars = list(filter = filter, pageToken = pageToken, pageSize = pageSize)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)
  response <- f()

  out <- response

  out

}

#' Creates a model.Returns a Model in the responsefield when it completes.When you create a model, several model evaluations are created for it:a global evaluation, and one evaluation for each annotation spec.
#'
#' @param projectId
#' @param locationId
#' @param datasetDisplayName the full name of your dataset.
#' @param columnDisplayName XXXXXXXXX
#' @param modelDisplayName the name of the model.
#' @param trainBudgetMilliNodeHours number of milli-node-hours for training. For example, 1000 = 1 hour.
#' @param optimizationObjective with the metric to optimize (optional). See https://cloud.google.com/automl-tables/docs/train#opt-obj
#' @param targetColumnSpecName with the full column name of your target column (optional).
#' @family Model functions
#' @export
gcat_create_model <- function(projectId,
                             locationId,
                             datasetDisplayName = gcat_get_global_dataset(),
                             columnDisplayName,
                             modelDisplayName,
                             trainBudgetMilliNodeHours = NULL,
                             optimizationObjective = NULL,
                             targetColumnSpecName = NULL) {

  message("> Submitting training job request...")

  location_path <- gcat_get_location(projectId = projectId,
                                     locationId = locationId)

  parent <- location_path$name

  dataset <- gcat_get_dataset(projectId = projectId,
                              locationId = locationId,
                              displayName = datasetDisplayName)


  columnSpec <- gcat_get_column_specs(projectId,
                                      locationId,
                                      displayName = datasetDisplayName,
                                      columnDisplayName)

  # Build model object request body
  create_model_request <- structure(
    list(
      datasetId = dataset[["tablesDatasetMetadata"]][["primaryTableSpecId"]],
      displayName = modelDisplayName,
      tablesModelMetadata = list(
        trainBudgetMilliNodeHours = trainBudgetMilliNodeHours,
        optimizationObjective = optimizationObjective,
        targetColumnSpec = list(
          name = column_spec[["name"]]
        )
      )
    ), class = c("gcat_Model", "list")
  )
  # browser()
  message("> TEST - API CALL HAPPENS HERE")
  gcat_create_model_do_call(Model = create_model_request,
                            parent = parent)

}

#' (Internal API call).
#' @param Model
#' @param parent
#'
#'
#' @noRd
gcat_create_model_do_call <- function(Model,
                                      parent) {

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/models",
                 parent)

  # automl.projects.locations.models.create
  f <- googleAuthR::gar_api_generator(url,
                                      "POST",
                                      data_parse_function = function(x) x)

  stopifnot(inherits(Model, "gcat_Model"))

  response <- f(the_body = Model)

  out <- response

  structure(out, class = "gcat_model")
  message("> Training started successfully")
  # message("> Monitor with function 'gcat_get_operation' or in GCP console")

}

