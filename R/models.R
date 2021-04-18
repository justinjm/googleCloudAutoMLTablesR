#' List Models
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param detail Set level of detail
#'
#' @family Model functions
#' @export
gcat_list_models <- function(projectId = gcat_project_get(),
                             locationId = gcat_region_get(),
                             detail = c("summary","full")) {

  detail <- match.arg(detail)

  parent <- sprintf("projects/%s/locations/%s",
                    projectId,
                    locationId)

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/models", parent)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = function(x) x)
  response <- f()

  out <- response$model

  out_names <- switch(detail,
                      summary = c("displayName",
                                  "datasetId",
                                  "createTime",
                                  "deploymentState",
                                  "updateTime"),
                      full = TRUE)

  out[,out_names]

}

#' Creates a model
#'
#' @description
#' Returns a Model in the responsefield when it completes. When you create a
#' model, several model evaluations are created for it:
#' a global evaluation, and one evaluation for each annotation spec.
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param datasetDisplayName the full name of your dataset
#' @param columnDisplayName the full name of the label or target
#' @param modelDisplayName the name of the model shown in the interface
#' @param trainBudgetMilliNodeHours number of milli-node-hours for training. For example, 1000 = 1 hour.
#' @param optimizationObjective with the metric to optimize (optional). See https://cloud.google.com/automl-tables/docs/train#opt-obj
#' @param targetColumnSpecName with the full column name of your target column (optional).
#'
#' @family Model functions
#' @export
gcat_create_model <- function(projectId = gcat_project_get(),
                              locationId = gcat_region_get(),
                              datasetDisplayName = gcat_get_global_dataset(),
                              columnDisplayName,
                              modelDisplayName,
                              trainBudgetMilliNodeHours = NULL,
                              optimizationObjective = NULL,
                              targetColumnSpecName = NULL) {

  existing_models <- gcat_list_models(projectId, locationId)

  if(modelDisplayName %in% existing_models$displayName) {
    stop("Model with specified modelDisplayName exists, specify unique modelDisplayName.")
  }

  myMessage("Submitting model training job request...", level = 3)

  location_path <- gcat_get_location(projectId = projectId,
                                     locationId = locationId)

  parent <- location_path$name

  dataset <- gcat_get_dataset(projectId = projectId,
                              locationId = locationId,
                              displayName = datasetDisplayName)

  # get dataset ID from url since not sure how else?
  dataset_id <- gsub(".*/datasets/" , "", dataset$name)

  column_spec <- gcat_get_column_spec(projectId,
                                      locationId,
                                      displayName = datasetDisplayName,
                                      columnDisplayName)

  # Build model object request body
  create_model_request_body <- structure(
    list(
      datasetId = dataset_id,
      displayName = modelDisplayName,
      tablesModelMetadata = list(
        trainBudgetMilliNodeHours = trainBudgetMilliNodeHours,
        optimizationObjective = optimizationObjective,
        targetColumnSpec = list(
          name = column_spec[["name"]]
        )
      )
    ), class = c("gcat_model", "list")
  )

  tryCatch({
    url <- sprintf("https://automl.googleapis.com/v1beta1/%s/models",
                   parent)

    f <- googleAuthR::gar_api_generator(
      url,
      "POST",
      data_parse_function = function(x) x)

    stopifnot(inherits(create_model_request_body, "gcat_model"))

    response <- f(the_body = create_model_request_body)

    out <- response

    structure(out, class = "gcat_operation")

  }, error = function(e) {
    stop("CreateModelRequest error: ", e$message)

  })

}

#' Gets a model.
#'
#' @description Gets information about a trained model. Gets the most recent
#' model if multiple models are named the same
#'
#' @param projectId GCP project id
#' @param locationId location of GCP resources
#' @param modelDisplayName the name of the model shown in the interface
#'
#' @family Model functions
#' @export
gcat_get_model <- function(projectId = gcat_project_get(),
                           locationId = gcat_region_get(),
                           modelDisplayName) {

  models_list <- gcat_list_models(projectId = projectId,
                                  locationId = locationId,
                                  detail = "full")

  # select most recently created model if multiple models created with same
  # modelDisplay name. This depends on gcat_list_models() results sorted
  # by createTime DESC by default
  model <- subset(models_list,
                  displayName == modelDisplayName)
  model <- model[1,]
  name <- model$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s",
                 name)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = function(x) x)
  response <- f()

  out <- response

  structure(out, class = "gcat_model")

}

#' Lists model evaluations.
#'
#' @param projectId GCP project id
#' @param locationId the location Id
#' @param modelDisplayName the name of the model shown in the interface
#'
#' @family Model functions
#' @export
gcat_list_model_evaluations <- function(projectId = gcat_project_get(),
                                        locationId = gcat_region_get(),
                                        modelDisplayName) {

  model <- gcat_get_model(modelDisplayName = modelDisplayName)

  parent <- model$name

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/modelEvaluations",
                 parent)

  parse_lm <- function(x) {
    x <- x$modelEvaluation
    x$createTime <- timestamp_to_r(x$createTime)

    x

  }


  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      data_parse_function = parse_lm)

  response <- f()

  out <- response

  out

}

#' Perform a batch prediction. Unlike the online Predict,
#' batchprediction result won't be immediately available in the response.
#' Instead,a long running operation object is returned.
#'
#' @param modelDisplayName the name of the model shown in the interface
#' @param inputSource  Google Cloud Storage URIs to input files, up to 2000 characters long. Full object path, e.g. gs://bucket/directory/object.csv
#' @param outputTarget STRING Required. Google Cloud Storage URI to output directory, up to 2000 characters long. Prefix path: gs://bucket/directory The requesting user must have write permission to the bucket. The directory is created if it doesn't exist.
#'
#' @family Model functions
#' @export
gcat_batch_predict <- function(modelDisplayName,
                               inputSource,
                               outputTarget) {

  # inputSource <- match.arg(input_source)

  # get name of model
  model <- gcat_get_model(modelDisplayName = modelDisplayName)
  name <- model$name

  # GCS
  batch_predict_request <- structure(
    list(
      inputConfig = list(
        gcsSource = list(
          inputUris = inputSource
        )
      ),
      outputConfig = list(
        gcsDestination = list(
          outputUriPrefix = outputTarget
        )
      )
    ),
    class = c("gcat_BatchPredictRequest", "list")
  )

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s:batchPredict",
                 name)

  # automl.projects.locations.models.batchPredict
  f <- googleAuthR::gar_api_generator(url,
                                      "POST",
                                      data_parse_function = function(x) x)

  stopifnot(inherits(batch_predict_request, "gcat_BatchPredictRequest"))

  response <- f(the_body = batch_predict_request)

  out <- response

  out

}
