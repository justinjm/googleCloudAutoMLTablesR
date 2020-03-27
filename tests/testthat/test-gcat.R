# TODO - move this to test function
# assumes you have client JSON saved to environment argument GAR_CLIENT_JSON
# app_project <- googleAuthR::gar_set_client(
#   scopes = c("https://www.googleapis.com/auth/cloud-platform"))
#
# library(googleAuthR)
# library(googleCloudAutoMLTablesR)
#
# gar_auth("gcat.oauth")

context("Setup")

options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/cloud-platform")
googleAuthR::gar_auth_service(json_file = Sys.getenv("GAR_SERVICE_JSON"))

skip_if_no_token <- function() {
  testthat::skip_if_not(googleAuthR::gar_has_token(), "No token")
}

context("Authentication")

test_that("Authentication", {
  skip_if_no_token()

  # manual auth
  expect_true(googleAuthR::gar_has_token())
})

context("Locations")

test_that("We can fetch a list of available GCP locations", {
  skip_if_no_token()

  project <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  expect_true(project != "")
  l <- gcat_list_locations(project)

  expect_s3_class(l, "data.frame")
  expect_true(
    all(names(l) %in% c("name","locationId"))
  )
})

test_that("We can fetch a GCP location path", {
  skip_if_no_token()

  location_path <- gcat_get_location(
    projectId = Sys.getenv("GCAT_DEFAULT_PROJECT_ID"),
    locationId = Sys.getenv("GCAT_DEFAULT_REGION"))

  expect_true(
    all(names(location_path) %in% c("name","locationId"))
  )
})

context("Datasets")

test_that("We can fetch a list of datasets", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  expect_true(projectId != "")
  expect_true(locationId != "")
  l <- gcat_list_datasets(projectId, locationId)

  expect_s3_class(l, "data.frame")
  expect_true(
    all(names(l) %in% c("displayName", "createTime", "etag", "name"))
  )
})

test_that("We can get a dataset object", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  displayName <- Sys.getenv("GCAT_DATASET_DISPLAY_NAME")
  expect_true(projectId != "")
  expect_true(locationId != "")
  expect_true(displayName != "")
  l <- gcat_get_dataset(projectId, locationId, displayName)

  expect_s3_class(l, "gcat_dataset")
  expect_true(
    all(names(l) %in% c("name", "displayName", "createTime", "etag",
                        "exampleCount", "tablesDatasetMetadata"))
  )
})

test_that("We can get a list of table specs", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  displayName <- Sys.getenv("GCAT_DATASET_DISPLAY_NAME")
  expect_true(projectId != "")
  expect_true(locationId != "")
  expect_true(displayName != "")
  l <- gcat_list_table_specs(projectId, locationId, displayName)

  expect_s3_class(l, "data.frame")
  expect_true(
    all(names(l) %in% c("name", "rowCount", "validRowCount", "inputConfigs",
                        "etag", "columnCount"))
  )
})

test_that("We can get a table spec", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  displayName <- Sys.getenv("GCAT_DATASET_DISPLAY_NAME")
  expect_true(projectId != "")
  expect_true(locationId != "")
  expect_true(displayName != "")
  l <- gcat_get_table_specs(projectId, locationId, displayName)

  expect_s3_class(l, "gcat_table_specs")
  expect_true(
    all(names(l) %in% c("name", "rowCount", "validRowCount", "inputConfigs",
                        "etag", "columnCount"))
  )
})

test_that("We can get a list of column specs", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  displayName <- Sys.getenv("GCAT_DATASET_DISPLAY_NAME")
  expect_true(projectId != "")
  expect_true(locationId != "")
  expect_true(displayName != "")
  l <- gcat_list_column_specs(projectId, locationId, displayName)

  expect_s3_class(l, "data.frame")
  expect_true(
    all(names(l) %in% c("name", "dataType", "displayName", "dataStats",
                        "etag" ))
  )
})

test_that("We can get a column spec", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  displayName <- Sys.getenv("GCAT_DATASET_DISPLAY_NAME")
  columnDisplayName <- Sys.getenv("GCAT_COLUMN_SPEC_DISPLAY_NAME")
  expect_true(projectId != "")
  expect_true(locationId != "")
  expect_true(displayName != "")
  expect_true(columnDisplayName != "")
  l <- gcat_get_column_spec(projectId, locationId, displayName,
                            columnDisplayName)

  expect_s3_class(l, "gcat_column_spec")
  expect_true(
    all(names(l) %in% c("name", "dataType", "displayName", "dataStats",
                        "etag" ))
  )
})

test_that("We can get a set a target column", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  displayName <- Sys.getenv("GCAT_DATASET_DISPLAY_NAME")
  columnDisplayName <- Sys.getenv("GCAT_COLUMN_SPEC_DISPLAY_NAME")
  expect_true(projectId != "")
  expect_true(locationId != "")
  expect_true(displayName != "")
  expect_true(columnDisplayName != "")
  l <- gcat_set_target_column(projectId, locationId, displayName,
                              columnDisplayName)

  expect_s3_class(l, "gcat_dataset")
  expect_true(
    all(names(l) %in% c("name", "displayName", "createTime", "etag",
                        "exampleCount", "tablesDatasetMetadata"))
  )
})

context("Models")

test_that("We can fetch a list of models", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  expect_true(projectId != "")
  expect_true(locationId != "")
  l <- gcat_list_models(projectId, locationId)

  expect_s3_class(l, "data.frame")
  expect_true(
    all(names(l) %in% c("name", "displayName", "datasetId", "createTime",
                        "deploymentState", "updateTime", "tablesModelMetadata"))
  )
})

test_that("We can get a model object", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_DEFAULT_REGION")
  modelDisplayName <- Sys.getenv("GCAT_MODEL_DISPLAY_NAME")
  expect_true(projectId != "")
  expect_true(locationId != "")
  expect_true(modelDisplayName != "")

  model <- gcat_get_model(projectId,
                          locationId,
                          modelDisplayName)

  expect_s3_class(model, "gcat_model")
  expect_true(
    all(names(model) %in% c("name", "displayName", "datasetId", "createTime",
                        "deploymentState", "updateTime", "tablesModelMetadata"))
  )
})
