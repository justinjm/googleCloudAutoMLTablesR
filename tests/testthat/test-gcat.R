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

  project <- Sys.getenv("GCAT_PROJECT_ID")
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
    projectId = Sys.getenv("GCAT_PROJECT_ID"),
    locationId = Sys.getenv("GCAT_LOCATION_ID"))

  expect_true(
    all(names(location_path) %in% c("name","locationId"))
  )
})

context("Models")

test_that("We can fetch a list of models", {
  skip_if_no_token()

  projectId <- Sys.getenv("GCAT_PROJECT_ID")
  locationId <- Sys.getenv("GCAT_LOCATION_ID")
  expect_true(projectId != "")
  expect_true(locationId != "")
  l <- gcat_list_models(projectId, locationId)

  expect_s3_class(l, "data.frame")
  expect_true(
    all(names(l) %in% c("name", "displayName", "datasetId", "createTime",
                        "deploymentState", "updateTime", "tablesModelMetadata"))
  )
})

