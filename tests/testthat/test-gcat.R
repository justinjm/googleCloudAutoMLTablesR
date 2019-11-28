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

test_that("Location List", {
  skip_if_no_token()

  project <- Sys.getenv("GCAT_PROJECT_ID")
  expect_true(project != "")
  l <- gcat_list_locations(project)

  expect_s3_class(l, "data.frame")
  expect_true(
    all(names(l) %in% c("name","locationId"))
    )
})
