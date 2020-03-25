#' Set the location
#'
#' Can also use environment argument CR_REGION
#'
#' @param region Region for the endpoint
#' @import assertthat
#' @export
gcat_region_set <- function(region = c("us-central1",
                                       "eu")){

  region <- match.arg(region)

  .gcat_env$region <- region

  myMessage("Region set to ", .gcat_env$region, level = 3)
  .gcat_env$region
}
