#' Set the region
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

#' Get Region Set
#'
#' @export
gcat_region_get <- function(){

  if(!is.null(.gcat_env$region)){
    return(.gcat_env$region)
  }

  if(Sys.getenv("GCAT_DEFAULT_REGION") != ""){
    .gcat_env$region <- Sys.getenv("GCAT_DEFAULT_REGION")
  }
  if(is.null(.gcat_env$region)){
    stop("No region set - use gcat_region_set() or env arg GCAT_DEFAULT_REGION",
         call. = FALSE)
  }
  .gcat_env$region
}
