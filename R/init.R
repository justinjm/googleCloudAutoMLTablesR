#' Set the region
#'
#' Can also use environment argument GCAT_DEFAULT_REGION
#'
#' @param region Region for the endpoint
#' @import assertthat
#' @export
gcat_region_set <- function(region = c("us-central1",
                                       "eu")){

  region <- match.arg(region)

  .gcat_env$region <- region

  myMessage("Region set to '", .gcat_env$region, "'", level = 3)
  return(invisible(.gcat_env$region))
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

#' Set the projectId for your AutoML Tables services
#'
#' Can also use environment argument GCAT_DEFAULT_PROJECT_ID
#'
#' @param projectId The projectId
#' @import assertthat
#' @export
gcat_project_set <- function(projectId){

  .gcat_env$project <- projectId

  myMessage("ProjectId set to '", .gcat_env$project, "'", level = 3)
  return(invisible(.gcat_env$project))
}

#' Get ProjectId
#'
#' @export
gcat_project_get <- function(){

  if(!is.null(.gcat_env$project)){
    return(.gcat_env$project)
  }

  if(Sys.getenv("GCAT_DEFAULT_PROJECT_ID") != ""){
    .gcat_env$project <- Sys.getenv("GCAT_DEFAULT_PROJECT_ID")
  }
  if(is.null(.gcat_env$project)){
    stop("No projectId set - use gcat_project_set() or env arg GCAT_DEFAULT_PROJECT_ID",
         call. = FALSE)
  }
  .gcat_env$project
}
