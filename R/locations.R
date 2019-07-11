#' Lists information about the supported locations for this service.
#'
#' @param projectId The resource that owns the locations collection, if applicable
#' @param pageToken The standard list page token
#' @param pageSize The standard list page size
#' @param filter The standard list filter
#'
#' @export
gcat_list_locations <- function(projectId,
                                pageToken = NULL,
                                pageSize = NULL,
                                filter = NULL) {

    url <- sprintf("https://automl.googleapis.com/v1beta1/projects/%s/locations",
                   projectId)

    # automl.projects.locations.list
    pars = list(pageToken = pageToken,
                pageSize = pageSize,
                filter = filter)

    f <- googleAuthR::gar_api_generator(url,
                                        "GET",
                                        pars_args = rmNullObs(pars),
                                        data_parse_function = function(x) x)

    f()

}

#' Create location path for use in other fucntions
#'
#' @param projectId project containing datasets to list
#' @param location location of GCP resources
#'
#' @export
gcat_location_path <- function(projectId,
                               location){

    location_path <- sprintf("projects/%s/locations/%s",
                             projectId,
                             location)

    location_path

}