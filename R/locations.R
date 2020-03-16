#' Lists information about the supported locations for this service.
#'
#' @param projectId the GCP project Id containing locations to fetch
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
    response <- f()

    out <- response$locations

    out

}

#' Get a location
#'
#' Gets information about a location
#'
#' @param projectId the GCP project Id containing location to fetch
#' @param locationId location of GCP resources
#'
#' @export
gcat_get_location <- function(projectId,
                              locationId) {

    locations <- gcat_list_locations(projectId = projectId)

    # change since subset doesn't like input same as column name?
    location_id <- locationId

    name <- subset(locations,
                   locationId == location_id,
                   select = c(name))

    url <- sprintf("https://automl.googleapis.com/v1beta1/%s",
                   name)

    f <- googleAuthR::gar_api_generator(url,
                                        "GET",
                                        data_parse_function = function(x) x)

    response <- f()

    out <- response

    structure(out, class = "gcat_location")

}
