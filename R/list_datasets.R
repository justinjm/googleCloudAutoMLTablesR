#' Lists datasets in a project.
#'
#' @param parent The resource name of the project from which to list datasets
#' @param filter An expression for filtering the results of the request
#' @param pageToken A token identifying a page of results for the server to return
#' @param pageSize Requested page size
#' @export
gcat_list_datasets <- function(parent,
                               filter = NULL,
                               pageToken = NULL,
                               pageSize = NULL) {

  url <- sprintf("https://automl.googleapis.com/v1beta1/%s/datasets",
                 parent)

  # automl.projects.locations.datasets.list
  pars = list(filter = filter,
              pageToken = pageToken,
              pageSize = pageSize)

  f <- googleAuthR::gar_api_generator(url,
                                      "GET",
                                      pars_args = rmNullObs(pars),
                                      data_parse_function = function(x) x)
  f()

}
