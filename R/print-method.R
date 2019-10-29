#' @export
print.gcat_dataset <- function(x,...){
  cat("==Google Cloud AutoML Table Dataset==\n")
  cat0("displayName:               ", x$displayName)
  cat0("exampleCount:              ", x$exampleCount)
  cat0("createTime:                ", x$createTime)
  cat0("primaryTableSpecId:        ", x[["tablesDatasetMetadata"]][["primaryTableSpecId"]])

}

#' @export
print.gcat_table_specs <- function(x,...){
  cat("==Google Cloud AutoML TableSpec==\n")
  cat0("rowCount:                  ", x$rowCount)
  cat0("validRowCount:             ", x$validRowCount)
  cat0("columnCount:               ", x$columnCount)
  cat0("name:                      ", x$name)
  # cat0("Display Name:              ", x$dataset_display_name)# TODO - @justinjm - add display name

}
