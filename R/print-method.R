#' @export
print.gcat_dataset <- function(x,...){
  cat("==Google Cloud AutoML Table Dataset==\n")
  cat0("Display Name:               ", x$displayName)
  cat0("Example Count:              ", x$exampleCount)
  cat0("Create Time:                ", x$createTime)
  cat0("Primary Table Spec ID:      ", x[["tablesDatasetMetadata"]][["primaryTableSpecId"]])

}

#' @export
print.gcat_table_specs <- function(x,...){
  cat("==Google Cloud AutoML Table==\n")
  cat0("Rows:                      ", x$rowCount)
  cat0("Valid Rows:                ", x$validRowCount)
  cat0("Columns:                   ", x$columnCount)
  cat0("Name:                      ", x$name)
  # cat0("Display Name:              ", x$dataset_display_name)# TODO - @justinjm - add display name

}
