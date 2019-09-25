#' @export
print.gcat_table_specs <- function(x,...){
  cat("==Google Cloud AutoML Table==\n")
  cat0("Rows:                 ", x$rowCount)
  cat0("Valid Rows:           ", x$validRowCount)
  cat0("Columns:              ", x$columnCount)
  cat0("Name:                 ", x$name)
  # cat0("Display Name:         ", x$dataset_display_name )
  # TODO - @justinjm - add display name

}
