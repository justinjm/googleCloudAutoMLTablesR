#' @export
print.gcat_location <- function(x,...){
  cat("==Google Cloud AutoML Tables Location==\n")
  cat0("name:                ", x$name)
  cat0("locationId:          ", x$locationId)

}

#' @export
print.gcat_dataset <- function(x,...){
  cat("==Google Cloud AutoML Tables Dataset==\n")
  cat0("displayName:         ", x$displayName)
  cat0("exampleCount:        ", x$exampleCount)
  cat0("createTime:          ", as.character(timestamp_to_r(x$createTime)))
  cat0("primaryTableSpecId:  ", x[["tablesDatasetMetadata"]][["primaryTableSpecId"]])
  cat0("targetColumnSpecId:  ", x[["tablesDatasetMetadata"]][["targetColumnSpecId"]])
  cat0("statsUpdateTime:     ", as.character(timestamp_to_r(x[["tablesDatasetMetadata"]][["statsUpdateTime"]])))
}

#' @export
print.gcat_table_specs <- function(x,...){
  cat("==Google Cloud AutoML Tables Table Spec==\n")
  cat0("columnCount:         ", x$columnCount)
  cat0("rowCount:            ", x$rowCount)
  cat0("validRowCount:       ", x$validRowCount)
  cat0("eTag:                ", x$etag)

}

#' @export
print.gcat_column_spec <- function(x,...){
  cat("==Google Cloud AutoML Tables Column Spec==\n")
  cat0("displayName:         ", x$displayName)
  cat0("dataType:            ", x[["dataType"]][["typeCode"]])
  cat0("distinctValueCount:  ", x[["dataStats"]][["distinctValueCount"]])
  cat0("eTag:                ", x$etag)

}

#' @export
print.gcat_model <- function(x,...){
  cat("==Google Cloud AutoML Tables Model==\n")
  cat0("name:                ", x$name)
  cat0("createTime:          ", as.character(timestamp_to_r(x[["metadata"]][["createTime"]])))

}
