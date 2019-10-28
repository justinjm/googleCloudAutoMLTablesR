#' A helper function that tests whether an object is either NULL _or_
#' a list of NULLs
#'
#' @keywords internal
is.NullOb <- function(x) is.null(x) | all(sapply(x, is.null))

#' Recursively step down into list, removing all such objects
#'
#' @keywords internal
rmNullObs <- function(x) {
  x <- Filter(Negate(is.NullOb), x)
  lapply(x, function(x) if (is.list(x)) rmNullObs(x) else x)
}

#' if argument is NULL, no line output
#'
#' @keywords internal
#' @noRd
cat0 <- function(prefix = "", x){
  if(!is.null(x)){
    cat(prefix, x, "\n")
  }
}

#' Unboxing of entry into a list
#' https://github.com/justinjm/googleCloudAutoMLTablesR/issues/1#issuecomment-510526353
jubox <- function(x) jsonlite::unbox(x)

#' base R safe rbind
#'
#' Send in a list of data.fames with different column names
#'
#' @return one data.frame
#' a safe rbind for variable length columns
#' @noRd
my_reduce_rbind <- function(x){
  classes <- lapply(x, inherits, what = "data.frame")
  stopifnot(all(unlist(classes)))

  # all possible names
  df_names <- Reduce(union, lapply(x, names))

  df_same_names <- lapply(x, function(y){
    missing_names <- setdiff(df_names,names(y))
    num_col <- length(missing_names)
    if(num_col > 0){
      missing_cols <- vapply(missing_names, function(i) NA, NA, USE.NAMES = TRUE)

      new_df <- data.frame(matrix(missing_cols, ncol = num_col))
      names(new_df) <- names(missing_cols)
      y <- cbind(y, new_df, row.names = NULL)
    }

    y[, df_names]

  })

  Reduce(rbind, df_same_names)
}

