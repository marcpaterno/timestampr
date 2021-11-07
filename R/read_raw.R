#' Return the version number (an integer) for the given (raw) datarame.
#'
#' version 1 data files are recognized by having 3 columns
#' version 2 data files are recognized by having 4 columns
#'
#' Any other number of columns produces an error.
#'
#' @param df A dataframe, as obtained by read_csv from a raw timestamp file
#'
#' @return The version number of the data format
#' @export
#'
raw_data_version <- function(df)
{
  ncols <- length(df)
  version = -1
  if (ncols == 3) version <- 1
  if (ncols == 4 ) version <-  2
  if (version == -1) stop("The given dataframe is not from a recognized raw file format")
  version
}


#' Create a tibble from a single rank timing file.
#'
#' See the documentation for read_raw_dataframes for a description of the
#' resulting dataframe.
#'
#' Note that an empty file will yield the most recent version dataframe, which
#' may not be expected by older code using `timestampr`.
#'
#' @param fname name of the file to read
#'
#' @return a data.frame
#' @export
#'
read_raw <- function(fname)
{
  checkmate::assert_scalar(fname)
  checkmate::assert_file_exists(fname)

  if (file.size(fname) == 0) {
    return(data.frame(ts = double(),
                      data = integer(),
                      sdata = character(),
                      step = character(),
                      rank = integer()))
  }

  filename <- basename(fname)
  parts <- stringr::str_split(filename, "[\\._]")[[1]]
  rank <- as.integer(parts[2])
  #result <- utils::read.csv(fname, header = FALSE, col.names = c("ts", "data", "step"), as.is = TRUE)
  result <- utils::read.csv(fname, header = FALSE, as.is = TRUE)


  if (raw_data_version(result) == 1)
    names(result) <- c("ts", "data", "step")
  else
    names(result) <- c("ts", "data", "sdata", "step")

  result$rank <- rank
  # Note: we do not turn the data.frame into a tibble here, because a bug in bind_rows
  # creates a later failure if we're trying to bind tibbles.
  #
  # Because of a problem in some versions of the record printing, strip leading and trailing spaces
  # from step names.
  result %>%
    dplyr::mutate(step = stringr::str_trim(.data$step))
}
