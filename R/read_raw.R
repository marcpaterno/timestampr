#' Create a tibble from a single rank timing file.
#'
#' See the documentation for read_raw_dataframes for a description of the
#' resulting dataframe.
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
  filename <- basename(fname)
  parts <- stringr::str_split(filename, "[\\._]")[[1]]
  rank <- as.integer(parts[2])
  result <- utils::read.csv(fname, header = FALSE, col.names = c("ts", "data", "step"), as.is = TRUE)

  if (nrow(result) == 0) {
    return(data.frame(ts = double(),
                      data = integer(),
                      step = character(),
                      rank = integer()))
  }
  result$rank <- rank
  # Note: we do not turn the data.frame into a tibble here, because a bug in bind_rows
  # creates a later failure if we're trying to bind tibbles.
  #
  # Because of a problem in some versions of the record printing, strip leading and trailing spaces
  # from step names.
  result %>%
    dplyr::mutate(step = stringr::str_trim(.data$step))
}
