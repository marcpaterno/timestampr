#' Create a tibble from all the rank timing files for a program run.
#'
#' Each row of the returned dataframe corresponds to a single recorded
#' timestamp.
#'
#' The dataframe columns are:
#' \describe{
#'     \item{ts}{the MPI timestamp recorded for this "event".}
#'     \item{data}{a context-dependent (integer) data value. See notes below.}
#'     \item{step}{the kind of "event" to which this record corresponds.}
#'     \item{rank}{the MPI rank in which this "event" was recorded.}
#' }
#'
#' The meaning of each "data" value depends on the "step" for which it is
#' recorded. See the documentation for the functions make_events_df,
#' make_global_df, make_reduction_phase_df, make_reduction_loop1_df, and
#' make_reduction_loop2_df for the details.
#'
#' @param fileglob a glob pattern identifying all the files to read
#' @param use_parallel force use of parallel reading
#'
#' @return a tibble combining all the dataframes read from the files
#' @export
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
read_raw_dataframes <- function(fileglob, use_parallel = NULL)
{
  checkmate::assert_scalar(fileglob)
  checkmate::assert_string(fileglob)
  fnames <- Sys.glob(fileglob)
  checkmate::assert_character(fnames, min.len = 1)
  num_real_cores <- parallel::detectCores(logical = FALSE)
  if (rlang::is_null(use_parallel))  use_parallel <- isTRUE(length(fnames) > 3 * num_real_cores)
  if (use_parallel) {
    cl <- parallel::makeForkCluster(num_real_cores)
    on.exit(parallel::stopCluster(cl))
    dfs <- parallel::parLapply(cl, fnames, read_raw)
  } else {
     dfs <- lapply(fnames, read_raw)
  }
  checkmate::assert_list(dfs)
  df <- dplyr::bind_rows(dfs)
  t0 = min(df$ts)
  df %>%
    tibble::as_tibble() %>%
    dplyr::mutate(rank = as.integer(rank),
                  ts = .data$ts - t0)
}
