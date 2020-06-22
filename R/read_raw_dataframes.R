#' Read several raw dataframes
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
  checkmate::check_scalar(fileglob)
  checkmate::check_string(fileglob)
  fnames <- Sys.glob(fileglob)
  checkmate::check_character(fnames, min.len = 1)
  num_real_cores <- parallel::detectCores(logical = FALSE)
  if (rlang::is_null(use_parallel))  use_parallel <- isTRUE(length(fnames) > 3 * num_real_cores)
  if (use_parallel) {
    cl <- parallel::makeForkCluster(num_real_cores)
    dfs <- parallel::parLapply(cl, fnames, read_raw)
    parallel::stopCluster(cl)
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
