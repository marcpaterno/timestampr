
#' Extract event-level data from an evenselection dataframe
#'
#' @param dx  an eventselection raw dataframe
#'
#' @return a tibble containing event-level data
#' @export
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
make_events_df <- function(dx)
{
  event_steps = c(
    "pre_create_records",
    "post_fill_records",
    "post_create_records",
    "post_process_slices"
  )
  x <- dx %>% dplyr::filter(.data$step %in% event_steps)

  nevents <- nrow(x) / 4
  x <- dplyr::mutate(x, evt = rep(1:nevents, each = 4))

  evts <-
    tidyr::pivot_wider(x,
      id_cols = c(.data$rank, .data$evt),
      names_from = .data$step,
      values_from = .data$ts
    ) %>%
    dplyr::rename(
      precr = .data$pre_create_records,
      postfr = .data$post_fill_records,
      postcr = .data$post_create_records,
      postps = .data$post_process_slices
    )
  # Next, we collect the number of slices in each event.

  slices_per_event <-
    x %>%
    dplyr::filter(.data$step == "post_create_records") %>%
    dplyr::select(nslices = .data$data, .data$evt)
  bytes_per_event <-
    x %>%
    dplyr::filter(.data$step == "post_fill_records") %>%
    dplyr::select(nbytes = .data$data, .data$evt)
  both_per_event <-
    dplyr::left_join(slices_per_event, bytes_per_event, by = "evt")

  # The final step of preparation is to put the number of slices per event
  #into the `evts` dataframe.
  #We calculate the time taken for three tasks:

  # 1. `load`, the time taken to read the data from _HEPnOS_ into the program memory,
  # 2. `rec`, the time taken to convert from the read structure to the _StandardRecord_ format, and
  # 3. `filt`, the time taken to run the NOvA filtering code on the _StandardRecord_ data.

  evts <-
    dplyr::left_join(evts, both_per_event, by = "evt")

  evts <-
    evts %>%
    dplyr::mutate(load = .data$postfr - .data$precr,
           rec = .data$postcr - .data$postfr,
           filt = .data$postps - .data$postcr)

  evts
}

#' Extract global (once per rank) data from an eventselection dataframe
#'
#' @param dx an eventselection raw dataframe
#'
#' @return a tibble containing global data
#' @export
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
make_global_df <- function(dx)
{
  global_steps = c("finish", "post_block_configurations", "post_broadcast", "post_dataset", "post_decompose", "post_reduction",
                   "pre_decompose", "pre_execute_block", "post_execute_block", "pre_reduction", "start")
  globals <- dplyr::filter(dx, .data$step %in% global_steps)
  globals <- tidyr::pivot_wider(globals, id_cols = .data$rank, names_from = .data$step, values_from = .data$ts)
  globals <- globals %>%
    dplyr::mutate(makeds = .data$post_dataset - .data$start,
           calcbc = .data$post_block_configurations - .data$post_dataset,
           broadcast = .data$post_broadcast - .data$post_block_configurations,
           prep = .data$pre_decompose - .data$post_broadcast,
           makeblocks = .data$post_decompose - .data$pre_decompose,
           makelambda = .data$pre_execute_block - .data$post_decompose,
           executeblock = .data$post_execute_block - .data$pre_execute_block,
           reduction = .data$post_reduction - .data$pre_reduction,
           output = .data$finish - .data$post_reduction,
           total = .data$finish - .data$start)
  globals
}


