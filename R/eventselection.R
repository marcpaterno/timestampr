
#' Extract event-level data from an evenselection dataframe.
#'
#' Each row in the returned dataframe corresponds to a single event
#' processed.
#'
#' The dataframe columns are:
#' \describe{
#'    \item{rank}{the MPI rank on which the call was made}
#'    \item{start}{time at start of the rank}
#'    \item{precr}{timestamp before create_records call}
#'    \item{postcr}{timestamp after create_records call}
#'    \item{postfr}{timestamp after records have been filled}
#'    \item{postps}{timestamp after slices have been filter for selection}
#'    \item{load}{duration of loading data from HEPnOS (ms)}
#'    \item{rec}{duration of record creation (ms)}
#'    \item{filt}{duration of filter criterion application (ms)}
#'    \item{evt}{id of this event}
#'    \item{bid}{id of block which arried this event}
#'    \item{nbytes}{sum of the size of records (in bytes) of this event}
#'    \item{nslices}{number of slices in this event}
#' }
#'
#' @param dx  an eventselection raw dataframe
#' @return a tibble containing event-level data
#' @export
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
make_events_df <- function(dx)
{
  event_steps = c("pre_create_records", "post_fill_records", "post_create_records", "post_process_slices")
  nsteps <- length(event_steps)
  x <- dplyr::select(dx, .data$ts, .data$step, .data$rank) %>% dplyr::filter(.data$step %in% event_steps)
  add_pass <- function(d, key) { nr <- nrow(d)/nsteps ; dplyr::mutate(d, pass = rep(1:nr, each = nsteps)) }
  res <- dplyr::group_by(x, .data$rank) %>%
    dplyr::group_modify(add_pass) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(names_from = .data$step, values_from = .data$ts) %>%
    dplyr::rename(precr = .data$pre_create_records,
                  postfr = .data$post_fill_records,
                  postcr = .data$post_create_records,
                  postps = .data$post_process_slices)

  evt <- dplyr::filter(dx, .data$step == "pre_create_records") %>%
         dplyr::pull(.data$data)

  nslices <- dplyr::filter(dx, .data$step == "post_create_records") %>%
             dplyr::pull(.data$data)

  nbytes <- dplyr::filter(dx, .data$step == "post_fill_records") %>%
            dplyr::pull(.data$data)
  bid <- dplyr::filter(dx, .data$step == "post_process_slices") %>%
         dplyr::pull(.data$data)

  res %>%
    dplyr::mutate(evt = evt,
                  nslices = nslices,
                  nbytes = nbytes,
                  bid = bid,
                  load = .data$postfr - .data$precr,
                  rec = .data$postcr - .data$postfr,
                  filt = .data$postps - .data$postcr) %>%
    dplyr::select(-.data$pass)
}

#' Extract global data (once-per-rank data) from an eventselection dataframe.
#'
#' Each row in the returned dataframe corresponds to one rank in the program.
#'
#' The dataframe columns are:
#' \describe{
#'     \item{rank}{the MPI rank on which the call was made}
#'     \item{start}{time at which the rank started running}
#'     \item{post_dataset}{time at which the HEPnOS dataset creation finished}
#'     \item{post_block_configurations}{time at which creation of EventSelectionConfigs finished}
#'     \item{post_broadcast}{time at which broadcast of EventSelectionConfigs is finished}
#'     \item{pre_decompose}{time before making DIY blocks}
#'     \item{post_decompose}{time after making DIY blocks}
#'     \item{pre_execute_block}{time before execution of blocks}
#'     \item{post_execute_block}{time after execution of blocks}
#'     \item{pre_create_partners}{time before creation of DIY merge partners}
#'     \item{pre_reduction}{time before DIY reduction starts }
#'     \item{post_reduction}{time after DIY reduction finishes}
#'     \item{finish}{time after output is written (see note 3 below)}
#'     \item{makeds}{duration for creation of HEPnOS dataset}
#'     \item{calcbc}{duration for making EventSelectionConfigs (see note 1 below)}
#'     \item{broadcast}{duration of broadcast (see note 1 below)}
#'     \item{prep}{duration of prep work before making blocks}
#'     \item{makeblocks}{duration of block creation}
#'     \item{makelambda}{duration creation of prefetcher and of the lambda that executes the blocks}
#'     \item{executeblock}{duration of the execution of all blocks}
#'     \item{makepartners}{duration of the creation of merge partners}
#'     \item{reduction}{duration of all reduction rounds}
#'     \item{output}{duration of writing result file (see note 2 below)}
#'     \item{total}{total running duration}
#' }
#'
#' Note 1: Only the local group leaders calculate block configurations, so
#' for most ranks the `calcbc` value will be close to zero. For these ranks,
#' the `broadcast` time will include the time spent waiting for the group
#' leaders to do their calculations of block configurations.
#'
#' Note 2: Only rank 0 writes the output file; others ranks should have close
#' to zero time for `output`.
#'
#' Note 3: The timingdata files are written *after* the finish timestamp is
#' recorded.
#'
#' @param dx an eventselection raw dataframe
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

#' Extract reduction round information from an eventselection dataframe.
#'
#' Each row in the returned dataframe corresponds to one call to the reduceData
#' function, for a given block.
#'
#' The dataframe columns are:
#' \describe{
#'     \item{rank}{the MPI rank on which the call was made}
#'     \item{start}{time at start of function}
#'     \item{mid}{time after end of first loop}
#'     \item{end}{time at end of the function}
#'     \item{duration}{total function duration}
#'     \item{deq}{duration of first loop (dequeue and merge)}
#'     \item{enq}{duration of second loop (enqueue)}
#'     \item{bid}{id of block on which reduceData was called}
#'     \item{nslices}{number of SliceIDs carrired by this block after the merge}
#'     \item{round}{the reduction round}
#' }
#'
#' @param dx an eventselection raw dataframe
#' @return a tibble containing reduction phases information
#' @export
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
make_reduction_phase_df <- function(dx)
{
  reduction_steps <- c("start_reduce_data", "mid_reduce_data", "end_reduce_data")
  # step                 meaning of 'data'
  # ---------------------------------------
  # start_reduce_data    current block id
  # mid_reduce_data      number of slices held by current block after dequeuing
  # end_reduce_data      round number
  nsteps <- length(reduction_steps)
  checkmate::assert_count(nsteps)
  x <- dplyr::select(dx, .data$ts, .data$step, .data$rank) %>% dplyr::filter(.data$step %in% reduction_steps)
  add_pass <- function(d, key) { nr <- nrow(d)/nsteps ; dplyr::mutate(d, pass = rep(1:nr, each = nsteps)) }
  res <- dplyr::group_by(x, .data$rank) %>%
    dplyr::group_modify(add_pass) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(names_from = .data$step, values_from = .data$ts) %>%
    dplyr::rename(start = .data$start_reduce_data, mid = .data$mid_reduce_data, end = .data$end_reduce_data) %>%
    dplyr::mutate(duration = .data$end - .data$start,
                  deq = .data$mid - .data$start,
                  enq = .data$end - .data$mid)
  nslices <-
    dplyr::filter(dx, .data$step == "mid_reduce_data") %>%
    dplyr::pull(.data$data)
  bid <-
    dplyr::filter(dx, .data$step == "start_reduce_data") %>%
    dplyr::pull(.data$data)
  round <-
    dplyr::filter(dx, .data$step == "end_reduce_data") %>%
    dplyr::pull(.data$data)
  res %>%
    dplyr::mutate(nslices = nslices,
                  bid = bid,
                  round = round) %>%
    dplyr::select(-.data$pass)
}

#' Extract reduction loop 1 information from an eventselection dataframe.
#'
#' Each row in the dataframe corresponds to an iteration in the
#' dequeue-and-merge loop.
#'
#' The dataframe columns are:
#' \describe{
#'     \item{rank}{the MPI rank on which the loop was executed}
#'     \item{start}{the time at the start of the loop}
#'     \item{med}{the time after dequeuing slices but before reduction}
#'     \item{end}{the time after the reduction}
#'     \item{idx}{the loop index for this iteration}
#'     \item{incoming_bid}{the id of the incoming block on this iteration}
#'     \item{ndq}{number of slices dequeued this iteration}
#'     \item{bid}{the id of the block on which reduceData was called}
#'     \item{round}{the reduction round}
#'     \item{t_dq}{duration of dequeue call}
#'     \item{t_red}{duration of the reduce call}
#'     \item{t_tot}{total duration of the loop}
#' }
#'
#' @param dx an eventselection raw dataframe
#' @return a tibble containing reduction loop 1 information
#' @export
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
make_reduction_loop1_df <- function(dx)
{
  reduction_steps <- c("start_dequeue_loop", "pre_dequeue", "pre_block_reduce", "post_block_reduce", "end_dequeue_loop")
  # step                 meaning of 'data'
  # ---------------------------------------
  # start_dequeue_loop   incoming block id
  # pre_dequeue          loop index
  # pre_block_reduce     number of SliceIDs dequeued this loop
  # post_block_reduce    our block id
  # end_dequeue_loop     round

  nsteps <- length(reduction_steps)
  dx <- dplyr::filter(dx, .data$step %in% reduction_steps)
  add_pass <- function(d, key) { nr <- nrow(d)/nsteps ; dplyr::mutate(d, pass = rep(1:nr, each = nsteps)) }
  res <-
    dplyr::group_by(dx, .data$rank) %>%
    dplyr::group_modify(add_pass) %>%
    dplyr::ungroup() %>%
    dplyr::select(.data$rank, .data$pass, .data$step, .data$ts) %>% # we do not keep data here
    # each pair of (rank, pass) defines a distinct record
    tidyr::pivot_wider(names_from = .data$step, values_from = .data$ts)

  incoming_bid <-
    dx %>%
    dplyr::filter(.data$step == "start_dequeue_loop") %>%
    dplyr::pull(.data$data)
  idx <-
    dx %>%
    dplyr::filter(.data$step == "pre_dequeue") %>%
    dplyr::pull(.data$data)
  ndq <-
    dx %>%
    dplyr::filter(.data$step == "pre_block_reduce") %>%
    dplyr::pull(.data$data)
  bid <-
    dx %>%
    dplyr::filter(.data$step == "post_block_reduce") %>%
    dplyr::pull(.data$data)
  round <-
    dx %>%
    dplyr::filter(.data$step == "end_dequeue_loop") %>%
    dplyr::pull(.data$data)

  dplyr::transmute(res,
                   rank = rank,
                   bid = bid,
                   round = round,
                   idx = idx,
                   incoming_bid = incoming_bid,
                   ndq = ndq,
                   start = .data$pre_dequeue,
                   med = .data$pre_block_reduce,
                   end = .data$post_block_reduce,
                   t_dq  = .data$med - .data$start,
                   t_red = .data$end - .data$med,
                   t_tot = .data$end - .data$start)
}

#' Extract reduction loop 2 (enqueue) information from an eventselection
#' dataframe
#'
#' Each row in the dataframe corresponds to an iteration in the enqueue loop.
#'
#' The dataframe columns are:
#' \describe{
#'     \item{rank}{the MPI rank on which the loop was executed}
#'     \item{start}{the time at the start of the loop}
#'     \item{end}{the time after the enqueuing}
#'     \item{idx}{the loop index for this iteration}
#'     \item{target_bid}{the id of the block to which the enqueued data are sent}
#'     \item{nenq}{number of slices enqueued this iteration}
#'     \item{bid}{the id of the block on which reduceData was called}
#'     \item{round}{the reduction round}
#'     \item{t_tot}{total duration of the loop}
#' }
#'
#' @param dx an eventselection raw dataframe
#' @return a tibble containing reduction loop 2 information
#' @export
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
make_reduction_loop2_df <- function(dx)
{
  reduction_steps <- c("start_enqueue_loop", "pre_enqueue", "post_enqueue", "pre_end_enqueue_loop",
                       "end_enqueue_loop")
  # step                 meaning of 'data'
  # ---------------------------------------
  # start_enqueue_loop   loop index
  # pre_enqueue          target block id
  # post_enqueue         number of slices enqueued
  # pre_end_enqueue_loop our block id
  # end_enqueue_loop     round

  nsteps <- length(reduction_steps)
  dx <- dplyr::filter(dx, .data$step %in% reduction_steps)
  add_pass <- function(d, key) { nr <- nrow(d)/nsteps ; dplyr::mutate(d, pass = rep(1:nr, each = nsteps)) }
  res <-
    dplyr::group_by(dx, .data$rank) %>%
    dplyr::group_modify(add_pass) %>%
    dplyr::ungroup() %>%
    dplyr::select(.data$rank, .data$pass, .data$step, .data$ts) %>% # we do not keep data here
    # each pair of (rank, pass) defines a distinct record
    tidyr::pivot_wider(names_from = .data$step, values_from = .data$ts)

  idx <-
    dx %>%
    dplyr::filter(.data$step == "start_enqueue_loop") %>%
    dplyr::pull(.data$data)
  target_bid <-
    dx %>%
    dplyr::filter(.data$step == "pre_enqueue") %>%
    dplyr::pull(.data$data)
  nenq <-
    dx %>%
    dplyr::filter(.data$step == "post_enqueue") %>%
    dplyr::pull(.data$data)
  bid <-
    dx %>%
    dplyr::filter(.data$step == "pre_end_enqueue_loop") %>%
    dplyr::pull(.data$data)
  round <-
    dx %>%
    dplyr::filter(.data$step == "end_enqueue_loop") %>%
    dplyr::pull(.data$data)
  dplyr::transmute(res,
                   rank = rank,
                   bid = bid,
                   round = round,
                   idx = idx,
                   target_bid = target_bid,
                   nenq = nenq,
                   start = .data$pre_enqueue,
                   end = .data$post_enqueue,
                   t_tot = .data$end - .data$start)
}
