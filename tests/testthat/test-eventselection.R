test_that("creating events dataframe works", {
  raw <- read_raw_dataframes("test_data/timing_?_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  events <- make_events_df(raw)
  expect_s3_class(events, "tbl_df")
  expect_equal(nrow(events), 4819L)
  nslices <- sum(events$nslices)
  expect_equal(nslices, 23422L)
})

test_that("creating global dataframe works", {
  print("Starting test of global dataframe")
  raw <- read_raw_dataframes("test_data/timing_?_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  ranks <- make_global_df(raw)
  expect_s3_class(ranks, "tbl_df")
  expect_equal(nrow(ranks), 4L)
  expected_names = c("rank", "start", "post_dataset",
                     "post_block_configurations", "post_broadcast",
                     "pre_decompose", "post_decompose", "pre_execute_block",
                     "post_execute_block", "pre_create_partners",
                     "pre_reduction", "post_reduction",
                     "finish", "makeds",
                     "calcbc", "broadcast", "prep", "makeblocks",
                     "makelambda", "executeblock", "makepartners",
                     "reduction", "output", "total")
  expect_named(ranks, expected=expected_names)
})

test_that("creating reduction pass dataframe works", {
  raw <- read_raw_dataframes("test_data/timing_?_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  rounds <- make_reduction_phase_df(raw)
  expect_s3_class(rounds, "tbl_df")
  expect_named(rounds, expected=c("rank", "start", "mid", "end",
                                  "duration", "deq", "enq",
                                  "bid","nslices","round"),
               ignore.order = TRUE)
  expect_equal(nrow(rounds), 16L)
})

test_that("creating reduction loop1 dataframe works", {
  raw <- read_raw_dataframes("test_data/timing_3_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  loop1 <- make_reduction_loop1_df(raw)
  expect_s3_class(loop1, "tbl_df")
  expect_named(loop1,
               expected = c("rank", "start", "med", "end",
                            "idx", "incoming_bid", "ndq", "bid", "round",
                            "t_dq", "t_red", "t_tot"),
               ignore.order = TRUE)
  expect_equal(nrow(loop1), 2L)
  expect_equal(loop1$incoming_bid, c(8, 9))
  expect_equal(loop1$idx, c(0, 1))
  expect_equal(loop1$ndq, c(0, 9))
  expect_equal(loop1$bid, c(8, 8))
  expect_equal(loop1$round, c(1, 1))
})

test_that("creating reduction loop2 dataframe works", {
  raw <- read_raw_dataframes("test_data/timing_3_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  loop2 <- make_reduction_loop2_df(raw)
  expect_s3_class(loop2, "tbl_df")
  expect_named(loop2,
               expected = c("rank", "start","end", "idx", "target_bid",
                            "nenq", "bid", "round", "t_tot"),
               ignore.order = TRUE)
  expect_equal(nrow(loop2),3L)
  expect_equal(loop2$bid, c(9, 8, 8))
  expect_equal(loop2$round, c(0, 0, 1))
  expect_equal(loop2$idx, c(0, 0, 0))
  expect_equal(loop2$target_bid, c(8, 8, 0))
})

test_that("reduction pass dataframe is correct", {
  raw <- read_raw_dataframes("test_data/timing_3_4_500.dat")
  rounds <- make_reduction_phase_df(raw)
  expect_equal(nrow(rounds), 3L)
  expect_equal(rounds$rank, c(3,3,3))
  expect_equal(rounds$bid, c(9, 8, 8))
  expect_equal(rounds$nslices, c(9, 6, 15))
  expect_equal(rounds$round, c(0, 0, 1))
})

test_that("events dataframe is correct", {
  raw <- read_raw_dataframes("test_data/timing_3_4_500.dat")
  events <- make_events_df(raw)
  expect_named(events,
               expected = c("rank", "bid", "evt",
                            "precr", "postfr", "postcr", "postps",
                            "nslices", "nbytes",
                            "load", "rec","filt"),
               ignore.order = TRUE)
  expect_equal(nrow(events), 919L)
  expect_equal(events$rank, rep(3L, 919L))
  top3 <- head(events, 3L)
  expect_equal(top3$evt, c(131865, 131918, 132002))
  expect_equal(top3$nslices, c(2, 3, 6))
  expect_equal(top3$nbytes, c(3312, 5916, 11280))
  # expect_equal(top3$)
})
