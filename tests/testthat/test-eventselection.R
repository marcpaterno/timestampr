test_that("creating events dataframe works", {
  raw <- read_raw_dataframes("timing_?_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  events <- make_events_df(raw)
  expect_s3_class(events, "tbl_df")
  expect_equal(nrow(events), 4819L)
  nslices <- sum(events$nslices)
  expect_equal(nslices, 23422L)
})

test_that("creating global dataframe works", {
  raw <- read_raw_dataframes("timing_?_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  ranks <- make_global_df(raw)
  expect_s3_class(ranks, "tbl_df")
  expect_equal(nrow(ranks), 4L)
})

test_that("creating reduction pass dataframe works", {
  raw <- read_raw_dataframes("timing_?_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  pass <- make_reduction_phase_df(raw)
  expect_s3_class(pass, "tbl_df")
  expect_equal(nrow(pass), 16L)
})

test_that("creating reduction loop1 dataframe works", {
  raw <- read_raw_dataframes("timing_3_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  loop1 <- make_reduction_loop1_df(raw)
  expect_s3_class(loop1, "tbl_df")
  expect_equal(nrow(loop1), 2L)
  expect_equal(loop1$incoming_bid, c(8, 9))
  expect_equal(loop1$idx, c(0, 1))
  expect_equal(loop1$ndq, c(0, 9))
  expect_equal(loop1$bid, c(8, 8))
  expect_equal(loop1$round, c(1, 1))
})

test_that("creating reduction loop2 dataframe works", {
  raw <- read_raw_dataframes("timing_3_4_500.dat")
  expect_s3_class(raw, "tbl_df")
  loop2 <- make_reduction_loop2_df(raw)
  expect_s3_class(loop2, "tbl_df")
  expect_equal(nrow(loop2),3L)
})

test_that("reduction pass dataframe is correct", {
  raw <- read_raw_dataframes("timing_3_4_500.dat")
  pass <- make_reduction_phase_df(raw)
  expect_equal(nrow(pass), 3L)
  expect_equal(pass$rank, c(3,3,3))
  expect_equal(pass$pass, 1:3)
  expect_equal(pass$nslices, c(9, 6, 15))
})