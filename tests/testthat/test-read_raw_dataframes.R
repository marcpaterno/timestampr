test_that("reading multiple old dataframes works", {
  # Old means before the introduction of detailed reduction information
  d <- read_raw_dataframes("timing_?_7056_100.dat.xz")
  expect_s3_class(d, "tbl_df")
  num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
  expect_equal(num_ranks, 10)
})

test_that("reading multiple new dataframes works", {
  # Old means before the introduction of detailed reduction information
  d <- read_raw_dataframes("timing_?_7168_100.dat.xz")
  expect_s3_class(d, "tbl_df")
  num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
  expect_equal(num_ranks, 10)
})

test_that("parallel reading multiple new dataframes works", {
  # Old means before the introduction of detailed reduction information
  d <- read_raw_dataframes("timing_?_7168_100.dat.xz", use_parallel = TRUE)
  expect_s3_class(d, "tbl_df")
  num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
  expect_equal(num_ranks, 10)
})