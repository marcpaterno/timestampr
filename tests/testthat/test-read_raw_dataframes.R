test_that("reading multiple v0 dataframes works", {
  # v0 means before the introduction of detailed reduction information
  d <- read_raw_dataframes("test_data/timing_?_7056_100.dat.xz")
  expect_s3_class(d, "tbl_df")
  num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
  expect_equal(num_ranks, 10)
})

test_that("reading multiple v1 dataframes works", {
  # v1  means before the introduction of detailed reduction information
  d <- read_raw_dataframes("test_data/timing_?_7168_100.dat.xz")
  expect_s3_class(d, "tbl_df")
  num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
  expect_equal(num_ranks, 10)
})

test_that("reading multiple v2 dataframes works", {
  # v2 means after the introduction of the 'sdata' column.
  d <- read_raw_dataframes("test_data/v2-timing_?_8_1.dat.xz")
  expect_s3_class(d, "tbl_df")
  num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
  expect_equal(num_ranks, 8)
})

# TODO: Figure out why this test works when run with devtools::test,
# and when run by devtools::check(cran = FALSE), but fails when
# run with devtools::check()
#
# test_that("parallel reading multiple new dataframes works", {
#   d <- read_raw_dataframes("timing_?_7168_100.dat.xz", use_parallel = TRUE)
#   expect_s3_class(d, "tbl_df")
#   num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
#   expect_equal(num_ranks, 10)
# })