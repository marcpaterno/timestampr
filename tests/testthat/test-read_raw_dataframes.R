test_that("reading multiple dataframes works", {
  d <- read_raw_dataframes("timing_?_7056_100.dat.xz")
  expect_s3_class(d, "tbl_df")
  num_ranks <- dplyr::select(d, rank) %>% dplyr::distinct() %>% nrow()
  expect_equal(num_ranks, 10)
})
