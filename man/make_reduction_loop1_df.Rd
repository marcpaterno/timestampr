% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/eventselection.R
\name{make_reduction_loop1_df}
\alias{make_reduction_loop1_df}
\title{Extract reduction loop 1 information from an eventselection dataframe.}
\usage{
make_reduction_loop1_df(dx)
}
\arguments{
\item{dx}{an eventselection raw dataframe}
}
\value{
a tibble containing reduction loop 1 information
}
\description{
Each row in the dataframe corresponds to an iteration in the
dequeue-and-merge loop.
}
\details{
The dataframe columns are:
\describe{
    \item{rank}{the MPI rank on which the loop was executed}
    \item{bid}{the id of the block on which reduceData was called}
    \item{round}{the reduction round}
    \item{idx}{the loop index for this iteration}
    \item{incoming_bid}{the id of the incoming block on this iteration}
    \item{ndq}{number of slices dequeued this iteration}
    \item{start}{the time at the start of the loop}
    \item{med}{the time after dequeuing slices but before reduction}
    \item{end}{the time after the reduction}
    \item{t_dq}{duration of dequeue call}
    \item{t_red}{duration of the reduce call}
    \item{t_tot}{total duration of the loop}
}
}
