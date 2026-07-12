#[cfg(test)]
extern crate self as tycho_indexer;

pub mod cli;
#[cfg(test)]
pub mod config;
pub mod extractor;
#[allow(clippy::result_large_err)]
pub mod pb;
pub mod services;
pub mod substreams;

#[allow(clippy::extra_unused_lifetimes)]
#[cfg(test)]
pub mod testing;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;
