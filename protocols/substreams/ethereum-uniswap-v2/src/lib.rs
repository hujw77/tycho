#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod abi;
pub mod core;
#[cfg(feature = "standalone-handlers")]
mod modules;
mod pb;

#[cfg(feature = "standalone-handlers")]
pub use modules::*;

mod store_key;
mod traits;
