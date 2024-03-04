//! # car-mirror-wasm
//!
//! This crate exposes wasm bindings to car-mirror *client* functions.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]
#![cfg(target_arch = "wasm32")]

/// A `BlockStore` implementation based on a JS interface
pub mod blockstore;
/// Bindings to the request and response messages used in car mirror
pub mod messages;

mod exports;
mod utils;

pub use exports::*;

//------------------------------------------------------------------------------
// Utilities
//------------------------------------------------------------------------------

/// Panic hook lets us get better error messages if our Rust code ever panics.
///
/// For more details see
/// <https://github.com/rustwasm/console_error_panic_hook#readme>
#[wasm_bindgen::prelude::wasm_bindgen(js_name = "setPanicHook")]
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}
