// The typed-html crate does pretty deep macro calls. Bump if
// recursion limit compilation errors return for html!() calls.
#![recursion_limit = "256"]
#[macro_use]
extern crate slog;
extern crate slog_term;

pub mod agent;
