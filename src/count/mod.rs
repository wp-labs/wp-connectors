//! Count source/sink implementations for wp-connectors
//!
//! 该模块用于调试与压测场景

mod factory;
mod sink;
mod source;

pub use factory::{CountSinkFactory, CountSourceFactory};
pub use sink::CountSink;
pub use source::CountSource;
