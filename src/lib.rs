#![forbid(unsafe_code)]
#![deny(clippy::panic, clippy::expect_used, clippy::unwrap_used)]

#![cfg_attr(feature = "sqs", cfg_attr(feature = "sns", doc = include_str!("../README.md")))]

pub mod offload;
