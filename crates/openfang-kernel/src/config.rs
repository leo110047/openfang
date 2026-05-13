//! Configuration loading compatibility module.
//!
//! The implementation lives in `openfang-types` so runtime infrastructure and
//! kernel startup use the same include/deep-merge/default semantics.

pub use openfang_types::config::{deep_merge_toml, default_config_path, load_config, openfang_home};
