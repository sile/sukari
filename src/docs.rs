//! Supplementary documents on specific topics in `sukari`
//!
//! Instead of describing individual types or components, this module collects
//! topics that span multiple components.

// The document bodies live in Markdown files under docs/ and are included via
// include_str!. This makes them viewable on docs.rs, and Rust code examples in
// the documents are verified as doctests.

/// Supplementary document on the storage architecture
#[doc = include_str!("../docs/architecture-overview.md")]
pub mod architecture_overview {}

/// Supplementary document on the segment format
#[doc = include_str!("../docs/segment-format.md")]
pub mod segment_format {}
