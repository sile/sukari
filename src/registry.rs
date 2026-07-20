//! Node registry metadata.

use crate::error::invalid_json;
use crate::storage::SyncPolicy;

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fmt,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

const NODE_REGISTRY_FILE_NAME: &str = "nodes.json";
const NODE_REGISTRY_TMP_FILE_NAME: &str = "nodes.json.tmp";
const NODE_REGISTRY_VERSION: u64 = 1;

/// Application-defined metadata for a Raft node.
///
/// The `startup` flag is interpreted by `sukari` for startup discovery.
/// The JSON metadata is stored and returned without application-specific
/// interpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMetadata {
    startup: bool,
    metadata: nojson::RawJsonOwned,
}

impl NodeMetadata {
    /// Makes node metadata.
    pub fn new(startup: bool, metadata: nojson::RawJsonOwned) -> Self {
        Self { startup, metadata }
    }

    /// Returns whether this node is marked for process startup.
    pub fn startup(&self) -> bool {
        self.startup
    }

    /// Returns application-defined opaque JSON metadata.
    pub fn metadata(&self) -> &nojson::RawJsonOwned {
        &self.metadata
    }

    /// Converts this value into application-defined opaque JSON metadata.
    pub fn into_metadata(self) -> nojson::RawJsonOwned {
        self.metadata
    }
}

impl Default for NodeMetadata {
    fn default() -> Self {
        Self {
            startup: false,
            metadata: nojson::RawJsonOwned::parse("{}")
                .expect("bug: empty object metadata should be valid JSON"),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct NodeRegistry {
    nodes: BTreeMap<noraft::NodeId, NodeRegistryEntry>,
}

impl NodeRegistry {
    pub(crate) fn load(dir: &Path) -> io::Result<Self> {
        let path = dir.join(NODE_REGISTRY_FILE_NAME);
        let mut text = String::new();
        match File::open(&path) {
            Ok(mut file) => {
                file.read_to_string(&mut text)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(e),
        }

        let json = nojson::RawJsonOwned::parse(text).map_err(invalid_json)?;
        parse_node_registry(json.value()).map_err(invalid_json)
    }

    pub(crate) fn save(&self, dir: &Path, sync: SyncPolicy) -> io::Result<()> {
        let path = dir.join(NODE_REGISTRY_FILE_NAME);
        let tmp_path = dir.join(NODE_REGISTRY_TMP_FILE_NAME);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(format_node_registry(self).as_bytes())?;
        if should_sync_metadata(sync) {
            file.sync_all()?;
        }
        drop(file);

        std::fs::rename(&tmp_path, &path)?;
        if should_sync_metadata(sync) {
            sync_dir(dir)?;
        }
        Ok(())
    }

    pub(crate) fn create_node(
        &mut self,
        node_id: noraft::NodeId,
        metadata: NodeMetadata,
    ) -> io::Result<()> {
        match self.nodes.entry(node_id) {
            Entry::Vacant(entry) => {
                entry.insert(NodeRegistryEntry {
                    metadata,
                    removed: false,
                });
                Ok(())
            }
            Entry::Occupied(_) => Err(node_already_exists_error()),
        }
    }

    pub(crate) fn remove_node(&mut self, node_id: noraft::NodeId) -> io::Result<()> {
        let Some(entry) = self.nodes.get_mut(&node_id) else {
            return Err(node_not_found_error());
        };
        if entry.removed {
            return Err(node_removed_error());
        }
        entry.removed = true;
        Ok(())
    }

    pub(crate) fn is_active(&self, node_id: noraft::NodeId) -> bool {
        self.nodes.get(&node_id).is_some_and(|entry| !entry.removed)
    }

    pub(crate) fn is_removed(&self, node_id: noraft::NodeId) -> bool {
        self.nodes.get(&node_id).is_some_and(|entry| entry.removed)
    }

    pub(crate) fn metadata(&self, node_id: noraft::NodeId) -> Option<&NodeMetadata> {
        self.nodes
            .get(&node_id)
            .filter(|entry| !entry.removed)
            .map(|entry| &entry.metadata)
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = (noraft::NodeId, &NodeMetadata)> + '_ {
        self.nodes
            .iter()
            .filter(|(_, entry)| !entry.removed)
            .map(|(node_id, entry)| (*node_id, &entry.metadata))
    }

    pub(crate) fn startup_nodes(
        &self,
    ) -> impl Iterator<Item = (noraft::NodeId, &NodeMetadata)> + '_ {
        self.nodes().filter(|(_, metadata)| metadata.startup())
    }

    pub(crate) fn active_node_ids(&self) -> BTreeSet<noraft::NodeId> {
        self.nodes().map(|(node_id, _)| node_id).collect()
    }

    pub(crate) fn active_node_count(&self) -> usize {
        self.nodes.values().filter(|entry| !entry.removed).count()
    }

    pub(crate) fn removed_node_count(&self) -> usize {
        self.nodes.values().filter(|entry| entry.removed).count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeRegistryEntry {
    metadata: NodeMetadata,
    removed: bool,
}

fn parse_node_registry(
    value: nojson::RawJsonValue<'_, '_>,
) -> Result<NodeRegistry, nojson::JsonParseError> {
    let version_value = value.to_member("version")?.required()?;
    let version: u64 = version_value.try_into()?;
    if version != NODE_REGISTRY_VERSION {
        return Err(version_value.invalid("unsupported node registry version"));
    }

    let nodes_value = value.to_member("nodes")?.required()?;
    let mut registry = NodeRegistry::default();
    for (key, value) in nodes_value.to_object()? {
        let key_text = key.to_unquoted_string_str()?;
        let node_id = key_text
            .parse()
            .map(noraft::NodeId::new)
            .map_err(|e| key.invalid(e))?;
        let entry = parse_node_registry_entry(value)?;
        if registry.nodes.insert(node_id, entry).is_some() {
            return Err(key.invalid("duplicate node ID"));
        }
    }
    Ok(registry)
}

fn parse_node_registry_entry(
    value: nojson::RawJsonValue<'_, '_>,
) -> Result<NodeRegistryEntry, nojson::JsonParseError> {
    let startup = value.to_member("startup")?.required()?.try_into()?;
    let metadata = value.to_member("metadata")?.required()?.try_into()?;
    let removed = value.to_member("removed")?.required()?.try_into()?;
    Ok(NodeRegistryEntry {
        metadata: NodeMetadata::new(startup, metadata),
        removed,
    })
}

fn format_node_registry(registry: &NodeRegistry) -> String {
    let mut text = nojson::json(|f| {
        f.set_indent_size(2);
        f.set_spacing(true);
        f.object(|f| {
            f.member("version", NODE_REGISTRY_VERSION)?;
            f.member("nodes", NodeRegistryNodesJson(registry))
        })
    })
    .to_string();
    text.push('\n');
    text
}

struct NodeRegistryNodesJson<'a>(&'a NodeRegistry);

impl nojson::DisplayJson for NodeRegistryNodesJson<'_> {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            for (node_id, entry) in &self.0.nodes {
                f.member(node_id.get(), NodeRegistryEntryJson(entry))?;
            }
            Ok(())
        })
    }
}

struct NodeRegistryEntryJson<'a>(&'a NodeRegistryEntry);

impl nojson::DisplayJson for NodeRegistryEntryJson<'_> {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        f.object(|f| {
            f.member("startup", self.0.metadata.startup)?;
            f.member("metadata", RawJsonText(&self.0.metadata.metadata))?;
            f.member("removed", self.0.removed)
        })
    }
}

struct RawJsonText<'a>(&'a nojson::RawJsonOwned);

impl nojson::DisplayJson for RawJsonText<'_> {
    fn fmt(&self, f: &mut nojson::JsonFormatter<'_, '_>) -> fmt::Result {
        write!(f.inner_mut(), "{}", self.0.text())
    }
}

fn should_sync_metadata(sync: SyncPolicy) -> bool {
    !matches!(sync, SyncPolicy::UnsafeNoSync)
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub(crate) fn node_not_found_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "node storage has not been created")
}

pub(crate) fn node_removed_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "node storage has been removed")
}

fn node_already_exists_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        "node storage has already been created",
    )
}
