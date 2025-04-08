#![doc = include_str!("../README.md")]

#[macro_use]
extern crate nestify;

use ordermap::OrderSet;
#[cfg(test)]
use serde::{Deserialize, Serialize};

use anyhow::{anyhow, bail, Result};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    rc::Rc,
};

use bon::Builder;
use digest::Digest;
#[cfg(feature = "md5")]
use md5::Md5;

/// A trait that defines the behavior of a hashing function used in the consistent hashing ring.
///
/// Implementors of this trait must provide a `digest` method that takes a string input
/// and returns a 64-bit hash value. This hash value is used to determine the placement
/// of keys and nodes in the consistent hashing ring.
pub trait RingHasherTrait {
    /// Computes a 64-bit hash value for the given input string.
    ///
    /// # Arguments
    /// - `data`: The input string to hash.
    ///
    /// # Returns
    /// - `Result<u64>`: The computed hash value or an error if the hashing fails.
    fn digest(&self, data: &str) -> Result<u64>;
}

#[derive(Debug, Default)]
pub enum RingHasher {
    #[default]
    Default_,
    #[cfg(feature = "md5")]
    Md5,
}

impl RingHasherTrait for RingHasher {
    fn digest(&self, data: &str) -> Result<u64> {
        match self {
            RingHasher::Default_ => {
                let mut hasher = DefaultHasher::new();
                hasher.write(data.as_bytes());

                Ok(hasher.finish())
            }
            #[cfg(feature = "md5")]
            RingHasher::Md5 => {
                let hash = Md5::digest(data.as_bytes());
                let slice = &hash[0..8];

                Ok(u64::from_be_bytes(slice.try_into().map_err(|err| {
                    anyhow!("Failed to convert MD5 hash to u64: {}", err)
                })?))
            }
        }
    }
}

type VirtualId = String;
type HashValue = u64;

nest! {
    /// Represents a physical node in the consistent hashing ring.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Builder)]*
    pub struct PhysicalNode {
        /// A unique identifier for the physical node.
        pub id: String,
        /// The number of virtual nodes associated with this physical node.
        pub num_vnodes: usize,
        /// A map of virtual node IDs to their corresponding `VirtualNode` structs.
        #[builder(default)]
        pub vnodes: BTreeMap<VirtualId, pub struct VirtualNode {
            /// The unique identifier for the virtual node.
            pub id: String,
            /// The hash value of the virtual node.
            pub hash: u64,
            /// A set of hash values associated with this virtual node.
            #[builder(default)]
            pub hashes: OrderSet<u64>,
        }>,
        /// A map of hash values to `Item` structs, representing the data stored on this node.
        pub data: BTreeMap<
            HashValue,
            #[cfg_attr(test, derive(Serialize, Deserialize))]
            pub struct Item {
                /// The key of the item.
                pub key: String,
                /// The value of the item.
                pub value: String,
            }
            >,
    }
}

impl Item {
    #[cfg(test)]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }
}

impl PhysicalNode {
    /// Initializes the virtual nodes for this physical node.
    ///
    /// This method generates virtual node IDs based on the physical node's ID and the number
    /// of virtual nodes (`num_vnodes`). It computes a hash for each virtual node ID using
    /// the provided hasher and creates a `VirtualNode` instance for each ID. The virtual
    /// nodes are then added to the `vnodes` map of the physical node.
    ///
    /// # Arguments
    /// - `hasher`: A reference to an object implementing the `RingHasherTrait` to compute hashes.
    ///
    /// # Returns
    /// - `Result<()>`: Returns `Ok(())` if the virtual nodes are successfully initialized,
    ///   or an error if hashing fails.
    pub fn init_vnodes<T: RingHasherTrait>(&mut self, hasher: &T) -> Result<()> {
        let ids = (0..self.num_vnodes).map(|i| format!("{}-{}", self.id, i));

        for id in ids {
            let hash = hasher.digest(&id)?;
            let vnode = VirtualNode::builder().id(id.clone()).hash(hash).build();
            self.vnodes.insert(id, vnode);
        }

        Ok(())
    }
}

/// Represents a consistent hashing ring.
///
/// This struct manages the consistent hashing ring, which is used to distribute data across multiple
/// physical nodes. It supports features such as replication, virtual nodes, and dynamic addition/removal
/// of nodes.
///
/// # Why `Rc<RefCell<>>` is used:
/// - `Rc` (Reference Counted) is used to allow multiple ownership of `PhysicalNode` instances. This is
///   necessary because multiple parts of the code may need to access the same physical node.
/// - `RefCell` is used to enable interior mutability, allowing the `PhysicalNode` to be mutated even
///   when it is wrapped in an `Rc`. This is important because there are cases that we need to borrow immutably and mutably in the same lifetime.
#[derive(Debug, Builder)]
#[builder(builder_type(doc {}))]
pub struct ConsistentHashingRing<T: RingHasherTrait> {
    /// The hashing function used to compute hash values for keys and nodes.
    pub hasher: T,
    /// The number of physical nodes each key is replicated to for fault tolerance.
    pub replication_factor: usize,
    /// A mapping of hash values to virtual node IDs.
    pub hash_to_vid: BTreeMap<HashValue, String>,
    /// A mapping of virtual node IDs to physical node IDs.
    pub vid_to_pid: HashMap<String, String>,
    /// A mapping of physical node IDs to their corresponding `PhysicalNode` instances.
    pub physicals: HashMap<String, Rc<RefCell<PhysicalNode>>>,
}

nest! {
    /// Represents the result of adding a physical node to the consistent hashing ring.
    ///
    /// This struct contains information about the virtual nodes created for the added physical node
    /// and the number of keys reassigned to each virtual node.
    #[derive(Debug, Default, Builder)]*
    pub struct AddNodeResult {
        /// A list of results for each virtual node created for the added physical node.
        pub values: Vec<pub struct AddNodeResultValue {
            /// The unique identifier of the virtual node.
            pub id: String,
            /// The hash value of the virtual node.
            pub hash: u64,
            /// The number of keys that were reassigned to this virtual node.
            pub keys_added: usize,
        }>,
    }
}

nest! {
    /// Represents the result of adding a key-value pair to the consistent hashing ring.
    ///
    /// This struct contains information about the key, its hash value, and the locations
    /// (virtual nodes and their corresponding physical nodes) where the key was added.
    #[derive(Debug, Builder)]*
    pub struct AddKeyResult {
        /// The key that was added to the ring.
        pub key: String,
        /// The hash value of the key.
        pub hash: u64,
        /// The locations where the key was added, represented as a list of virtual node and physical node pairs.
        #[builder(default)]
        pub locations: Vec<pub struct AddKeyResultLocation {
            /// The unique identifier of the virtual node.
            pub id: String,
            /// The unique identifier of the physical node.
            pub pid: String,
        }>,
    }
}

nest! {
    /// Represents information about hash ranges in the consistent hashing ring.
    ///
    /// This struct contains details about the ranges of hash values managed by the ring,
    /// including the number of keys and the items stored within each range.
    #[derive(Debug, Builder)]*
    #[cfg_attr(test, derive(Serialize, Deserialize))]*
    pub struct RangeInfo {
        /// A list of hash ranges managed by the ring.
        pub ranges: Vec<pub struct Range {
            /// The starting hash value of the range.
            pub hash_start: u64,
            /// The ending hash value of the range.
            pub hash_end: u64,
            /// The number of keys within the range.
            pub count: usize,
            /// The items stored within the range.
            pub items: Vec<pub struct HashItem {
                /// The hash value of the item.
                pub hash: u64,
                /// The actual item stored in the range.
                pub inner: Item,
            }>,
        }>,
    }
}

impl RangeInfo {
    pub fn key_count(&self) -> usize {
        self.ranges.iter().map(|r| r.count).sum()
    }

    #[cfg(test)]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    #[cfg(test)]
    pub fn to_json_string_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}

nest! {
    #[derive(Debug, Builder)]*
    #[cfg_attr(test, derive(Serialize, Deserialize))]*
    pub struct ItemsByVNode {
        #[builder(default)]
        pub values: Vec<pub struct ItemsByVNodeValue {
            pub vid: String,
            pub items: Vec<Item>,
        }>,
    }
}

impl ItemsByVNode {
    #[cfg(test)]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    #[cfg(test)]
    pub fn to_json_string_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}

impl<T: RingHasherTrait> ConsistentHashingRing<T> {
    pub fn vid_to_physical(&self, vid: &str) -> Result<Rc<RefCell<PhysicalNode>>> {
        let pid = self
            .vid_to_pid
            .get(vid)
            .ok_or_else(|| anyhow!("Physical node not found, vid: {}", vid))?;

        self.physicals
            .get(pid)
            .ok_or_else(|| anyhow!("Physical node not found, pid: {}", pid))
            .cloned()
    }

    /// Adds a physical node to the consistent hashing ring.
    ///
    /// This method initializes the virtual nodes for the given physical node, calculates their hash values,
    /// and inserts them into the ring. It also redistributes data from the previous virtual nodes to the new
    /// virtual nodes based on their hash ranges.
    ///
    /// # Steps
    /// 1. **Initialize Virtual Nodes**: Generate virtual node IDs for the physical node and compute their hash values.
    /// 2. **Insert Physical Node**: Add the physical node to the `physicals` map, wrapped in `Rc<RefCell>` for shared ownership.
    /// 3. **Redistribute Data**:
    ///    - For each virtual node, find the previous virtual node in the ring.
    ///    - Move data from the hash range `[prev_hash, curr_hash]` of the previous virtual node to the new virtual node.
    /// 4. **Update Mappings**:
    ///    - Update `hash_to_vid` to map the hash of the new virtual node to its ID.
    ///    - Update `vid_to_pid` to map the virtual node ID to the physical node ID.
    /// 5. **Track Results**: Record the number of keys reassigned to each virtual node in the `AddNodeResult`.
    ///
    /// # Arguments
    /// - `node`: The physical node to be added to the ring.
    ///
    /// # Returns
    /// - `Result<AddNodeResult>`: A result containing information about the virtual nodes created and the
    ///   number of keys reassigned to each virtual node, or an error if the operation fails.
    pub fn add_physical_node(&mut self, mut node: PhysicalNode) -> Result<AddNodeResult> {
        // Clone the physical node's ID for later use.
        let pid = node.id.clone();

        // Initialize virtual nodes for the physical node using the hasher.
        node.init_vnodes(&self.hasher)?;

        // Wrap the physical node in an Rc<RefCell> for shared ownership and mutability.
        let pnode = Rc::new(RefCell::new(node));
        self.physicals.insert(pid.clone(), pnode.clone());

        // Prepare the result object to track the operation's outcome.
        let mut result = AddNodeResult::default();

        // Iterate over the virtual nodes of the physical node.
        for vnode in pnode.borrow().vnodes.values() {
            let hash = vnode.hash;

            // Find the previous hash in the ring that is less than the current hash.
            let prev_hash = self.hash_to_vid.range(..hash).last();
            let mut keys_added = 0;

            // If a previous virtual node exists, move data from its range to the new virtual node.
            if let Some((prev_hash, prev_vid)) = prev_hash {
                let prev_node = self.vid_to_physical(prev_vid)?;

                // Collect keys within the range [prev_hash, hash].
                let keys_to_move = prev_node
                    .borrow()
                    .data
                    .range(prev_hash..=&hash)
                    .map(|(k, _)| *k)
                    .collect::<Vec<_>>();
                keys_added += keys_to_move.len();

                // Move the keys and their associated values to the new virtual node.
                for key in keys_to_move {
                    if let Some(value) = prev_node.borrow_mut().data.remove(&key) {
                        pnode.borrow_mut().data.insert(key, value);
                    }
                }
            }

            // Update the hash-to-virtual-node mapping.
            self.hash_to_vid.insert(hash, vnode.id.clone());

            // Update the virtual-node-to-physical-node mapping.
            self.vid_to_pid
                .insert(vnode.id.clone(), pnode.borrow().id.clone());

            // Add the virtual node's result to the operation's result object.
            result.values.push(
                AddNodeResultValue::builder()
                    .id(vnode.id.clone())
                    .hash(hash)
                    .keys_added(keys_added)
                    .build(),
            );
        }

        // Return the result of the operation.
        Ok(result)
    }

    /// Removes a physical node from the consistent hashing ring.
    ///
    /// This method removes all virtual nodes associated with the given physical node ID (`pid`).
    /// It redistributes the data stored in the virtual nodes of the removed physical node to the
    /// next virtual nodes in the ring that belong to different physical nodes.
    ///
    /// # Steps
    /// 1. Check if the `physicals` map is empty. If so, clear all mappings and return early.
    /// 2. Retrieve the virtual node IDs (`vids`) associated with the physical node.
    /// 3. Remove each virtual node using the `remove_vnode` method.
    /// 4. Remove the physical node from the `physicals` map.
    ///
    /// # Arguments
    /// - `pid`: The unique identifier of the physical node to be removed.
    ///
    /// # Returns
    /// - `Result<()>`: Returns `Ok(())` if the physical node is successfully removed, or an error
    ///   if the physical node does not exist or if any operation fails.
    pub fn remove_physical_node(&mut self, pid: &str) -> Result<()> {
        // If there are no physical nodes, clear all mappings and return early.
        if self.physicals.is_empty() {
            self.hash_to_vid.clear(); // Clear the hash-to-virtual-node mapping.
            self.vid_to_pid.clear(); // Clear the virtual-node-to-physical-node mapping.
            return Ok(());
        }

        // Retrieve the virtual node IDs (`vids`) associated with the physical node.
        let vids = {
            let pnode = self
                .physicals
                .get(pid)
                .ok_or_else(|| anyhow!("Physical node not found, pid: {}", pid))? // Error if the physical node does not exist.
                .clone();
            let keys = pnode.borrow().vnodes.keys().cloned().collect::<Vec<_>>(); // Collect virtual node IDs.
            keys
        };

        // Remove each virtual node using the `remove_vnode` method.
        vids.iter().try_for_each(|vid| self.remove_vnode(vid))?;

        // Remove the physical node from the `physicals` map.
        self.physicals.remove(pid);

        Ok(())
    }

    /// Inserts a key-value pair into the consistent hashing ring.
    ///
    /// This method computes the hash of the given key, determines the virtual nodes
    /// where the key should be stored based on the replication factor, and inserts
    /// the key-value pair into the appropriate physical nodes.
    ///
    /// # Steps
    /// 1. Compute the hash of the key using the hasher.
    /// 2. Determine the virtual nodes (`vids`) where the key should be stored.
    /// 3. For each virtual node:
    ///    - Check if the physical node has already been used for replication.
    ///    - Insert the key-value pair into the virtual node and its corresponding physical node.
    ///    - Track the virtual node and physical node in the result.
    /// 4. Stop once the replication factor is satisfied.
    ///
    /// # Arguments
    /// - `key`: The key to be inserted into the ring.
    /// - `value`: The value associated with the key.
    ///
    /// # Returns
    /// - `Result<AddKeyResult>`: A result containing information about the virtual nodes
    ///   where the key was stored, or an error if the operation fails.
    pub fn insert(&mut self, key: &str, value: &str) -> Result<AddKeyResult> {
        // Compute the hash of the key.
        let hash = self.hasher.digest(key)?;

        // Prepare the result object to track the operation's outcome.
        let mut result = AddKeyResult::builder()
            .key(key.to_string())
            .hash(hash)
            .build();

        // Track physical nodes to ensure no duplicate replication.
        let mut pids = HashSet::new();

        // Determine the virtual nodes (`vids`) where the key should be stored.
        let vids: Vec<String> = self
            .hash_to_vid
            .range(hash..) // Start from the hash and wrap around the ring.
            .chain(self.hash_to_vid.range(..)) // Handle wrap-around.
            .map(|(_, vid)| vid.clone())
            .collect();

        // Iterate over the virtual nodes to insert the key-value pair.
        for vid in vids {
            // Get the physical node corresponding to the virtual node.
            let pnode = self.vid_to_physical(&vid)?;
            let pid = pnode.borrow().id.clone();

            // Get a mutable reference to the physical node.
            let mut pnode_refmut = pnode.borrow_mut();

            // Get the virtual node from the physical node.
            let vnode = pnode_refmut
                .vnodes
                .get_mut(&vid)
                .ok_or_else(|| anyhow!("Virtual node not found, vid: {}", vid))?;

            // Skip if this physical node has already been used for replication.
            if pids.contains(&pid) {
                continue;
            }

            // Insert the hash into the virtual node's hash set.
            vnode.hashes.insert(hash);

            // Insert the key-value pair into the physical node's data map.
            pnode_refmut.data.insert(
                hash,
                Item::builder()
                    .key(key.to_string())
                    .value(value.to_string())
                    .build(),
            );

            // Track the physical node to avoid duplicate replication.
            pids.insert(pid.clone());

            // Add the virtual node and physical node to the result.
            result.locations.push(
                AddKeyResultLocation::builder()
                    .id(vid.clone())
                    .pid(pid.clone())
                    .build(),
            );

            // Stop once the replication factor is satisfied.
            if pids.len() == self.replication_factor {
                break;
            }
        }

        // Return the result of the operation.
        Ok(result)
    }

    /// Inserts multiple key-value pairs into the consistent hashing ring.
    ///
    /// This method iterates over a list of items, computes their hashes, and inserts
    /// each key-value pair into the appropriate virtual nodes in the ring.
    ///
    /// # Steps
    /// 1. Iterate over the list of items.
    /// 2. For each item, call the `insert` method to add the key-value pair to the ring.
    /// 3. Collect the results of each insertion into a vector.
    ///
    /// # Arguments
    /// - `items`: A slice of `Item` structs, each containing a key and value to be inserted.
    ///
    /// # Returns
    /// - `Result<Vec<AddKeyResult>>`: A vector of results for each inserted item, or an error
    ///   if any insertion fails.
    pub fn insert_many(&mut self, items: &[Item]) -> Result<Vec<AddKeyResult>> {
        // Initialize a vector to store the results of each insertion.
        let mut results = Vec::new();

        // Iterate over each item and insert it into the ring.
        for item in items {
            // Insert the key-value pair into the ring and collect the result.
            let result = self.insert(&item.key, &item.value)?;
            results.push(result);
        }

        // Return the results of all insertions.
        Ok(results)
    }

    /// Retrieves an item from the consistent hashing ring by its key.
    ///
    /// This method computes the hash of the given key, determines the virtual nodes
    /// where the key might be stored, and retrieves the associated value if it exists.
    ///
    /// # Steps
    /// 1. Compute the hash of the key using the hasher.
    /// 2. Iterate over the virtual nodes (`vids`) where the key might be stored.
    /// 3. Check each physical node's data map for the key.
    /// 4. Return the item if found, or an error if the key does not exist.
    ///
    /// # Arguments
    /// - `key`: The key to retrieve from the ring.
    ///
    /// # Returns
    /// - `Result<Item>`: The item associated with the key, or an error if the key is not found.
    pub fn get_item(&self, key: &str) -> Result<Item> {
        // Compute the hash of the key.
        let hash = self.hasher.digest(key)?;

        // Determine the virtual nodes (`vids`) where the key might be stored.
        let vids: Vec<String> = self
            .hash_to_vid
            .range(hash..) // Start from the hash and wrap around the ring.
            .chain(self.hash_to_vid.range(..)) // Handle wrap-around.
            .map(|(_, vid)| vid.clone())
            .collect();

        // Iterate over the virtual nodes to find the key.
        for vid in vids {
            let pnode = self.vid_to_physical(&vid)?;
            let pnode_ref = pnode.borrow();

            // Check if the key exists in the physical node's data map.
            if let Some(item) = pnode_ref.data.get(&hash) {
                return Ok(item.clone());
            }
        }

        // Return an error if the key is not found.
        bail!("Item not found for key: {}", key)
    }

    /// Removes a key-value pair from the consistent hashing ring.
    ///
    /// This method computes the hash of the given key, iterates over the virtual nodes
    /// in both clockwise and anti-clockwise directions, and removes the key-value pair
    /// from the appropriate physical nodes. The removal stops once the replication factor
    /// is satisfied.
    ///
    /// # Steps
    /// 1. Compute the hash of the key using the hasher.
    /// 2. Iterate over the virtual nodes in both clockwise and anti-clockwise directions.
    /// 3. For each virtual node:
    ///    - Check if the key exists in the virtual node's hash set.
    ///    - Remove the key-value pair from the physical node's data map.
    ///    - Track the number of successful removals.
    /// 4. Stop once the replication factor is satisfied.
    /// 5. Return an error if the key is not found or if the replication factor is not met.
    ///
    /// # Arguments
    /// - `key`: The key to be removed from the ring.
    ///
    /// # Returns
    /// - `Result<()>`: Returns `Ok(())` if the key is successfully removed, or an error
    ///   if the key does not exist or if the replication factor is not met.
    pub fn remove(&mut self, key: &str) -> Result<()> {
        // Compute the hash of the key.
        let hash = self.hasher.digest(key)?;
        let mut remove_count = 0;

        // Define iterators for clockwise and anti-clockwise traversal of the ring.
        let ring_iters = [
            self.hash_to_vid
                .range(hash..) // Clockwise: Start from the hash and wrap around.
                .chain(self.hash_to_vid.range(..)),
            self.hash_to_vid
                .range(..hash) // Anti-clockwise: Start before the hash and wrap around.
                .chain(self.hash_to_vid.range(hash..)),
        ];

        // Iterate over both clockwise and anti-clockwise directions.
        for it in ring_iters {
            for (_, vid) in it {
                // Retrieve the physical node associated with the virtual node.
                let pnode = self.vid_to_physical(vid)?;
                let mut pnode_refmut = pnode.borrow_mut();

                // Retrieve the virtual node from the physical node.
                let vnode = pnode_refmut
                    .vnodes
                    .get_mut(vid)
                    .ok_or_else(|| anyhow!("Virtual node not found, vid: {}", vid))?;

                // Check if the key exists in the virtual node's hash set.
                let removed = vnode.hashes.remove(&hash);

                if removed {
                    // Remove the key-value pair from the physical node's data map.
                    if pnode_refmut.data.remove(&hash).is_some() {
                        remove_count += 1;
                    }

                    // Stop once the replication factor is satisfied.
                    if remove_count == self.replication_factor {
                        return Ok(());
                    }
                } else {
                    // If a replica wasn't found, stop checking further in this direction.
                    break;
                }
            }
        }

        // If no replicas were removed, return an error indicating the key was not found.
        if remove_count == 0 {
            bail!("Item not found for key: {}", key);
        }

        // If the replication factor is not met, return an error.
        bail!(
            "Remove count {} is less than replication factor {}",
            remove_count,
            self.replication_factor
        )
    }

    /// Retrieves the physical node IDs (`pids`) that contain a specific key.
    ///
    /// This method computes the hash of the given key, determines the virtual nodes
    /// where the key might be stored, and collects the physical node IDs that store the key.
    ///
    /// # Steps
    /// 1. Compute the hash of the key using the hasher.
    /// 2. Iterate over the virtual nodes (`vids`) where the key might be stored.
    /// 3. Check each physical node's data map for the key.
    /// 4. Collect the physical node IDs that contain the key.
    ///
    /// # Arguments
    /// - `key`: The key to locate in the ring.
    ///
    /// # Returns
    /// - `Result<Vec<String>>`: A vector of physical node IDs that contain the key, or an error
    ///   if the operation fails.
    pub fn get_pids_containing_key(&self, key: &str) -> Result<Vec<String>> {
        // Compute the hash of the key.
        let hash = self.hasher.digest(key)?;

        // Determine the virtual nodes (`vids`) where the key might be stored.
        let vids: Vec<String> = self
            .hash_to_vid
            .range(hash..) // Start from the hash and wrap around the ring.
            .chain(self.hash_to_vid.range(..)) // Handle wrap-around.
            .map(|(_, vid)| vid.clone())
            .collect();

        // Use a set to track unique physical node IDs.
        let mut pids = HashSet::new();

        // Iterate over the virtual nodes to find the key.
        for vid in vids {
            let pnode = self.vid_to_physical(&vid)?;
            let pid = pnode.borrow().id.clone();

            // Skip if the physical node has already been checked.
            if pids.contains(&pid) {
                continue;
            }

            // Check if the key exists in the physical node's data map.
            if pnode.borrow().data.contains_key(&hash) {
                pids.insert(pid);
            }
        }

        // Return the collected physical node IDs as a vector.
        Ok(pids.into_iter().collect())
    }

    /// Removes a virtual node from the consistent hashing ring.
    ///
    /// This method removes the specified virtual node (`vid`) from the ring. It redistributes
    /// the data stored in the virtual node to the next virtual node in the ring that belongs
    /// to a different physical node and does not already contain the same data (due to replication).
    ///
    /// # Steps
    /// 1. Retrieve the physical node associated with the virtual node.
    /// 2. Remove the virtual node from the physical node's `vnodes` map.
    /// 3. Redistribute the data stored in the virtual node to the next virtual node in the ring.
    /// 4. Update the `hash_to_vid` and `vid_to_pid` mappings to reflect the removal.
    ///
    /// # Arguments
    /// - `vid`: The unique identifier of the virtual node to be removed.
    ///
    /// # Returns
    /// - `Result<()>`: Returns `Ok(())` if the virtual node is successfully removed, or an error
    ///   if the virtual node does not exist or if any operation fails.
    pub fn remove_vnode(&mut self, vid: &str) -> Result<()> {
        // Retrieve the physical node associated with the virtual node.
        let pnode = self.vid_to_physical(vid)?;
        let pid = pnode.borrow().id.clone();

        // Remove the virtual node from the physical node's `vnodes` map.
        let vnode = {
            let mut pnode_refmut = pnode.borrow_mut();
            pnode_refmut
                .vnodes
                .remove(vid)
                .ok_or_else(|| anyhow!("Virtual node not found, vid: {}", vid))?
        };

        // Redistribute the data stored in the virtual node.
        for hash in vnode.hashes.iter() {
            // Find the next virtual node in the ring that belongs to a different physical node
            // and does not already contain the same hash (due to replication).
            let (_, next_vid) = self
                .hash_to_vid
                .range(vnode.hash + 1..) // Search for the next hash in the ring.
                .find(|(_, vid)| {
                    let next_pnode = self.vid_to_physical(vid).unwrap();
                    let next_pid = next_pnode.borrow().id.clone();

                    // Ensure the next virtual node belongs to a different physical node
                    // and does not already contain the same hash.
                    next_pid != pid && !next_pnode.borrow().data.contains_key(hash)
                })
                .ok_or_else(|| anyhow!("Next vnode not found for vid: {}", vid))?;

            // Retrieve the next physical node and move the data to it.
            let next_pnode = self.vid_to_physical(next_vid)?;
            let mut next_pnode_refmut = next_pnode.borrow_mut();

            if let Some(key) = pnode.borrow_mut().data.remove(hash) {
                next_pnode_refmut.data.insert(*hash, key);
            }

            // Update the next virtual node's hash set to include the redistributed hashes.
            next_pnode_refmut
                .vnodes
                .get_mut(next_vid)
                .ok_or_else(|| anyhow!("Next vnode not found for vid: {}", next_vid))?
                .hashes
                .extend(vnode.hashes.iter());
        }

        // Remove the virtual node from the mappings.
        self.hash_to_vid.remove(&vnode.hash);
        self.vid_to_pid.remove(vid);

        Ok(())
    }

    /// Adds a single virtual node to a physical node in the consistent hashing ring.
    ///
    /// This method creates a new virtual node for the specified physical node (`pid`),
    /// computes its hash, and redistributes data from the next virtual node in the ring
    /// to the newly added virtual node.
    ///
    /// # Steps
    /// 1. Retrieve the physical node associated with the given `pid`.
    /// 2. Generate a unique virtual node ID (`vid`) and compute its hash.
    /// 3. Ensure the hash is unique and does not collide with existing virtual nodes.
    /// 4. Redistribute data from the next virtual node in the ring to the new virtual node.
    /// 5. Update the `hash_to_vid` and `vid_to_pid` mappings.
    /// 6. Add the new virtual node to the physical node's `vnodes` map.
    ///
    /// # Arguments
    /// - `pid`: The unique identifier of the physical node to which the virtual node will be added.
    ///
    /// # Returns
    /// - `Result<()>`: Returns `Ok(())` if the virtual node is successfully added, or an error
    ///   if any operation fails.
    pub fn add_one_vnode(&mut self, pid: &str) -> Result<()> {
        // Retrieve the physical node associated with the given `pid`.
        let pnode = self
            .physicals
            .get(pid)
            .ok_or_else(|| anyhow!("Physical node not found, pid: {}", pid))?
            .clone();
        let pid = pnode.borrow().id.clone();

        // Generate a unique virtual node ID (`vid`) and compute its hash.
        let vid = format!("{}-{}", pid, pnode.borrow().num_vnodes);
        let hash = self.hasher.digest(&vid)?;

        // Ensure the hash is unique and does not collide with existing virtual nodes.
        if self.hash_to_vid.contains_key(&hash) {
            bail!("Virtual node with hash {} already exists", hash);
        }

        // Create the new virtual node.
        let mut vnode = VirtualNode::builder().id(vid.clone()).hash(hash).build();

        // Find the next virtual node in the ring.
        let (next_hash, next_vid) = self
            .hash_to_vid
            .range(hash + 1..) // Search for the next hash in the ring.
            .chain(self.hash_to_vid.range(..)) // Handle wrap-around.
            .next()
            .ok_or_else(|| anyhow!("Next vnode not found for vid: {}", vid))?;

        // Retrieve the next physical node and its virtual node.
        let next_pnode = self.vid_to_physical(next_vid)?;
        let next_pid = next_pnode.borrow().id.clone();
        let hashes_to_move = next_pnode
            .borrow()
            .vnodes
            .get(next_vid)
            .ok_or_else(|| anyhow!("Next vnode not found for vid: {}", next_vid))?
            .hashes
            .iter()
            .filter(|candidate| {
                if &hash < next_hash {
                    **candidate < hash
                } else {
                    // Handle wrap-around.
                    **candidate < hash && *candidate > next_hash
                }
            })
            .cloned()
            .collect::<Vec<_>>();

        // Add the hashes to the new virtual node.
        vnode.hashes.extend(hashes_to_move.iter());

        // Update the next virtual node's hash set to remove the redistributed hashes.
        {
            let mut next_pnode_refmut = next_pnode.borrow_mut();
            let next_vnode = next_pnode_refmut
                .vnodes
                .get_mut(next_vid)
                .ok_or_else(|| anyhow!("Next vnode not found for vid: {}", next_vid))?;
            for hash in hashes_to_move.iter() {
                next_vnode.hashes.remove(hash);
            }
        }

        // Move data from the next virtual node to the new virtual node.
        for hash in hashes_to_move.iter() {
            if pid != next_pid {
                if let Some(value) = next_pnode.borrow_mut().data.remove(hash) {
                    pnode.borrow_mut().data.insert(*hash, value);
                }
            }
        }

        // Update the `hash_to_vid` and `vid_to_pid` mappings.
        self.hash_to_vid.insert(hash, vid.clone());
        self.vid_to_pid.insert(vid.clone(), pid.clone());

        // Add the new virtual node to the physical node's `vnodes` map.
        pnode.borrow_mut().vnodes.insert(vid.clone(), vnode);
        pnode.borrow_mut().num_vnodes += 1;

        Ok(())
    }

    /// Adjusts the number of virtual nodes for a physical node in the consistent hashing ring.
    ///
    /// This method increases or decreases the number of virtual nodes for the specified physical node (`pid`).
    /// If the new number of virtual nodes is zero, the physical node is removed from the ring.
    ///
    /// # Steps
    /// 1. Retrieve the physical node associated with the given `pid`.
    /// 2. If `new_num_vnodes` is zero, remove the physical node.
    /// 3. If `new_num_vnodes` is less than the current number, remove the extra virtual nodes.
    /// 4. If `new_num_vnodes` is greater than the current number, add new virtual nodes.
    ///
    /// # Arguments
    /// - `pid`: The unique identifier of the physical node.
    /// - `new_num_vnodes`: The desired number of virtual nodes for the physical node.
    ///
    /// # Returns
    /// - `Result<()>`: Returns `Ok(())` if the operation is successful, or an error if any operation fails.
    pub fn set_num_vnodes(&mut self, pid: &str, new_num_vnodes: usize) -> Result<()> {
        // Retrieve the physical node associated with the given `pid`.
        let pnode = self
            .physicals
            .get(pid)
            .ok_or_else(|| anyhow!("Physical node not found, pid: {}", pid))?
            .clone();
        let curr_num_vnodes = pnode.borrow().num_vnodes;

        // If the number of virtual nodes is unchanged, return early.
        if new_num_vnodes == curr_num_vnodes {
            return Ok(());
        }

        // If the new number of virtual nodes is zero, remove the physical node.
        if new_num_vnodes == 0 {
            self.remove_physical_node(pid)?;
            return Ok(());
        }

        // If the new number of virtual nodes is less, remove the extra virtual nodes.
        if new_num_vnodes < curr_num_vnodes {
            for i in new_num_vnodes..curr_num_vnodes {
                let vid = format!("{}-{}", pid, i);
                self.remove_vnode(&vid)?; // Remove the virtual node.
            }
        } else {
            // If the new number of virtual nodes is greater, add new virtual nodes.
            for _ in curr_num_vnodes..new_num_vnodes {
                self.add_one_vnode(pid)?; // Add a new virtual node.
            }
        }

        Ok(())
    }

    /// Retrieves information about the hash ranges in the consistent hashing ring.
    ///
    /// This method computes the hash ranges managed by the ring, including the number of keys
    /// and the items stored within each range. It provides a detailed view of how data is distributed
    /// across the virtual nodes in the ring.
    ///
    /// # Steps
    /// 1. Collect all hash values from the `hash_to_vid` mapping.
    /// 2. Handle the case where there are no hashes (empty ring).
    /// 3. Handle the case where there is only one hash (single virtual node).
    /// 4. Iterate over the hash values to compute the ranges between consecutive hashes.
    /// 5. For each range, collect the items stored in the corresponding virtual node.
    ///
    /// # Returns
    /// - `Result<RangeInfo>`: A `RangeInfo` struct containing details about the hash ranges,
    ///   including the start and end hashes, the number of keys, and the items in each range.
    pub fn range_info(&self) -> Result<RangeInfo> {
        // Initialize an empty `RangeInfo` result.
        let mut result = RangeInfo::builder().ranges(vec![]).build();

        // Collect all hash values from the `hash_to_vid` mapping.
        let hashes = self.hash_to_vid.keys().cloned().collect::<Vec<_>>();

        // Handle the case where there are no hashes (empty ring).
        if hashes.is_empty() {
            return Ok(result);
        }

        // Handle the case where there is only one hash (single virtual node).
        if hashes.len() == 1 {
            let vid = self.hash_to_vid.get(&hashes[0]).unwrap();
            let node = self.vid_to_physical(vid)?;
            let items = node.borrow().data.values().cloned().collect::<Vec<_>>();

            // Add the single range to the result.
            result.ranges.push(
                Range::builder()
                    .hash_start(hashes[0])
                    .hash_end(hashes[0])
                    .count(items.len())
                    .items(
                        items
                            .iter()
                            .map(|item| {
                                HashItem::builder()
                                    .hash(hashes[0])
                                    .inner(item.clone())
                                    .build()
                            })
                            .collect(),
                    )
                    .build(),
            );

            return Ok(result);
        }

        // Iterate over the hash values to compute the ranges between consecutive hashes.
        for i in 0..hashes.len() {
            let start_hash = hashes[i];
            let end_hash = hashes[(i + 1) % hashes.len()]; // Wrap around for the last range.

            // Retrieve the virtual node corresponding to the end hash.
            let end_vid = self
                .hash_to_vid
                .get(&end_hash)
                .ok_or_else(|| anyhow!("Virtual ID not found for hash: {}", end_hash))?;
            let end_pnode = self.vid_to_physical(end_vid)?;
            let end_pnode_ref = end_pnode.borrow();
            let end_vnode = end_pnode_ref
                .vnodes
                .get(end_vid)
                .ok_or_else(|| anyhow!("Virtual node not found for vid: {}", end_vid))?;

            // Collect the items stored in the virtual node for the current range.
            let items = end_vnode
                .hashes
                .iter()
                .map(|hash| {
                    let value = end_pnode_ref
                        .data
                        .get(hash)
                        .ok_or_else(|| anyhow!("Key not found for hash: {}", hash))?
                        .clone();

                    Ok(HashItem::builder().hash(*hash).inner(value).build())
                })
                .collect::<Result<Vec<_>>>()?;

            // Add the range to the result.
            result.ranges.push(
                Range::builder()
                    .hash_start(start_hash)
                    .hash_end(end_hash)
                    .count(end_vnode.hashes.len())
                    .items(items)
                    .build(),
            );
        }

        // Return the computed `RangeInfo`.
        Ok(result)
    }

    /// Retrieves the total number of keys stored in the consistent hashing ring.
    ///
    /// This method iterates over all physical nodes in the ring and sums up the number of keys
    /// stored in their respective data maps.
    ///
    /// # Returns
    /// - `Result<usize>`: The total number of keys in the ring, or an error if any operation fails.
    pub fn key_count(&self) -> Result<usize> {
        // Sum up the number of keys in the data maps of all physical nodes.
        Ok(self
            .physicals
            .values()
            .map(|node| node.borrow().data.len()) // Get the number of keys in each physical node.
            .sum()) // Sum up the counts.
    }

    /// Retrieves the items stored in each virtual node of the consistent hashing ring.
    ///
    /// This method iterates over all virtual nodes in the ring and collects the items stored
    /// in each virtual node. It provides a detailed view of how data is distributed across
    /// the virtual nodes.
    ///
    /// # Returns
    /// - `Result<ItemsByVNode>`: An `ItemsByVNode` struct containing the items stored in each virtual node,
    ///   or an error if any operation fails.
    pub fn items_by_vnode(&self) -> Result<ItemsByVNode> {
        // Initialize an empty `ItemsByVNode` result.
        let mut result = ItemsByVNode::builder().build();

        // Handle the case where the ring is empty.
        if self.hash_to_vid.is_empty() {
            return Ok(result);
        }

        // Handle the case where there is only one virtual node.
        if self.hash_to_vid.len() == 1 {
            let (_, vid) = self.hash_to_vid.iter().next().unwrap(); // Get the single virtual node ID.
            let node = self.vid_to_physical(vid)?; // Retrieve the physical node associated with the virtual node.
            let items = node.borrow().data.values().cloned().collect::<Vec<_>>(); // Collect all items in the physical node.

            // Add the items to the result.
            result.values.push(
                ItemsByVNodeValue::builder()
                    .vid(vid.clone())
                    .items(items)
                    .build(),
            );

            return Ok(result);
        }

        // Iterate over all virtual nodes in the ring.
        for (_, vid) in self.hash_to_vid.iter() {
            let pnode = self.vid_to_physical(vid)?; // Retrieve the physical node associated with the virtual node.
            let pnode_ref = pnode.borrow();
            let vnode = pnode_ref.vnodes.get(vid).unwrap(); // Get the virtual node.

            // Collect the items stored in the virtual node.
            let items = vnode
                .hashes
                .iter()
                .map(|hash| {
                    pnode_ref
                        .data
                        .get(hash) // Retrieve the item associated with the hash.
                        .cloned()
                        .ok_or_else(|| anyhow!("Key not found for hash: {}", hash))
                })
                .collect::<Result<Vec<_>>>()?;

            // Add the items to the result.
            result.values.push(
                ItemsByVNodeValue::builder()
                    .vid(vid.clone())
                    .items(items)
                    .build(),
            );
        }

        // Return the result containing items grouped by virtual nodes.
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sugars::{btmap, hmap};

    struct TestHasher;
    impl RingHasherTrait for TestHasher {
        fn digest(&self, data: &str) -> Result<u64> {
            //     5   | key1
            //     6   | key2
            //    10   | node2-0
            //    15   | key3
            //    20   | node3-0
            //    25   | key4
            //    30   | node1-0
            //    40   | node3-1
            //    50   | node2-1
            //    60   | node3-2
            //    65   | key5
            //    70   | node1-1
            match data {
                "node1-0" => Ok(30),
                "node1-1" => Ok(70),
                "node1-2" => Ok(26),
                "node2-0" => Ok(10),
                "node2-1" => Ok(50),
                "node3-0" => Ok(20),
                "node3-1" => Ok(40),
                "node3-2" => Ok(60),
                "key1" => Ok(5),
                "key2" => Ok(6),
                "key3" => Ok(15),
                "key4" => Ok(25),
                "key5" => Ok(65),
                _ => {
                    bail!("Unimplemented test hasher for data: {}", data)
                }
            }
        }
    }

    fn ring() -> ConsistentHashingRing<TestHasher> {
        ConsistentHashingRing::builder()
            .hasher(TestHasher)
            .replication_factor(2)
            .hash_to_vid(BTreeMap::new())
            .vid_to_pid(HashMap::new())
            .physicals(HashMap::new())
            .build()
    }

    fn nodes() -> Vec<PhysicalNode> {
        vec![
            PhysicalNode::builder()
                .id("node1".to_string())
                .num_vnodes(1)
                .data(BTreeMap::new())
                .build(),
            PhysicalNode::builder()
                .id("node2".to_string())
                .num_vnodes(2)
                .data(BTreeMap::new())
                .build(),
            PhysicalNode::builder()
                .id("node3".to_string())
                .num_vnodes(3)
                .data(BTreeMap::new())
                .build(),
        ]
    }

    fn items() -> Vec<Item> {
        serde_json::from_value(json!([
            { "key": "key1", "value": "value1" },
            { "key": "key2", "value": "value2" },
            { "key": "key3", "value": "value3" },
            { "key": "key4", "value": "value4" },
            { "key": "key5", "value": "value5" }
        ]))
        .unwrap()
    }

    #[test]
    fn should_successfully_add_physical_nodes() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        assert_eq!(ring.physicals.len(), 3);
        assert_eq!(ring.hash_to_vid.len(), 6);
        assert_eq!(ring.vid_to_pid.len(), 6);

        let expected_vnode_counts = vec![("node1", 1), ("node2", 2), ("node3", 3)];
        for (node_id, vnode_count) in expected_vnode_counts {
            let node = ring.physicals.get(node_id).unwrap().borrow();
            assert_eq!(node.vnodes.len(), vnode_count);
            assert_eq!(node.data.len(), 0);

            for (i, (_, vnode)) in node.vnodes.iter().enumerate() {
                assert_eq!(vnode.id, format!("{}-{}", node_id, i));
            }
        }

        assert_eq!(
            ring.hash_to_vid,
            btmap! {
                30 => "node1-0".to_string(),
                10 => "node2-0".to_string(),
                50 => "node2-1".to_string(),
                20 => "node3-0".to_string(),
                40 => "node3-1".to_string(),
                60 => "node3-2".to_string(),
            }
        );

        assert_eq!(
            ring.vid_to_pid,
            hmap! {
                "node1-0".to_string() => "node1".to_string(),
                "node2-0".to_string() => "node2".to_string(),
                "node2-1".to_string() => "node2".to_string(),
                "node3-0".to_string() => "node3".to_string(),
                "node3-1".to_string() => "node3".to_string(),
                "node3-2".to_string() => "node3".to_string(),
            }
        );
    }

    #[test]
    fn should_successfully_add_keys() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        assert!(ring.key_count().unwrap() == 10);
        let items_by_vnode = ring.items_by_vnode().unwrap();

        assert_eq!(
            items_by_vnode.to_json(),
            json!({
              "values": [
                {
                  "vid": "node2-0",
                  "items": [
                    { "key": "key1", "value": "value1" },
                    { "key": "key2", "value": "value2" },
                    { "key": "key5", "value": "value5" }
                  ]
                },
                {
                  "vid": "node3-0",
                  "items": [
                    { "key": "key1", "value": "value1" },
                    { "key": "key2", "value": "value2" },
                    { "key": "key3", "value": "value3" },
                    { "key": "key5", "value": "value5" }
                  ]
                },
                {
                  "vid": "node1-0",
                  "items": [
                    { "key": "key3", "value": "value3" },
                    { "key": "key4", "value": "value4" }
                  ]
                },
                {
                  "vid": "node3-1",
                  "items": [
                    { "key": "key4", "value": "value4" }
                  ]
                },
                {
                  "vid": "node2-1",
                  "items": []
                },
                {
                  "vid": "node3-2",
                  "items": []
                }
              ]
            })
        );
    }

    #[test]
    fn should_successfully_remove_key() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.remove("key1").unwrap();
        assert_eq!(ring.key_count().unwrap(), 8);

        let range_info = ring.range_info().unwrap();
        assert_eq!(range_info.key_count(), 8);
        assert_eq!(
            range_info.to_json(),
            json!({
                "ranges": [
                  {
                    "hash_start": 10,
                    "hash_end": 20,
                    "count": 3,
                    "items": [
                      {
                        "hash": 6,
                        "inner": {
                          "key": "key2",
                          "value": "value2"
                        }
                      },
                      {
                        "hash": 15,
                        "inner": {
                          "key": "key3",
                          "value": "value3"
                        }
                      },
                      {
                        "hash": 65,
                        "inner": {
                          "key": "key5",
                          "value": "value5"
                        }
                      }
                    ]
                  },
                  {
                    "hash_start": 20,
                    "hash_end": 30,
                    "count": 2,
                    "items": [
                      {
                        "hash": 15,
                        "inner": {
                          "key": "key3",
                          "value": "value3"
                        }
                      },
                      {
                        "hash": 25,
                        "inner": {
                          "key": "key4",
                          "value": "value4"
                        }
                      }
                    ]
                  },
                  {
                    "hash_start": 30,
                    "hash_end": 40,
                    "count": 1,
                    "items": [
                      {
                        "hash": 25,
                        "inner": {
                          "key": "key4",
                          "value": "value4"
                        }
                      }
                    ]
                  },
                  {
                    "hash_start": 40,
                    "hash_end": 50,
                    "count": 0,
                    "items": []
                  },
                  {
                    "hash_start": 50,
                    "hash_end": 60,
                    "count": 0,
                    "items": []
                  },
                  {
                    "hash_start": 60,
                    "hash_end": 10,
                    "count": 2,
                    "items": [
                      {
                        "hash": 6,
                        "inner": {
                          "key": "key2",
                          "value": "value2"
                        }
                      },
                      {
                        "hash": 65,
                        "inner": {
                          "key": "key5",
                          "value": "value5"
                        }
                      }
                    ]
                  }
                ]
              }
            )
        )
    }

    #[test]
    fn should_successfully_remove_vnode() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.remove_vnode("node2-0").unwrap();
        assert_eq!(ring.physicals.len(), 3);
        assert_eq!(ring.key_count().unwrap(), 10);

        let range_info = ring.range_info().unwrap();
        assert_eq!(range_info.key_count(), 10);

        assert_eq!(
            range_info.to_json(),
            json!({
              "ranges": [
                {
                  "hash_start": 20,
                  "hash_end": 30,
                  "count": 5,
                  "items": [
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } },
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 30,
                  "hash_end": 40,
                  "count": 1,
                  "items": [
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 40,
                  "hash_end": 50,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 50,
                  "hash_end": 60,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 60,
                  "hash_end": 20,
                  "count": 4,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                }
              ]
            })
        )
    }

    #[test]
    fn should_successfully_remove_vnode_of_physical_node_with_one_vnode() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();
        ring.remove_vnode("node1-0").unwrap();

        assert_eq!(ring.physicals.len(), 3);
        assert_eq!(ring.key_count().unwrap(), 10);

        let count = ring
            .physicals
            .get("node1")
            .unwrap()
            .clone()
            .borrow()
            .vnodes
            .len();
        assert_eq!(count, 0)
    }

    #[test]
    fn should_successfully_remove_physical_node_with_one_vnode() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.remove_physical_node("node1").unwrap();
        assert_eq!(ring.physicals.len(), 2);

        let range_info = ring.range_info().unwrap();
        assert_eq!(ring.key_count().unwrap(), 10);
        assert_eq!(range_info.key_count(), 10);

        assert_eq!(
            range_info.to_json(),
            json!({
              "ranges": [
                {
                  "hash_start": 10,
                  "hash_end": 20,
                  "count": 4,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 20,
                  "hash_end": 40,
                  "count": 1,
                  "items": [
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 40,
                  "hash_end": 50,
                  "count": 2,
                  "items": [
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 50,
                  "hash_end": 60,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 60,
                  "hash_end": 10,
                  "count": 3,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                }
              ]
            })
        );
    }

    #[test]
    fn should_successfully_remove_physical_node_with_multiple_vnodes() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.remove_physical_node("node2").unwrap();
        assert_eq!(ring.physicals.len(), 2);

        assert_eq!(ring.key_count().unwrap(), 10);

        let range_info = ring.range_info().unwrap();
        assert_eq!(range_info.key_count(), 10);

        assert_eq!(
            range_info.to_json(),
            json!({
              "ranges": [
                {
                  "hash_start": 20,
                  "hash_end": 30,
                  "count": 5,
                  "items": [
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } },
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 30,
                  "hash_end": 40,
                  "count": 1,
                  "items": [
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 40,
                  "hash_end": 60,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 60,
                  "hash_end": 20,
                  "count": 4,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                }
              ]
            })
        );
    }

    #[test]
    fn should_successfully_add_one_vnode() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.add_one_vnode("node1").unwrap();
        assert_eq!(ring.physicals.len(), 3);
        assert_eq!(ring.key_count().unwrap(), 10);

        let range_info = ring.range_info().unwrap();
        assert_eq!(range_info.key_count(), 10);

        assert_eq!(
            range_info.to_json(),
            json!({
              "ranges": [
                {
                  "hash_start": 10,
                  "hash_end": 20,
                  "count": 4,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 20,
                  "hash_end": 30,
                  "count": 2,
                  "items": [
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 30,
                  "hash_end": 40,
                  "count": 1,
                  "items": [
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 40,
                  "hash_end": 50,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 50,
                  "hash_end": 60,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 60,
                  "hash_end": 70,
                  "count": 1,
                  "items": [
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 70,
                  "hash_end": 10,
                  "count": 2,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } }
                  ]
                }
              ]
            })
        )
    }

    #[test]
    fn should_successfully_decrease_num_vnodes_to_zero() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.set_num_vnodes("node1", 0).unwrap();
        assert_eq!(ring.physicals.len(), 2);
        assert_eq!(ring.key_count().unwrap(), 10);

        let range_info = ring.range_info().unwrap();
        assert_eq!(range_info.key_count(), 10);
    }

    #[test]
    fn should_successfully_descrease_num_vnodes() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.set_num_vnodes("node3", 1).unwrap();
        assert_eq!(ring.physicals.len(), 3);
        assert_eq!(ring.key_count().unwrap(), 10);

        let range_info = ring.range_info().unwrap();
        assert_eq!(range_info.key_count(), 10);

        assert_eq!(
            range_info.to_json(),
            json!({
              "ranges": [
                {
                  "hash_start": 10,
                  "hash_end": 20,
                  "count": 4,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 20,
                  "hash_end": 30,
                  "count": 2,
                  "items": [
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 30,
                  "hash_end": 50,
                  "count": 1,
                  "items": [
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 50,
                  "hash_end": 10,
                  "count": 3,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                }
              ]
            })
        )
    }

    #[test]
    fn should_successfully_increase_num_vnodes() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        ring.set_num_vnodes("node1", 3).unwrap();
        assert_eq!(ring.physicals.len(), 3);
        assert_eq!(ring.key_count().unwrap(), 10);

        let range_info = ring.range_info().unwrap();
        assert_eq!(range_info.key_count(), 10);

        assert_eq!(
            range_info.to_json(),
            json!({
              "ranges": [
                {
                  "hash_start": 10,
                  "hash_end": 20,
                  "count": 4,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } },
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 20,
                  "hash_end": 26,
                  "count": 2,
                  "items": [
                    { "hash": 15, "inner": { "key": "key3", "value": "value3" } },
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 26,
                  "hash_end": 30,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 30,
                  "hash_end": 40,
                  "count": 1,
                  "items": [
                    { "hash": 25, "inner": { "key": "key4", "value": "value4" } }
                  ]
                },
                {
                  "hash_start": 40,
                  "hash_end": 50,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 50,
                  "hash_end": 60,
                  "count": 0,
                  "items": []
                },
                {
                  "hash_start": 60,
                  "hash_end": 70,
                  "count": 1,
                  "items": [
                    { "hash": 65, "inner": { "key": "key5", "value": "value5" } }
                  ]
                },
                {
                  "hash_start": 70,
                  "hash_end": 10,
                  "count": 2,
                  "items": [
                    { "hash": 5, "inner": { "key": "key1", "value": "value1" } },
                    { "hash": 6, "inner": { "key": "key2", "value": "value2" } }
                  ]
                }
              ]
            })
        )
    }

    #[test]
    fn items_should_exist() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        assert_eq!(
            ring.get_item("key1").unwrap().to_json(),
            json!({ "key": "key1", "value": "value1" })
        );

        assert_eq!(
            ring.get_item("key2").unwrap().to_json(),
            json!({ "key": "key2", "value": "value2" })
        );
    }

    #[test]
    fn items_should_not_exist() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        assert!(ring.get_item("key100").is_err());
    }

    #[test]
    fn should_return_pids_containing_key() {
        let mut ring = ring();
        let nodes = nodes();

        for node in nodes {
            ring.add_physical_node(node).unwrap();
        }

        let items = items();
        ring.insert_many(&items).unwrap();

        let mut actual = ring.get_pids_containing_key("key1").unwrap();
        actual.sort();
        assert_eq!(actual, vec!["node2".to_string(), "node3".to_string()]);
    }
}
