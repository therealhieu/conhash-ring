# conhash-ring: Rust implementation of Consistent Hashing

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/therealhieu/conhash-ring/ci.yml)
[![codecov](https://codecov.io/gh/therealhieu/conhash-ring/graph/badge.svg?token=IUM6TUHHY1)](https://codecov.io/gh/therealhieu/conhash-ring)
![docs.rs](https://img.shields.io/docsrs/conhash-ring)
![Crates.io Version](https://img.shields.io/crates/v/conhash-ring)

## Overview

This is a Rust implementation of consistent hashing, a technique used in distributed systems to distribute data across multiple nodes in a way that minimizes the amount of data that needs to be moved when nodes are added or removed.

This implementation serves as an educational example to demonstrate the concept of a consistent hash ring. It is not designed for production environments.

## Features

- Support pluggable hash functions.
- Support virtual nodes: Each physical node can be represented by multiple virtual nodes to improve load balancing. Physical nodes contain real data, while virtual nodes contain key hashes.
- Support replication factor: Each key can be stored on multiple physical nodes to improve fault tolerance.

## APIs
Checkout [ConsistentHashingRing](https://docs.rs/conhash-ring/latest/conhash_ring/struct.ConsistentHashingRing.html) for more details.

Checkout [ConsistentHashingRing](https://docs.rs/conhash-ring/latest/conhash_ring/struct.ConsistentHashingRing.html) for more details.

## Test case examples

Checkout test cases in `src/lib.rs` for more details.
Criteria is to maintain the replication factor `r`.

- **Adding a key**: When a key is added, it is hashed to a position on the ring. The key is then stored on `r` physical nodes, starting from the position of the key's hash and moving clockwise around the ring. If 2 virtual nodes of the same physical are close to each other, the key will be stored on the first virtual node, then replicated to the next physical nodes

- **Removing a key**: Starting from the position of the key's hash, remove the `r` keys from both clockwise and anti-clockwise directions.

- **Adding a virtual node**: When a new node is added, keys are redistributed clockwise to the nearest virtual node belonging to a physical node that does not already store the key.

- **Removing a virtual node**: When a node is removed, keys are redistributed anti-clockwise to the nearest virtual node belonging to a physical node that does not already store the key.

### 1. Add keys
<div style="display: flex; justify-content: space-between;">
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/init1.png" alt="initial state" width="48%" />
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/add_keys.png" alt="initial state" width="48%" />
</div>

### 2. Remove a key
<div style="display: flex; justify-content: space-between;">
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/init2.png" alt="initial state" width="48%" />
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/remove_key.png" alt="remove key" width="48%" />
</div>

### 3. Removing node 2 (with 2 vnodes)
<div style="display: flex; justify-content: space-between;">
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/init2.png" alt="initial state" width="48%" />
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/remove_node2.png" alt="remove node 2" width="48%" />
</div>

### 4. Adding 1 vnode (hash = 70) to node 1
<div style="display: flex; justify-content: space-between;">
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/init2.png" alt="initial state" width="48%" />
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/add_1_vnode.png" alt="add 1 vnode" width="48%" />
</div>

### 5. Reducing node 3 vnodes to 1
<div style="display: flex; justify-content: space-between;">
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/init2.png" alt="initial state" width="48%" />
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/reduce_vnodes.png" alt="reduce node 3 vnodes" width="48%" />
</div>

### 6. Increasing node 1 vnodes to 3
<div style="display: flex; justify-content: space-between;">
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/init2.png" alt="initial state" width="48%" />
  <img src="https://github.com/therealhieu/conhash-ring/raw/master/images/increase_vnodes.png" alt="increase node 1 vnodes" width="48%" />
</div>
