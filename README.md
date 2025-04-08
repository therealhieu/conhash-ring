# conhash-ring: Rust implementation of Consistent Hashing
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/therealhieu/conhash-ring/CI)
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

## Test case examples
Checkout test cases in `src/lib.rs` for more details.

### 1. Add keys
<div style="display: flex; justify-content: space-between;">
  <img src="images/init1.png" alt="initial state" width="48%" />
  <img src="images/add_keys.png" alt="add keys" width="48%" />
</div>

### 2. Removing node 2 (with 2 vnodes)
<div style="display: flex; justify-content: space-between;">
  <img src="images/init2.png" alt="initial state" width="48%" />
  <img src="images/remove_node2.png" alt="remove node 2" width="48%" />
</div>

### 3. Adding 1 vnode (hash = 70) to node 1
<div style="display: flex; justify-content: space-between;">
  <img src="images/init2.png" alt="initial state" width="48%" />
  <img src="images/add_1_vnode.png" alt="add 1 vnode" width="48%" />
</div>

### 4. Reducing node 3 vnodes to 1
<div style="display: flex; justify-content: space-between;">
  <img src="images/init2.png" alt="initial state" width="48%" />
  <img src="images/reduce_vnodes.png" alt="reduce node 3 vnodes" width="48%" />
</div>

### 5. Increasing node 1 vnodes to 3
<div style="display: flex; justify-content: space-between;">
  <img src="images/init2.png" alt="initial state" width="48%" />
  <img src="images/increase_vnodes.png" alt="increase node 1 vnodes" width="48%" />
</div>