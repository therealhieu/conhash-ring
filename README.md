# conhash-ring: Rust implementation of Consistent Hashing
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/therealhieu/conhash-ring/CI)
[![codecov](https://codecov.io/gh/therealhieu/conhash-ring/graph/badge.svg?token=IUM6TUHHY1)](https://codecov.io/gh/therealhieu/conhash-ring)

## Overview
This is a Rust implementation of consistent hashing, a technique used in distributed systems to distribute data across multiple nodes in a way that minimizes the amount of data that needs to be moved when nodes are added or removed.

This implementation serves as an educational example to demonstrate the concept of a consistent hash ring. It is not designed for production environments.

## Features
- Support pluggable hash functions.
- Support virtual nodes: Each physical node can be represented by multiple virtual nodes to improve load balancing. Physical nodes contain real data, while virtual nodes contain key hashes.
- Support replication factor: Each key can be stored on multiple physical nodes to improve fault tolerance.

## APIs

## Test case examples
### 1. Add keys
![initial state](images/init1.png)
![add keys](images/add_keys.png)

### 2. Removing node 2 (with 2 vnodes)
![init2](images/init2.png)
![remove node 2](images/remove_node2.png)

### 3. Adding 1 vnode (hash = 70) to node 1
![init2](images/init2.png)
![add 1 vnode](images/add_1_vnode.png)

### 4. Reducing node 3 vnodes to 1
![init2](images/init2.png)
![reduce node 3 vnodes](images/reduce_vnodes.png)

### 5. Increasing node 1 vnodes to 3
![init2](images/init2.png)
![increase vnodes](images/increase_vnodes.png)