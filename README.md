<div align="center">
   <img width="100%" src="https://capsule-render.vercel.app/api?type=waving&height=250&color=0:0f172a,50:1e3a8a,100:3b82f6&text=Cudahsyat&fontColor=FFF4A3&fontSize=50" />
</div>

# Raft Protocol


## Group Members

| Name           | NIM        |
|----------------|------------|
|        Brian Ricardo Tamin        |   13523126        |
|    Boye Mangaratua Ginting            |   13523127         |
|    Nathanael Rachmat            |   13523142        |

## Features

| Feature               | Status       |
|---------------------|--------------|
| Heartbeat          | completed  |
| Leader Election    | completed  |
| Log Replication    | completed  |
| Membership Change  | completed  |

## How to Run
```
cargo build
cargo run --bin server <ip_addr> <port>
cargo run --bin client <ip_addr> <port>
```

## Project Description

Implementation of the Raft consensus protocol in Rust using Tonic for gRPC. This project provides a basic Raft implementation with leader election, log replication, and KV store operations.

## Project Structure

```
├── build.rs
├── Cargo.lock
├── Cargo.toml
├── proto
│   └── raft_service.proto
├── README.md
└── src
    ├── client.rs
    └── server.rs
```

## Implemented Features

Based on the existing code, the implemented features include:

- Heartbeat mechanism
- Leader election with voting
- Basic log replication
- KV store operations (GET, SET, APPEND, DEL, STRLEN)
- Basic membership change handling
- Automatic redirect to leader
- Basic state persistence

## Code Architecture

- **server.rs**: Raft node with state management, RPC handlers, and background tasks
- **client.rs**: CLI client for testing and interaction
- **raft_service.proto**: Protobuf definitions for gRPC communication
- **build.rs**: Build script to generate code from proto

## Client Usage

After running the client, available commands:

- ping: Test connection
- get <key>: Get value
- set <key> <value>: Set value
- append <key> <value>: Append to existing value
- del <key>: Delete key
- strln <key>: String length
- request_log: View replication log
- remove <node_id>: Remove member
- change <ip> <port>: Change target server
- help: Help

<div align="center">
   <img width="100%" src="https://capsule-render.vercel.app/api?type=waving&height=150&color=0:0f172a,50:1e3a8a,100:3b82f6&text&fontColor=FFF4A3&fontSize=50&section=footer" />
</div>
