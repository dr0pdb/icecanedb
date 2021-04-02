# IcecaneDB

![CI](https://github.com/dr0pdb/icecanedb/workflows/CI/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/dr0pdb/icecanedb)](https://goreportcard.com/report/github.com/dr0pdb/icecanedb)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/github.com/dr0pdb/icecanedb)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/golang-standards/project-layout)](https://pkg.go.dev/github.com/dr0pdb/icecanedb)
[![Release](https://img.shields.io/github/release/golang-standards/project-layout.svg?style=flat-square)](https://github.com/dr0pdb/icecanedb/releases/latest)

A hobby Distributed SQL database management system written in Go. This is being developed with a motive to learn about distributed systems and databases.

**Note**: The project is under development and isn't ready. It isn't part of my focus area at work hence will take a considerable amount of time to make it fully functional.

## Progress

Approximate progress of the project.

### Server
- [x] Storage engine
- [x] Raft
- [x] MVCC transactions
- [x] Key value service

### Client
- [ ] Lexer & Parser - **In Progress**
- [ ] Table encoding
- [ ] Query planner
- [ ] Query execution
- [ ] Query optimization

### Common
- [ ] Unit testing
- [ ] Integration testing

## Features
TODO: List of features

## Architecture
The project is divided into two main parts:
1. Server
2. Client

The server is a grpc service exposing a transactional key value storage layer. The server receives grpc calls from the client and serves them. It also communicates with the other servers for replication using the [Raft consensus protocol](https://raft.github.io/). The server contains a LSM storage layer for persisting data to disk.

The client is responsible for most of the heavy lifting. It provides a REPL for the user to enter their SQL queries. It then executes those queries and uses the server as the storage medium.

TODO: More details, diagrams and blog posts.
