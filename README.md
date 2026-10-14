# P2P File Synchronization System

A peer-to-peer (P2P) file synchronization system developed in Python using UDP sockets as a team project for the Computer Networks: Application and Transport course at UNISINOS.

Each peer can send and receive files, detect changes in a shared directory, and propagate file additions, modifications, and removals to other configured peers.

## Overview

The system maintains a synchronized `tmp/` directory across multiple peers without relying on a central file server.

Each peer runs an independent Python process and communicates directly with the other peers through UDP. The network uses a static peer list defined in a JSON configuration file.

## Features

- **Distributed file synchronization:** Propagates file additions, modifications, and removals between peers.
- **Chunk-based transfers:** Splits files into smaller chunks for transmission.
- **Application-level acknowledgments:** Uses ACK messages and retransmission attempts to handle missing acknowledgments.
- **Out-of-order chunk handling:** Stores received chunks by sequence number and reconstructs the file in the correct order.
- **New peer synchronization:** Allows a peer to request the existing file list when joining the network.
- **Change detection:** Uses file size and SHA-256 metadata to identify changes in local files.
- **Concurrent execution:** Uses Python threads to receive messages and monitor the shared directory.
- **JSON messaging:** Encodes protocol messages as JSON and file chunks as Base64 data.

## Architecture

Each peer acts as both a sender and a receiver. Peers are configured in advance and communicate directly with one another.

```text
                 Peer 1
                UDP :5001
               /         \
              /           \
      Peer 2  <----------> Peer 3
     UDP :5002             UDP :5003
```

The diagram illustrates a three-peer setup. Each peer has a unique identifier, a listening address and port, a list of known peers, and a local `tmp/` directory.

## Communication Protocol

The application defines several message types:

| Message | Purpose |
|---|---|
| `ANNOUNCE` | Announces a file addition or modification, including its metadata. |
| `REQUEST` | Requests a file from another peer. |
| `DATA` | Transfers a file chunk with its sequence number and transfer metadata. |
| `ACK` | Acknowledges receipt of a chunk. |
| `REMOVE` | Propagates a file removal. |
| `LIST_REQUEST` | Requests the current file list from another peer. |
| `LIST_RESPONSE` | Returns file metadata to support initial synchronization. |

### File Transfer

Files are divided into chunks of 700 bytes before Base64 encoding and transmission. For each chunk, the sender waits up to one second for an acknowledgment and makes up to five transmission attempts.

The receiver stores chunks by sequence number and reconstructs the file after receiving all expected chunks. It then checks the reconstructed file size before saving it.

SHA-256 metadata is used to compare file versions during synchronization. End-to-end SHA-256 verification of the reconstructed file against the sender's expected hash is not currently implemented.

Because UDP does not guarantee delivery, ordering, or uniqueness of datagrams, these application-level mechanisms help handle some transmission issues. They do not provide a general guarantee of reliable delivery.

## Technologies

- **Python 3**
- UDP sockets (`socket`)
- Multithreading (`threading`)
- JSON (`json`)
- File handling (`os`)
- SHA-256 hashing (`hashlib`)
- Base64 encoding (`base64`)
- Git and GitHub

The project uses Python's standard library and does not require third-party packages.

## Project Structure

```text
trabalho-grau-a-redes-p2p/
├── tmp/
│   └── .gitkeep
├── .gitignore
├── config_exemplo.json
├── peer.py
└── README.md
```

- `peer.py` — Main peer implementation and message handling.
- `config_exemplo.json` — Template for configuring a peer.
- `tmp/` — Directory monitored for file changes and used to store synchronized files.
- `.gitignore` — Excludes local configuration and temporary transfer files.

## Getting Started

### Prerequisites

- Python 3 installed.
- A network environment in which the configured peers can communicate over UDP.

No external Python packages are required.

### 1. Clone the repository

```bash
git clone https://github.com/eduardo-heck/trabalho-grau-a-redes-p2p.git
cd trabalho-grau-a-redes-p2p
```

### 2. Configure a peer

Create a local file named `config.json` by copying `config_exemplo.json`.

Example configuration:

```json
{
  "peer_id": "peer1",
  "host": "0.0.0.0",
  "port": 5001,
  "peers": [
    {
      "id": "peer2",
      "host": "PEER_2_IP",
      "port": 5002
    },
    {
      "id": "peer3",
      "host": "PEER_3_IP",
      "port": 5003
    }
  ]
}
```

Replace the example peer addresses with the reachable IP addresses of the other machines. Configure a unique identifier and listening port for each peer, and ensure the relevant UDP traffic is permitted by the network and firewall.

Each peer needs its own configuration. The local `config.json` is excluded from version control; do not commit machine-specific addresses.

### 3. Start the peer

From the project directory, run:

```bash
python peer.py
```

To specify a configuration file explicitly, run:

```bash
python peer.py config.json
```

Start each configured peer on its respective machine. Add or modify files inside its `tmp/` directory to trigger synchronization.

## Testing

The original course project documented tests involving three peers on separate computers connected through a virtual network. The scenarios included file creation, modification, removal, chunk transfers, acknowledgment handling, and synchronization of a newly joining peer.

These tests explored how peers coordinate file changes and transfer files over UDP.

## Limitations

- Peer discovery is not automatic; peers must be configured in advance.
- The system does not guarantee reliable delivery under all network conditions.
- Completed transfers are checked for file size, but final SHA-256 verification against the sender's hash is not yet implemented.
- The current implementation is intended as an educational networking project, not a production-ready file synchronization service.

## Team

Developed collaboratively by **Eduardo Heck, Felipe Egert, and Lucas Schubert**.

**Institution:** University of Vale do Rio dos Sinos (UNISINOS)  
**Course:** Computer Networks: Application and Transport  
**Year:** 2026