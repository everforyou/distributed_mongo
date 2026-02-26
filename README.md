# distributed-mongo

A lightweight distributed MongoDB wrapper with multi-server support and storage-aware balancing.

## Features

- Multi-server MongoDB support
- Automatic size-aware distribution
- Bulk operations with retry & backoff
- Pluggable logger

## Installation

```bash
pip install .
```

## Usage

```python
from distributed_mongodb import DistributedMongoDB

servers = ["mongodb://localhost:27017"]
db = DistributedMongoDB(servers, "test_db", "test_collection")

db.find({"name": "John"})
```
