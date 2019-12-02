# DFS

Simple Distributed File System with Python, HTTP and fuse API that
can be run in docker swarm mode.

# Cluster

To run cluster manualy on single host you can run following commands

```bash
# Run namenode
DFS_NODE_CLASS=name_node.NameNode \
DFS_DB_PATH=./nodes \
DFS_HOST=localhost \
server.py

# Run datanode
DFS_NODE_CLASS=data_node.DataNode \
DFS_FS_ROOT=./rootfs \
DFS_HOST=localhost \
DFS_PORT=8181 \
DFS_NAMENODE_URL=http://localhost:8180/ \
server.py
```

You can run arbitrary number of data nodes with single name node this
way.

To run cluster in docker swarm mode use following command when connected 
to swarm manager

```bash
docker stack deploy -c <(DATA_HOST=<public address> dcc config) dfs
```

or run localy with docker compose

```bash
DATA_HOST=localhost dcc up -d --build
```

Here DATA_HOST defines hostname that clients of cluster should use for
interaction with data nodes, when reading file for example,
`<public address>` is address of any node in swarm that is accessible
by you. In this mode swarm mesh service takes care of distributing
requests among data node containers.

# Client

Clients of the cluster can use it through HTTP requests, through
`HttpNameNode` instance in Python code or throught fuse
(requires `fuse` kernel modules and `fusepy` python package installed)

To use mount DFS you can use `dfs.py` script like this

```bash
dfs.py mount_point 192.168.0.200
```

Here `192.168.0.200` is hostname of swarm manager or any other hostname
from cluster (which is less performant, but still posible due to
swarm mesh).

Alternatively, if you run name node server localy through compose or
directly on host with server running on non-standart port you can run

```bash
dfs.py mount_point localhost 8080
```

