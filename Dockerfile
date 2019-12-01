FROM python:3.8

WORKDIR /app

COPY . .

ENV DFS_NODE_CLASS= \
    DFS_HOST=0.0.0.0 \
    DFS_PORT=8180 \
    DFS_FS_ROOT=/data/rootfs \
    DFS_NAMENODE_URL= \
    DFS_DB_PATH=/data/nodes

VOLUME /data

CMD ["python", "server.py"]
