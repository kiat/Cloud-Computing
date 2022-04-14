# Docker commands to install Cassandra

# docker network create cassandra
# docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra


# If you want to login to the container.
# docker exec -it cassandra /bin/bash
# run cqlsh to get to the cassandra shell


# pip install cassandra-driver


# https://docs.datastax.com/en/developer/python-driver/3.24/getting_started/
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

# Documentation link https://cassandra.apache.org/doc/latest/cassandra/data_modeling/data_modeling_rdbms.html

# CQL https://cassandra.apache.org/doc/latest/cassandra/cql/index.html


# From the quick start guide
# We create a keyspace
session.execute("CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };")

# We create a table
session.execute("CREATE TABLE IF NOT EXISTS store.shopping_cart (userid text PRIMARY KEY, item_count int, last_update_timestamp timestamp);")

# Insert into the table.
session.execute("INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('9876', 2, toTimeStamp(now()));")
session.execute("INSERT INTO store.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('1234', 5, toTimeStamp(now()));")

# select all and print them back

a = session.execute("SELECT * FROM store.shopping_cart;")

# print(a.one())
# We print all of the rows
for row in a:
  print(row)
