#!/usr/bin/env python

import dany

cluster1 = dany.Cluster(cluster_id=1,
                        name="test_cluster",
                        comment="woot")

node1 = dany.Node(node_id=1,
                  comment="this is node 1",
                  conninfo="dbname=slony hostname=node1.test")

node2 = dany.Node(node_id=2,
                  comment="this is node 2",
                  conninfo={
                      "db": "slony",
                      "hostname": "node2.test"
                  })

nodes = [node1, node2]

table1 = dany.Table(table_id=1,
                    comment="this is a table!",
                    key="uk1",
                    schema="public",
                    name="foo")

table2 = dany.Table(table_id=2,
                    comment="this is another table!",
                    name="bar")

tables = [table1, table2]

set1 = dany.Set(set_id=1)

dan = dany.Dan(cluster=cluster1, nodes=nodes)

dan.init_cluster()
dan.store_node(node=node2, event_node=node1)

# Here we set up the situation where
#  1. Every node connects to every other node
#  2. Every node is accessible from every other node through the same
#     "admin conninfo" we use here to manage the nodes
for server_node in nodes:
    for client_node in nodes:
        if server_node != client_node:
            dan.store_path(server=server_node,
                           client=client_node,
                           conninfo=server_node.conninfo)

# Or set up more specific paths
dan.store_path(server=node1, client=node2, conninfo="path.to.node1.from.node2")

dan.create_set(set1, origin=node1)

dan.set_add_table(set1, tables)

dan.subscribe_set(set1, provider=node1, receiver=node2)
