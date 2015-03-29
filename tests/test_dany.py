#!/usr/bin/env python

import dany

import psycopg2

import shutil
try:
    import subprocess32 as subprocess
except:
    import subprocess
import tempfile
import time
import unittest

pg_ctl = "/usr/lib/postgresql/9.3/bin/pg_ctl"
postgres = "/usr/lib/postgresql/9.3/bin/postgres"

data_dir = None
pg_proc = None
pg_port = "5433"

def setUpModule():
    global data_dir
    global pg_proc

    data_dir = tempfile.mkdtemp()
    rc = subprocess.call([pg_ctl, "init", "-D", data_dir])
    if rc != 0:
        raise Exception("Couldn't initialize db")
    pg_proc = subprocess.Popen([postgres,
                                "-p", pg_port,
                                "-k", data_dir,
                                "-D", data_dir])
    time.sleep(1)

def tearDownModule():
    global data_dir
    global pg_proc
    pg_proc.kill()
    pg_proc.wait()
    shutil.rmtree(data_dir)

class TestDany(unittest.TestCase):

    def setUp(self):

        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="postgres") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("create database slony1;")
                cur.execute("create database slony2;")

        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony1") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                qry = """
                create table public.foo
                (
                  id serial primary key,
                  bar text
                )
                ;
                """
                cur.execute(qry)

    def tearDown(self):
        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="postgres") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("drop database slony2;")
                cur.execute("drop database slony1;")

    def _check_node_exists(self, cur, node):
        qry = """
        select exists (select 1
                       from sl_node
                       where no_id = %(node_id)s)
        ;
        """
        cur.execute(qry, {"node_id": node.node_id})
        row = cur.fetchone()
        self.assertTrue(row[0], "Node does not exist in sl_node")

    def _check_path_exists(self, cur, server, client):
        qry = """
        select exists (select 1
                       from sl_path
                       where pa_server = %(server_id)s
                         and pa_client = %(client_id)s)
        ;
        """
        cur.execute(qry, {"server_id": server.node_id,
                          "client_id": client.node_id})
        row = cur.fetchone()
        self.assertTrue(row[0], "Path does not exist in sl_path")

    def _check_set_exists(self, cur, set_, origin):
        qry = """
        select exists (select 1
                       from sl_set
                       where set_id = %(set_id)s
                         and set_origin = %(origin_id)s)
        ;
        """
        cur.execute(qry, {"set_id": set_.set_id,
                          "origin_id": origin.node_id})
        row = cur.fetchone()
        self.assertTrue(row[0], "Set does not exist in sl_set")

    def _check_set_has_table(self, cur, set_, table):
        qry = """
        select exists (select 1
                       from sl_table
                       where tab_set = %(set_id)s
                         and tab_id = %(table_id)s)
        ;
        """
        cur.execute(qry, {"set_id": set_.set_id,
                          "table_id": table.table_id})
        row = cur.fetchone()
        self.assertTrue(row[0], "Table does not exist in sl_table")

    def _check_subscription_exists(self, cur, set_, provider, receiver):
        qry = """
        select exists (select 1
                       from sl_subscribe
                       where sub_set = %(set_id)s
                         and sub_provider = %(provider_id)s
                         and sub_receiver = %(receiver_id)s)
        ;
        """
        cur.execute(qry, {"set_id": set_.set_id,
                          "provider_id": provider.node_id,
                          "receiver_id": receiver.node_id})
        row = cur.fetchone()
        self.assertTrue(row[0], "Subscription does not exist in sl_subscribe")
        
    def test_init_cluster(self):

        cluster1 = dany.Cluster(cluster_id=1,
                                name="test_cluster",
                                comment="woot")

        node1 = dany.Node(node_id=1,
                          comment="this is node 1",
                          conninfo={"host": data_dir,
                                    "port": pg_port,
                                    "dbname": "slony1"})

        nodes = [node1]

        dan = dany.Dan(cluster=cluster1, nodes=nodes)
        dan.init_cluster(node1)

        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony1") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("set search_path = _test_cluster")
                self._check_node_exists(cur, node1)

    def test_store_node(self):

        cluster1 = dany.Cluster(cluster_id=1,
                                name="test_cluster",
                                comment="woot")

        node1 = dany.Node(node_id=1,
                          comment="this is node 1",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony1"
                          })

        node2 = dany.Node(node_id=2,
                          comment="this is node 2",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony2",
                          })

        nodes = [node1, node2]

        dan = dany.Dan(cluster=cluster1, nodes=nodes)
        dan.init_cluster(node1)
        dan.store_node(node=node2, event_node=node1)

        # Verify both nodes show as registered in slony1
        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony1") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("set search_path = _test_cluster")
                self._check_node_exists(cur, node1)
                self._check_node_exists(cur, node2)

        # Verify both nodes show as registered in slony2
        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony2") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("set search_path = _test_cluster")
                self._check_node_exists(cur, node1)
                self._check_node_exists(cur, node2)

    def test_store_path(self):

        cluster1 = dany.Cluster(cluster_id=1,
                                name="test_cluster",
                                comment="woot")

        node1 = dany.Node(node_id=1,
                          comment="this is node 1",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony1"
                          })

        node2 = dany.Node(node_id=2,
                          comment="this is node 2",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony2",
                          })

        nodes = [node1, node2]

        dan = dany.Dan(cluster=cluster1, nodes=nodes)
        dan.init_cluster(node1)
        dan.store_node(node=node2, event_node=node1)

        conninfo = "host={0} port={1} dbname=slony1".format(data_dir, pg_port)

        dan.store_path(server=node1,
                       client=node2,
                       conninfo=conninfo,
                       connretry=10)

        # Verify path shows as registered in slony2
        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony2") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("set search_path = _test_cluster")
                self._check_path_exists(cur, node1, node2)

    def test_create_set(self):

        cluster1 = dany.Cluster(cluster_id=1,
                                name="test_cluster",
                                comment="woot")

        node1 = dany.Node(node_id=1,
                          comment="this is node 1",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony1"
                          })

        nodes = [node1]

        set1 = dany.Set(set_id=1)

        dan = dany.Dan(cluster=cluster1, nodes=nodes)
        dan.init_cluster(node1)
        dan.create_set(set1, origin=node1)

        # Verify set shows as registered in slony1
        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony1") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("set search_path = _test_cluster")
                self._check_set_exists(cur, set1, node1)

    def test_set_add_table(self):

        cluster1 = dany.Cluster(cluster_id=1,
                                name="test_cluster",
                                comment="woot")

        node1 = dany.Node(node_id=1,
                          comment="this is node 1",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony1"
                          })

        nodes = [node1]

        set1 = dany.Set(set_id=1)

        dan = dany.Dan(cluster=cluster1, nodes=nodes)
        dan.init_cluster(node1)
        dan.create_set(set1, origin=node1)

        table1 = dany.Table(table_id=1,
                            comment="this is a table!",
                            schema="public",
                            name="foo")

        tables = [table1]
        dan.set_add_table(set1, tables)

        # Verify table shows as registered in slony1
        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony1") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("set search_path = _test_cluster")
                self._check_set_has_table(cur, set1, table1)

    def test_subscribe_set(self):

        cluster1 = dany.Cluster(cluster_id=1,
                                name="test_cluster",
                                comment="woot")

        node1 = dany.Node(node_id=1,
                          comment="this is node 1",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony1"
                          })

        node2 = dany.Node(node_id=2,
                          comment="this is node 2",
                          conninfo={
                              "host": data_dir,
                              "port": pg_port,
                              "dbname": "slony2",
                          })

        nodes = [node1, node2]

        set1 = dany.Set(set_id=1)

        dan = dany.Dan(cluster=cluster1, nodes=nodes)
        dan.init_cluster(node1)
        dan.store_node(node=node2, event_node=node1)

        conninfo = "host={0} port={1} dbname=slony1".format(data_dir, pg_port)
        dan.store_path(server=node1,
                       client=node2,
                       conninfo=conninfo,
                       connretry=10)

        dan.create_set(set1, origin=node1)

        table1 = dany.Table(table_id=1,
                            comment="this is a table!",
                            schema="public",
                            name="foo")

        tables = [table1]
        dan.set_add_table(set1, tables)

        dan.subscribe_set(set1, node1, node2)

        # Verify subscription shows as registered in slony1
        with psycopg2.connect(host=data_dir,
                              port=pg_port,
                              dbname="slony1") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("set search_path = _test_cluster")
                self._check_subscription_exists(cur, set1, node1, node2)
