#!/usr/bin/env python

import psycopg2
import psycopg2.extras

import re

DANY_VERSION_STRING = "2.2.4"
DANY_FUNC_VERSION_STRING = "2_2_4"

def multiple_replace(text, adict):
    """
    From the the python cookbook. adict's keys should be the patterns
    to match and their values should be the replacements.
    """
    rx = re.compile("|".join(map(re.escape, adict)))
    def one_xlat(match):
        return adict[match.group(0)]
    return rx.sub(one_xlat, text)

class DanyException(Exception):
    pass

class DanCursor(psycopg2.extras.DictCursor):

    def execute(self, *args, **kwargs):
        try:
            super(DanCursor, self).execute(*args, **kwargs)
        except:
            raise DanyException("NOPE")

    def get_nodes(self):
        qry = """
        select no_id as node_id,
               no_active as active,
               no_comment as comment
        from sl_node
        ;
        """
        self.execute(qry)
        for row in self:
            yield row

    def get_paths(self):
        qry = """
        select pa_server as server,
               pa_client as client,
               pa_conninfo as conninfo,
               pa_connretry as connretry
        from sl_path
        ;
        """
        self.execute(qry)
        for row in self:
            yield row

    def get_listens(self):
        qry = """
        select li_origin as origin_id,
               li_provider as provider_id,
               li_receiver as receiver_id
        from sl_listen
        ;
        """
        self.execute(qry)
        for row in self:
            yield row

    def get_set(self, set_id):
        qry = """
        select set_id as id,
               set_origin as origin,
               set_comment as comment
        from sl_set
        where set_id = %(set_id)s
        ;
        """
        d = {
            "set_id": set_id,
        }
        self.execute(qry, d)
        row = self.fetchone()
        return row

    def get_sets(self):
        qry = """
        select set_id,
               set_origin as origin_id,
               set_comment as comment
        from sl_set
        ;
        """
        self.execute(qry)
        for row in self:
            yield row

    def check_subscription_exists(self, set_id, receiver_id, provider_id):
        qry = """
        select exists (select 1
                       from sl_subscribe
                       where sub_set = %(set_id)s
                         and sub_receiver = %(receiver_id)s
                         and sub_active = true
                         and sub_provider != %(provider_id)s) as sub_exists
        ;
        """
        d = {
            "set_id": set_id,
            "receiver_id": receiver_id,
            "provider_id": provider_id,
        }
        self.execute(qry, d)
        row = self.fetchone()
        return row["sub_exists"]

    def get_subscriptions(self):
        qry = """
        select sub_set as set_id,
               sub_provider as provider_id,
               sub_receiver as receiver_id,
               sub_forward as forward,
               sub_active as active
        from sl_subscribe
        ;
        """
        self.execute(qry)
        for row in self:
            yield row

    def get_max_seqno_by_origin(self):
        qry = """
        select ev_origin as origin_id,
               max(ev_seqno) as max_seqno
        from sl_event
        group by ev_origin
        ;
        """
        self.execute(qry)
        for row in self:
            yield row

    def set_seqno_value(self, name, value):
        qry = """
        select setval(%(sequence)s, %(value)s);
        """
        d = {
            "name": name,
            "value": value,
        }
        self.execute(qry, d)

    def insert_confirmation(self, origin_id, receiver_id, seqno):
        qry = """
        insert into sl_confirm
        (
          con_origin,
          con_received,
          con_seqno,
          con_timestamp
        )
        values
        (
          %(origin_id)s,
          %(received_id)s,
          %(seqno)s,
          current_timestamp
        )
        ;
        """
        d = {
            "origin": origin_id,
            "received": receiver_id,
            "seqno": seqno,
        }
        self.execute(qry, d)

    def initializeLocalNode(self, node_id, comment):
        qry = """
        select initializeLocalNode(%(node_id)s, %(comment)s);
        """
        d = {
            "node_id": node_id,
            "comment": comment,
        }
        self.execute(qry, d)

    def storeNode(self, node_id, comment):
        qry = """
        select storeNode_int(%(node_id)s, %(comment)s);
        """
        d = {
            "node_id": node_id,
            "comment": comment,
        }
        self.execute(qry, d)

    def enableNode(self, node_id):
        qry = """
        select enableNode_int(%(node_id)s);
        """
        d = {
            "node_id": node_id,
        }
        self.execute(qry, d)

    def storePath(self, server_id, client_id, conninfo, connretry):
        qry = """
        select storePath_int(%(server_id)s,
                             %(client_id)s,
                             %(conninfo)s,
                             %(connretry)s)
        ;
        """
        d = {
            "server_id": server_id,
            "client_id": client_id,
            "conninfo": conninfo,
            "connretry": connretry,
        }
        self.execute(qry, d)

    def storeListen(self, origin_id, provider_id, receiver_id):
        qry = """
        select storeListen_int(%(origin_id)s,
                               %(provider_id)s,
                               %(receiver_id)s)
        ;
        """
        d = {
            "origin_id": origin_id,
            "provider_id": provider_id,
            "receiver_id": receiver_id,
        }
        self.execute(qry, d)

    def storeSet(self, set_id, origin_id, comment):
        qry = """
        select storeSet_int(%(set_id)s,
                            %(origin_id)s,
                            %(comment)s)
        ;
        """
        d = {
            "set_id": set_id,
            "origin_id": origin_id,
            "comment": comment,
        }
        self.execute(qry, d)

    def subscribeSet(self, set_id, provider_id, receiver_id, forward, active):
        qry = """
        select subscribeSet_int(%(set_id)s,
                                %(provider_id)s,
                                %(receiver_id)s,
                                %(forward)s,
                                %(active)s)
        ;
        """
        d = {
            "set_id": set_id,
            "provider_id": provider_id,
            "receiver_id": receiver_id,
            "forward": forward,
            "active": active,
        }
        self.execute(qry, d)

    def enableSubscription(self, set_id, provider_id, receiver_id):
        qry = """
        select enableSubscription_int(%(set_id)s,
                                      %(provider_id)s,
                                      %(receiver_id)s)
        ;
        """
        d = {
            "set_id": set_id,
            "provider_id": provider_id,
            "receiver_id": receiver_id,
        }
        self.execute(qry, d)

    def reshapeSubscription(self, set_id, provider_id, receiver_id):
        qry = """
        select reshapeSubscription(%(set_id)s,
                                   %(provider_id)s,
                                   %(receiver_id)s)
        ;
        """
        d = {
            "set_id": set_id,
            "provider_id": provider_id,
            "receiver_id": receiver_id,
        }
        self.execute(qry, d)

    def determineIdxnameUnique(self, fqdn, key):
        qry = """
        select determineIdxnameUnique(%(fqdn)s, %(key)s) as idxname;
        """
        d = {
            "fqdn": fqdn,
            "key": key,
        }
        self.execute(qry, d)
        row = self.fetchone()
        return row["idxname"]

    def setAddTable(self, set_id, table_id, fqdn, idxname, comment):
        qry = """
        select setAddTable(%(set_id)s,
                           %(table_id)s,
                           %(fqdn)s,
                           %(idxname)s,
                           %(comment)s)
        ;
        """
        d = {
            "set_id": set_id,
            "table_id": table_id,
            "fqdn": fqdn,
            "idxname": idxname,
            "comment": comment,
        }
        self.execute(qry, d)

    def createEvent(self, schema, event):
        qry = """
        select createEvent(%(schema)s, %(event)s);
        """
        d = {
            "schema": schema,
            "event": event,
        }
        self.execute(qry, d)

    def acquire_event_lock(self):
        self.execute("lock table sl_event_lock;")

    def acquire_config_lock(self):
        self.execute("lock table sl_config_lock;")

class Node(object):

    def __init__(self, node_id, conninfo, comment=None):

        self.node_id = node_id
        self.conninfo = conninfo
        self.comment = comment

    def connect(self, schema):
        if isinstance(self.conninfo, basestring):
            conn = psycopg2.connect(self.conninfo, cursor_factory=DanCursor)
        elif isinstance(self.conninfo, dict):
            conn = psycopg2.connect(cursor_factory=DanCursor, **self.conninfo)
        else:
            raise DanyException("Bad conninfo: {0}".format(self.conninfo))
        level = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
        conn.set_isolation_level(level)
        cur = conn.cursor()
        cur.execute("set search_path = %s;", (schema,))
        return conn

    def __eq__(self, other):
        return self.node_id == other.node_id

class Cluster(object):

    def __init__(self, cluster_id, name, comment=None):
        """
        Define a slony cluster.
        """

        self.cluster_id = cluster_id
        self.name = name
        self.comment = comment

class Set(object):

    def __init__(self, set_id, comment="An exciting replication set"):
        self.set_id = set_id
        self.comment = comment

class Table(object):

    def __init__(self, table_id, name, schema="public", key=None, comment=None):
        self.table_id = table_id
        self.name = name
        self.schema = schema
        self.key = key
        self.comment = comment

    @property
    def fqdn(self):
        return "{0}.{1}".format(self.schema, self.name)

class AdminConnections(object):

    def __init__(self, schema):
        self.schema = schema
        self.connections = {}

    def __getitem__(self, node):

        if node not in self.connections:
            conn = node.connect(self.schema)
            self.connections[node] = conn

        return self.connections[node]

    def lookup_id(self, node_id):

        for node in self.connections:
            if node.node_id == node_id:
                return self.connections[node]

        raise DanyException("No connection info for node {0}".format(node_id))

    def __iter__(self):
        return iter(self.connections)

class Dan(object):

    def __init__(self, cluster, nodes):
        self.cluster = cluster
        self.schema = "_{0}".format(cluster.name)
        self.nodes = nodes
        self.connections = AdminConnections(self.schema)

    def _load_script(self, cur, fname):
        
        sql = None
        with open(fname) as f:
            d = {
                "@CLUSTERNAME@": self.cluster.name,
                "@MODULEVERSION@": DANY_VERSION_STRING,
                "@NAMESPACE@": self.schema,
                "@FUNCVERSION@": DANY_FUNC_VERSION_STRING,
            }
            sql = multiple_replace(f.read(), d)
        if sql is not None:
            cur.execute(sql)
        else:
            raise DanyException("Couldn't load {0}".format(fname))

    def _load_base(self, cur, node):
        # Maybe one day the schema name will be bound properly here :/
        cur.execute("create schema %s;" % (self.schema,))
        self._load_script(cur, "slony1_base.2.2.4.sql")
        self._load_script(cur, "slony1_base.v84.2.2.4.sql")
        self._load_script(cur, "slony1_funcs.2.2.4.sql")
        self._load_script(cur, "slony1_funcs.v84.2.2.4.sql")

    def _lookup_origin_helper(self, set_id, cur):

        row = cur.get_set(set_id)

        if row is None:
            return None

        return row["origin"]

    def _lookup_origin(self, set_id, cur=None):

        origin_id = None

        msg = "Cannot determine set origin for set {0}".format(set_id)

        if cur is None:

            for node in self.connections:
                cur = self.connections[node].cursor()
                origin_id = self._lookup_origin_helper(set_id, cur)
                if origin_id is not None:
                    return origin_id

            # We made it through all the connections and didn't find
            # the origin
            raise DanyException(msg)

        else:

            origin_id = self._lookup_origin_helper(set_id, cur)

            if origin_id is None:
                raise DanyException(msg)

            return origin_id

    def init_cluster(self, origin):

        conn = self.connections[origin]
        cur = conn.cursor()

        self._load_base(cur, origin)

        cur.acquire_event_lock()
        cur.acquire_config_lock()
        cur.initializeLocalNode(origin.node_id, origin.comment)
        cur.enableNode(origin.node_id)

        conn.commit()

    def store_node(self, node, event_node):

        if node == event_node:
            msg = "ev_origin for store_node cannot be the new node"
            raise DanyException(msg)

        conn_node = self.connections[node]
        cur_node = conn_node.cursor()

        self._load_base(cur_node, node)

        cur_node.acquire_event_lock()
        cur_node.acquire_config_lock()

        cur_node.initializeLocalNode(node.node_id, node.comment)
        cur_node.enableNode(node.node_id)

        conn_event_node = self.connections[event_node]
        cur_event_node = conn_event_node.cursor()

        # Copy sl_node entries from event node to the node being stored
        for row in cur_event_node.get_nodes():
            cur_node.storeNode(row["node_id"], row["comment"])
            if row["active"]:
                cur_node.enableNode(row["node_id"])

        # Copy sl_path entries from event node to the node being stored
        for row in cur_event_node.get_paths():
            cur_node.storePath(row["server_id"],
                               row["client_id"],
                               row["conninfo"],
                               row["connretry"])

        # Copy sl_listen entries from event node to the node being stored
        for row in cur_event_node.get_listens():
            cur_node.storeListen(row["origin_id"],
                                 row["provider_id"],
                                 row["receiver_id"])

        # Copy sl_set
        for row in cur_event_node.get_sets():
            cur_node.storeSet(row["set_id"], row["origin_id"], row["comment"])

        # Copy sl_subscribe
        for row in cur_event_node.get_subscriptions():

            # Every subscription here starts as inactive
            cur_node.subscribeSet(row["set_id"],
                                  row["provider_id"],
                                  row["receiver_id"],
                                  row["forward"],
                                  False)

            # Then, if the subscription is supposed to be active,
            # enable it
            if row["active"]:
                cur_node.enableSubscription(row["set_id"],
                                            row["provider_id"],
                                            row["receiver_id"])

        # For nodes that aren't this new node, mark the new node as
        # having confirmed every event up until now (to mark us as up
        # to speed). I'm not sure how we could have yet generated a
        # noticeable event that the event node would then kick back
        # for us here, but if we did then our event sequence gets
        # kicked up to that value.
        for row in cur_event_node.get_max_seqno_by_origin():
            if node.node_id == row["origin_id"]:
                cur_node.set_seqno_val("sl_event_seq", row["max_seqno"])
            else:
                cur_node.insert_confirmation(row["ev_origin"],
                                             node.node_id,
                                             row["max_ev_seqno"])

        # Now register the new node in the event node. If auto-wait is
        # enabled, this should wait for confirmation.
        cur_event_node.storeNode(node.node_id, node.comment)
        cur_event_node.enableNode(node.node_id)

        conn_node.commit()
        conn_event_node.commit()

    def store_path(self, server, client, conninfo, connretry=10):

        conn = self.connections[client]
        cur = conn.cursor()

        cur.acquire_event_lock()
        cur.acquire_config_lock()

        cur.storePath(server.node_id, client.node_id, conninfo, connretry)

        conn.commit()

    def create_set(self, set_, origin):

        conn = self.connections[origin]
        cur = conn.cursor()

        cur.acquire_event_lock()
        cur.acquire_config_lock()

        cur.storeSet(set_.set_id, origin.node_id, set_.comment)

        conn.commit()

    def set_add_table(self, set_, tables):
        """
        Add each of the provided tables to the given replication set.
        """
        origin_id = self._lookup_origin(set_.set_id)
        conn_origin = self.connections.lookup_id(origin_id)
        cur_origin = conn_origin.cursor()

        cur_origin.acquire_event_lock()
        cur_origin.createEvent(self.schema, "SYNC")

        # wait for that sync

        # Obtain a lock on sl_event_lock. It must be obtained before
        # calling setAddTable()
        cur_origin.acquire_event_lock()
        cur_origin.acquire_config_lock()

        for table in tables:
            idxname = cur_origin.determineIdxnameUnique(table.fqdn, table.key)
            cur_origin.acquire_config_lock()
            cur_origin.setAddTable(set_.set_id,
                                   table.table_id,
                                   table.fqdn,
                                   idxname,
                                   table.comment)

        conn_origin.commit()

    def subscribe_set(self, set_, provider, receiver,
                      forward=True, omitcopy=True):

        conn_provider = self.connections[provider]
        cur_provider = conn_provider.cursor()

        # Check if this node already subscribes to this set via a node
        # that's NOT the proposed provider. If so we will need to
        # reshape it later.
        reshape = cur_provider.check_subscription_exists(set_.set_id,
                                                         receiver.node_id,
                                                         provider.node_id)

        # Now look up the set's definition to find the origin node
        origin_id = self._lookup_origin(set_.set_id, cur_provider)
        conn_origin = self.connections.lookup_id(origin_id)
        cur_origin = conn_origin.cursor()

        cur_origin.acquire_event_lock()
        cur_origin.acquire_config_lock()

        cur_origin.subscribeSet(set_.set_id,
                                provider.node_id,
                                receiver.node_id,
                                forward,
                                omitcopy)

        if reshape:
            conn_receiver = self.connections[receiver]
            cur_receiver = conn_receiver.cursor()

            cur_receiver.acquire_config_lock()
            cur_receiver.reshapeSubscription(set_.set_id,
                                             provider.node_id,
                                             receiver.node_id)

            conn_receiver.commit()

        conn_provider.commit()
        conn_origin.commit()

    def sync(self, node):
        conn = self.connections[node]
        cur = conn.cursor()
        cur.acquire_event_lock()
        cur.createEvent(self.schema, "SYNC")
        conn.commit()
