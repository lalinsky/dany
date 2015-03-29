# dany
A python library for managing slony

dany is intended to be used as an alternative for the slonik DSL for
configuring and managing slony clusters. Cool.

## Why?

1. Why not? That's why!
2. A new API in a known language is arguably easier to pick up than a
   new DSL. If you disagree, maybe see reason #1 above.
3. slonik lacks control flow that makes automation easy.

For that last point, we might for instance want to establish paths
between every node in the cluster (with a few assumptions), like so:

```
for server_node in nodes:
    for client_node in nodes:
        if server_node != client_node:
            dan.store_path(server=server_node,
                           client=client_node,
                           conninfo=server_node.conninfo)
```

Slonik's "set add table" offers some regex matching for automatically
adding multiples tables at once, but we could leverage some finer
grained control through all different ways - tables stored as lists in
yaml or other formats, introspected from an orm's model definitions,
extracted from a database, etc. For instance:

```
set_ = dany.Set(...)
cur.execute("select * from pg_class")
for row in cur:
    table = dany.Table(...)
    dan.set_add_table(set_, table)
```

Or maybe we stash a list of sets and their tables on disk. Then we
could keep a simple driver script around to load that config and build
the config. The altperl scripts do something like this, but as a
translation wrapper to slonik.

It's not currently possible to add a table directly to a subscribed
set in slonik's DSL, but even with that limitation in this library, it
would be cool to see hooks in migration libraries that took care of
adding tables to replication sets as the tables are being created. For
instance:

```
def upgrade():
    op.create_table(
        'account',
        ...
    )
    table = dany.Table(name="account", ...)
    dan.set_add_table(some_set, table)

def downgrade():
    table = dany.Table(name="account", ...)
    dan.set_drop_table(some_set, table)
    op.drop_table('account')
```

Although, that muddies up the migration and could make it painful to
use in a testing environment that has no replication. Maybe a dryrun
mode or something similar would be nice to have.

## Testing

There is no installation, yet. However, there are some tests that can
be run if you feel like fighting a bit. Everything as it stands is
hard-coded to work with slony-2.2.4 with at least postgres 8.4.

The testing setup will spin up a temporary postgres instance, so
you'll need to set the paths to the pg_ctl and postgres binaries
manually.

## Caveats

This is more a POC than anything, started because somebody made an
interesting comment in a hallway at a conference.
