from redis import Redis
from redis._compat import long
from redis.client import Pipeline
from rediscluster import RedisCluster
from rediscluster.exceptions import RedisClusterException
import sys

def get_cas_page(r, cursor, count):
    return r.scan(
        cursor=cursor,
        match="ContentAddressableStorage:*",
        count=count)

redis_host = None
if len(sys.argv) > 1:
    redis_host = sys.argv[1]
redis_port = 6379
if len(sys.argv) > 2:
    redis_port = int(sys.argv[2])
if not redis_host:
    print ("usage: buildfarm-indexer.py <redis_host>")
    sys.exit(1)

class FakeNodes:
    def __init__(self, name):
        self.name = name

    def all_masters(self):
        return [{"name": self.name}]

    def keyslot(self, any):
        return ""

    def node_from_slot(self, slot):
        return self.name

class FakeStruct:
    def __init__(self, v):
        self.v = v

    def __getitem__(self, i):
        return self.v

    def values(self):
        return [self.v]

try:
    r = RedisCluster(startup_nodes=[{"host": redis_host, "port": redis_port}], skip_full_coverage_check=True)

    nodes = r.connection_pool.nodes

    slots = set(range(0, 16384))

    node_key = 0
    node_keys = {}
    while slots:
        node_key = node_key + 1
        slot = nodes.keyslot(str(node_key))
        if slot in slots:
            slots.remove(slot)
            node_keys[slot] = str(node_key)
    get_connection_by_node = r.connection_pool.get_connection_by_node
except RedisClusterException as e:
    r = Redis(host=redis_host, port=redis_port)
    nodes = FakeNodes("singleton")
    node_keys = FakeStruct("")
    get_connection_by_node = lambda n : r.connection_pool.get_connection("singleton", 0)

# config f"{backplane.workersHashName}_storage"
workers = r.hkeys("Workers_storage")

worker_count = len(workers)

if worker_count == 0:
    print("Refusing to index 0 workers. Just flush the databases")
    sys.exit(1)

print ("%d workers" % worker_count)

p = r.pipeline()
for node_key in node_keys.values():
    p.delete("{%s}:intersecting-workers" % node_key)
    p.sadd("{%s}:intersecting-workers" % node_key, *workers)
p.execute()

print ("created sets")

oversized_cas_names = []

def map_cas_page(r, count, method):
    cursors = {}
    conns = {}
    for master_node in nodes.all_masters():
        name = master_node["name"]
        cursors[name] = "0"
        conns[name] = get_connection_by_node(master_node)

    print("Page Complete: %d %d total: %s" % (0, 0, ", ".join([name for name, cur in cursors.items() if cur != 0])))

    while not all(cursors[node] == 0 for node in cursors):
        for node in cursors:
            if cursors[node] == 0:
                continue

            conn = conns[node]

            pieces = [
                'SCAN', str(cursors[node]),
                'MATCH', "ContentAddressableStorage:*"
            ]
            if count is not None:
                pieces.extend(['COUNT', count])

            conn.send_command(*pieces)

            raw_resp = conn.read_response()

            # cursor, resp = r._parse_scan(raw_resp)
            cursor, resp = raw_resp
            cursor = long(cursor)

            if method(resp, conn, cursors, node):
                cursors[node] = cursor
    for conn in conns.values():
        r.connection_pool.release(conn)

class FakePool:
    def __init__(self, connection):
        self.connection = connection

    def get_connection(self, command, hint):
        return self.connection

    def release(self, conn):
        pass

def highlight(name):
    return f"\x1b[7m{name}\x1b[27m"

class Indexer:
    def __init__(self, r):
        self.processed = 0
        self.r = r

    def pipeline(self, conn):
        return Pipeline(connection_pool=FakePool(conn), response_callbacks={}, transaction=False, shard_hint=None)

    def process(self, cas_names, conn, cursors, current):
        count = len(cas_names)
        p = self.pipeline(conn)
        for i in range(count):
            name = cas_names[i].decode()
            keyslot = nodes.keyslot(name)
            # have to do this if scans return keys from other slots because... redis
            if nodes.node_from_slot(keyslot) == current:
                node_key = node_keys[keyslot]
                set_key = "{%s}:intersecting-workers" % node_key
                p.sinterstore(name, set_key, name)
        p.execute()
        self.processed += count
        print("\x1b[A\x1b[KPage Complete: %d %d total: %s" % (count, self.processed, ", ".join([name if current != name else highlight(name) for name, cur in cursors.items() if cur != 0])))
        return True

indexer = Indexer(r)

map_cas_page(r, 10000, indexer.process)

p = r.pipeline()
for node_key in node_keys.values():
    p.delete("{%s}:intersecting-workers" % node_key)
p.execute()

print("\n%d processed" % (indexer.processed))
