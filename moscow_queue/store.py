"""SQLite coordination for ONE persistent local disk, not shared NFS/container scratch."""
import contextlib
import hashlib
import json
import sqlite3
import time
import uuid


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


class LeaseLost(RuntimeError):
    pass


class Conflict(RuntimeError):
    pass


class Store:
    def __init__(self, path, clock=time.time):
        self.clock = clock
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS tasks (
          id INTEGER PRIMARY KEY, queue TEXT NOT NULL CHECK(queue IN ('research','write','qa','export')),
          dedupe TEXT NOT NULL, entity_id INTEGER NOT NULL, shard INTEGER NOT NULL,
          payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ready',
          owner TEXT, token TEXT, expires REAL, batch TEXT, attempts INTEGER NOT NULL DEFAULT 0,
          reason TEXT, purpose TEXT, UNIQUE(queue,dedupe));
        CREATE INDEX IF NOT EXISTS claimable ON tasks(queue,status,shard,id);
        CREATE TABLE IF NOT EXISTS operations (
          key TEXT PRIMARY KEY, task_id INTEGER NOT NULL UNIQUE REFERENCES tasks(id),
          state TEXT NOT NULL, before_json TEXT NOT NULL, planned_json TEXT NOT NULL,
          result_json TEXT, readback_json TEXT);
        CREATE TABLE IF NOT EXISTS journal (
          seq INTEGER PRIMARY KEY, at REAL NOT NULL, task_id INTEGER REFERENCES tasks(id),
          event TEXT NOT NULL, body TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON journal
          BEGIN SELECT RAISE(ABORT,'append only'); END;
        CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON journal
          BEGIN SELECT RAISE(ABORT,'append only'); END;
        CREATE TABLE IF NOT EXISTS holds (
          action TEXT NOT NULL, target TEXT NOT NULL, reason TEXT NOT NULL,
          evidence TEXT NOT NULL, PRIMARY KEY(action,target));
        CREATE TABLE IF NOT EXISTS imports (key TEXT PRIMARY KEY, digest TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        ''')

    def bind_transport(self, mode):
        with self.tx():
            existing = self.db.execute("SELECT value FROM metadata WHERE key='transport'").fetchone()
            if existing and existing[0] != mode:
                raise Conflict('fixture and MCP bridge must use separate ledgers')
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES('transport',?)",(mode,))

    def close(self):
        self.db.close()

    @contextlib.contextmanager
    def tx(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def log(self, task_id, event, body):
        self.db.execute('INSERT INTO journal(at,task_id,event,body) VALUES(?,?,?,?)',
                        (self.clock(), task_id, event, encode(body)))

    def _enqueue(self, queue, dedupe, entity_id, payload):
        if type(entity_id) is not int or entity_id <= 0:
            raise ValueError('exact positive source ID required')
        raw = encode(payload)
        row = self.db.execute('SELECT * FROM tasks WHERE queue=? AND dedupe=?', (queue,dedupe)).fetchone()
        if row:
            if row['payload'] != raw or row['entity_id'] != entity_id:
                raise Conflict('dedupe key reused with different input')
            return row['id']
        task_id = self.db.execute('INSERT INTO tasks(queue,dedupe,entity_id,shard,payload) VALUES(?,?,?,?,?)',
                         (queue,dedupe,entity_id,entity_id % 3,raw)).lastrowid
        self.log(task_id,'enqueued',{'queue':queue,'dedupe':dedupe})
        return task_id

    def enqueue(self, queue, dedupe, entity_id, payload):
        with self.tx():
            return self._enqueue(queue,dedupe,entity_id,payload)

    def claim(self, queue, owner, shard=None, ttl=120):
        if not owner or ttl <= 0 or (queue == 'research' and shard not in (0,1,2)):
            raise ValueError('owner, positive lease, and research shard 0/1/2 required')
        with self.tx():
            now = self.clock()
            # Expired external intents are NEVER reset to prepared or blindly retried.
            self.db.execute("UPDATE tasks SET status='ready',owner=NULL,token=NULL,expires=NULL WHERE queue=? AND status='leased' AND expires<=?", (queue,now))
            if queue == 'write':
                if self.db.execute("SELECT 1 FROM tasks WHERE queue='write' AND status='leased'").fetchone():
                    return []
                unresolved = self.db.execute("SELECT task_id FROM operations WHERE state IN ('sending','unknown') ORDER BY task_id LIMIT 1").fetchone()
                if unresolved:
                    return []  # Uncertain sends belong exclusively to readback recovery, never another writer.
                else:
                    rows = self.db.execute("SELECT * FROM tasks WHERE queue='write' AND status='ready' ORDER BY id LIMIT 1").fetchall()
            else:
                rows = self.db.execute("SELECT * FROM tasks WHERE queue=? AND status='ready' AND (? IS NULL OR shard=?) ORDER BY id LIMIT 20",(queue,shard,shard)).fetchall()
            batch = str(uuid.uuid4())
            claimed = []
            for row in rows:
                token = str(uuid.uuid4())
                self.db.execute("UPDATE tasks SET status='leased',owner=?,token=?,expires=?,batch=?,purpose=?,attempts=attempts+1 WHERE id=?",(owner,token,now+ttl,batch,queue,row['id']))
                self.log(row['id'],'claimed',{'owner':owner,'token':token,'expires':now+ttl,'batch':batch})
                claimed.append(self.task(row['id']))
            return claimed

    def claim_recovery(self, owner, ttl=120):
        """Lease a READBACK ONLY action; never reassign an unknown send to a writer."""
        if not owner or ttl <= 0:
            raise ValueError('owner and positive lease required')
        with self.tx():
            now = self.clock()
            row = self.db.execute("""SELECT t.* FROM tasks t JOIN operations o ON o.task_id=t.id
                WHERE o.state IN ('sending','unknown') AND
                (t.status='ready' OR (t.status='leased' AND t.expires<=?))
                ORDER BY t.id LIMIT 1""",(now,)).fetchone()
            if not row:
                return None
            token = str(uuid.uuid4())
            self.db.execute("UPDATE tasks SET status='leased',owner=?,token=?,expires=?,purpose='readback' WHERE id=?",
                            (owner,token,now+ttl,row['id']))
            self.log(row['id'],'readback_claimed',{'owner':owner,'token':token,'expires':now+ttl})
            return self.task(row['id'])

    def reconciliation(self, task_id, historical_event, outcome, evidence):
        """Append evidence without rewriting history or releasing holds/leases."""
        if not evidence or outcome not in ('SUCCESS','UNKNOWN','DENIED'):
            raise ValueError('explicit reconciliation outcome and evidence required')
        with self.tx():
            if not self.db.execute('SELECT 1 FROM journal WHERE seq=? AND task_id IS ?', (historical_event,task_id)).fetchone():
                raise ValueError('exact historical journal event required')
            self.log(task_id,'reconciliation',{'historical_event':historical_event,'outcome':outcome,'evidence':evidence})

    def metrics(self):
        """Separate dimensions; retries/replays do not increment entity operation counts."""
        return {
            'read': self.db.execute("SELECT count(DISTINCT task_id) FROM journal WHERE event='read'").fetchone()[0],
            'changed': self.db.execute("SELECT count(*) FROM operations WHERE state='applied'").fetchone()[0],
            'readback': self.db.execute("SELECT count(DISTINCT task_id) FROM journal WHERE event='readback'").fetchone()[0],
            'qa': self.db.execute("SELECT count(*) FROM tasks WHERE queue='qa' AND status='done'").fetchone()[0],
            'public': self.db.execute("SELECT count(*) FROM operations WHERE state='observed_match'").fetchone()[0],  # Observed MCP state, not CAS attribution.
            'excel_staged': self.db.execute("SELECT count(*) FROM tasks WHERE queue='export' AND status='done'").fetchone()[0],
            'excel_merged': 0,  # Master replacement held; no remote readback receipt.
        }

    def task(self, task_id):
        row = dict(self.db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone())
        row['payload'] = json.loads(row['payload'])
        return row

    def check(self, lease):
        row = self.task(lease['id'])
        if row['status'] != 'leased' or row['owner'] != lease['owner'] or row['token'] != lease['token'] or row['expires'] <= self.clock():
            raise LeaseLost('expired or replaced owner/token')
        return row

    def heartbeat(self, lease, ttl=120):
        if ttl <= 0:
            raise ValueError('positive lease required')
        with self.tx():
            self.check(lease)
            self.db.execute('UPDATE tasks SET expires=? WHERE id=?',(self.clock()+ttl,lease['id']))

    def finish(self, lease, status='done', reason=None, outputs=()):
        if status not in ('done','held','conflict','failed'):
            raise ValueError('terminal state required')
        with self.tx():
            self.check(lease)
            for queue,key,entity,payload in outputs:
                self._enqueue(queue,key,entity,payload)
            self.db.execute('UPDATE tasks SET status=?,reason=?,owner=NULL,token=NULL,expires=NULL WHERE id=?',(status,reason,lease['id']))
            self.log(lease['id'],status,{'reason':reason})

    def hold(self, action, target, reason, evidence):
        if not reason or not evidence:
            raise ValueError('holds require exact reason and evidence')
        with self.tx():
            self.db.execute('INSERT OR IGNORE INTO holds VALUES(?,?,?,?)',(action,target,reason,encode(evidence)))
            self.log(None,'hold',{'action':action,'target':target,'reason':reason,'evidence':evidence})

    def held(self, action, target):
        return self.db.execute('SELECT reason FROM holds WHERE action=? AND target=?',(action,target)).fetchone()

    def operation(self, task_id):
        row = self.db.execute('SELECT * FROM operations WHERE task_id=?',(task_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        for key in ('before_json','planned_json','result_json','readback_json'):
            result[key] = json.loads(result[key]) if result[key] is not None else None
        return result

    def prepare(self, lease, before, planned):
        with self.tx():
            self.check(lease)
            if lease['queue'] != 'write':
                raise ValueError('write queue only')
            existing = self.operation(lease['id'])
            if existing:
                if existing['before_json'] != before or existing['planned_json'] != planned:
                    raise Conflict('immutable operation changed')
                return existing
            key = digest({'task':lease['dedupe'],'before':before,'planned':planned})
            self.db.execute('INSERT INTO operations(key,task_id,state,before_json,planned_json) VALUES(?,?,?,?,?)',(key,lease['id'],'prepared',encode(before),encode(planned)))
            self.log(lease['id'],'before',before)
            self.log(lease['id'],'planned',planned)
            return self.operation(lease['id'])

    def record(self, lease, state, event, value):
        allowed = {'prepared':{'sending','dry_run','conflict','held'}, 'sending':{'unknown','applied','observed_match','conflict','held'}, 'unknown':{'unknown','applied','observed_match','conflict','held'}}
        with self.tx():
            self.check(lease)
            op = self.operation(lease['id'])
            if state not in allowed.get(op['state'],set()):
                raise Conflict('invalid operation transition')
            if event not in ('result','readback','intent'):
                raise ValueError('invalid operation event')
            self.db.execute('UPDATE operations SET state=? WHERE task_id=?',(state,lease['id']))
            if event in ('result','readback'):
                column = event + '_json'
                self.db.execute(f'UPDATE operations SET {column}=? WHERE task_id=?',(encode(value),lease['id']))
            self.log(lease['id'],event,value)

    def summary(self):
        return [dict(r) for r in self.db.execute('SELECT queue,status,count(*) AS count FROM tasks GROUP BY queue,status ORDER BY queue,status')]
