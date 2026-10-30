"""Import only a reconciled coordinator manifest, retaining original records verbatim."""
from .store import Conflict, digest, encode
from .engine import validate_snapshot

MASTER = 'libfile_2b65a185f7288191b24705e00a6df22f'


def import_manifest(store,manifest):
    if not manifest.get('reconciled_by') or not manifest.get('evidence') or manifest.get('version') != 1:
        raise ValueError('coordinator reconciliation identity and evidence required')
    if set(manifest.get('collectors',{})) != {'МСК3','МСК4','МСК5'}:
        raise ValueError('exact reconciled state for all three old collectors required')
    for name,record in manifest['collectors'].items():
        if not all(k in record for k in ('active','leases','reserves','holds','journal_ref')):
            raise ValueError('missing collector state: '+name)
    if manifest.get('master') != {'id':MASTER,'replace':'held'}:
        raise ValueError('master Excel replace must remain held')
    content_hash = digest(manifest)
    key = manifest['key']
    with store.tx():
        previous = store.db.execute('SELECT digest FROM imports WHERE key=?',(key,)).fetchone()
        if previous:
            if previous[0] != content_hash:
                raise Conflict('manifest changed under same key')
            return False
        for hold in manifest['holds']:
            if not all(hold.get(k) for k in ('action','target','reason','evidence')):
                raise ValueError('incomplete exact hold')
            store.db.execute('INSERT OR IGNORE INTO holds VALUES(?,?,?,?)',(hold['action'],hold['target'],hold['reason'],encode(hold['evidence'])))
        store.db.execute('INSERT OR IGNORE INTO holds VALUES(?,?,?,?)',('replace',MASTER,'coordinator: master replacement held',encode(manifest['evidence'])))
        for item in manifest['tasks']:
            validate_snapshot(item['snapshot'])
            if item['entity_id'] != item['snapshot']['id']:
                raise ValueError('ID mismatch')
            # No inferred IDs or fresh ready tasks from active/reserved/unknown work.
            task_id = store._enqueue(item['queue'],item['key'],item['entity_id'],{'snapshot':item['snapshot'],'legacy':item})
            status = item['status']
            if status not in ('ready','done','held','failed','leased','reserved','unknown'):
                raise ValueError('unrecognized imported status')
            imported_status = status if status in ('done','held','failed') else 'held'
            # All imported work is frozen until exact reservations/holds are reviewed.
            store.db.execute('UPDATE tasks SET status=?,reason=? WHERE id=?',(imported_status,'staged import; explicit per-operation handover required',task_id))
        store.db.execute('INSERT INTO imports VALUES(?,?)',(key,content_hash))
        store.log(None,'coordinator_manifest',manifest)
    return True
