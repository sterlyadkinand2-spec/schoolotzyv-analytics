"""Workers use durable tasks; AI/research callbacks do not control queue state."""
import copy
import re
from .store import Conflict, LeaseLost, digest


class AccessHeld(Exception):
    def __init__(self, action, target, reason):
        self.action, self.target, self.reason = action, target, reason
        super().__init__(reason)


def validate_snapshot(snapshot):
    if (type(snapshot.get('id')) is not int or snapshot['id'] <= 0 or
        snapshot.get('city_id') != 530 or snapshot.get('type') != 'school_building' or
        snapshot.get('parent', {}).get('type') != 'cluster' or
        snapshot.get('parent', {}).get('city_id') != 530 or
        type(snapshot.get('parent', {}).get('id')) is not int):
        raise ValueError('only verified Moscow public-cluster school buildings')
    if not isinstance(snapshot.get('fields'),dict) or not isinstance(snapshot.get('urls'),list):
        raise ValueError('canonical fields and original URL list required')
    if not isinstance(snapshot.get('writable_fields'),list):
        raise ValueError('explicit writable field allowlist required')


def validate_public_values(value):
    if isinstance(value,str) and re.search(r'вывод\s+по',value,re.IGNORECASE):
        raise ValueError('research annotations forbidden in public fields; keep basis separately')
    if isinstance(value,dict):
        for item in value.values():
            validate_public_values(item)
    elif isinstance(value,list):
        for item in value:
            validate_public_values(item)


def plan(snapshot, candidates):
    """Fill proven gaps only. Existing values, IDs, links and URLs stay unchanged."""
    validate_snapshot(snapshot)
    planned = copy.deepcopy(snapshot)
    reviews = []
    for candidate in candidates:
        key, value = candidate['field'], candidate['value']
        validate_public_values(value)
        if key not in snapshot['writable_fields'] or not candidate.get('evidence') or value in (None,'',[],{}):
            reviews.append({'candidate':candidate,'reason':'missing evidence, empty value or nonwritable field'})
            continue
        existing = planned['fields'].get(key)
        if existing == value:
            continue
        if existing not in (None,'',[],{}):
            reviews.append({'candidate':candidate,'reason':'preserve populated field; normalization needs review'})
            continue
        planned['fields'][key] = value
    return planned,reviews


def research_one(store, lease, researcher):
    lease = store.check(lease)
    snapshot = lease['payload']['snapshot']
    validate_snapshot(snapshot)
    if snapshot['id'] != lease['entity_id']:
        raise ValueError('snapshot ID mismatch')
    candidates, held_sources = [], []
    with store.tx():
        store.check(lease)
        store.log(lease['id'],'read',snapshot)
    # Existing database snapshot is always the first input, even if partly filled.
    for source in lease['payload'].get('sources',[]):
        if store.held('read',source):
            held_sources.append(source)
            continue
        try:
            result = researcher(copy.deepcopy(snapshot),source)
            store.check(lease)
            candidates.extend(result)
        except AccessHeld as error:
            store.hold(error.action,error.target,error.reason,{'task':lease['id'],'source':source})
            held_sources.append(source)
    planned,reviews = plan(snapshot,candidates)
    payload = {'snapshot':snapshot,'planned':planned,'evidence':candidates,'reviews':reviews,'held_sources':held_sources,
               'acceptance':'pending independent source and semantic QA'}
    key = digest({'task':lease['dedupe'],'proposal':payload})
    outputs = [('qa',key,lease['entity_id'],payload)]
    if planned != snapshot:
        outputs.append(('write',key,lease['entity_id'],payload))
    # Missing/held sources are retained for review; other permitted work can continue.
    store.finish(lease,outputs=outputs)


def write_one(store, lease, adapter, dry_run=True, crash=None):
    """Fixture-only implementation. No production write transport can be enabled here."""
    lease = store.check(lease)
    if getattr(adapter,'mode',None) != 'fixture':
        raise RuntimeError('production transport disabled pending handover, CAS and durable runtime')
    store.bind_transport('fixture')
    if lease.get('purpose') == 'readback':
        raise RuntimeError('readback lease cannot send writes')
    before,planned = lease['payload']['snapshot'],lease['payload']['planned']
    validate_snapshot(before)
    validate_snapshot(planned)
    # Independently enforce the gap-only policy at the mutation boundary.
    changed = [k for k in set(before['fields']) | set(planned['fields']) if before['fields'].get(k) != planned['fields'].get(k)]
    if any(k not in before['writable_fields'] or before['fields'].get(k) not in (None,'',[],{}) or planned['fields'].get(k) in (None,'',[],{}) for k in changed):
        raise Conflict('writer accepts evidenced gap fills only')
    if {k:v for k,v in before.items() if k != 'fields'} != {k:v for k,v in planned.items() if k != 'fields'}:
        raise Conflict('identity, URL, scope or metadata changed')
    for key in changed:
        validate_public_values(planned['fields'][key])
        if not any(c.get('field') == key and c.get('value') == planned['fields'][key] and c.get('evidence') for c in lease['payload'].get('evidence',[])):
            raise Conflict('missing field-level evidence')
    op = store.prepare(lease,before,planned)
    if op['state'] in ('applied','dry_run'):
        return _finish_write(store,lease,op['state'])
    if op['state'] in ('held','conflict'):
        return store.finish(lease,status=op['state'],reason='persisted operation outcome')
    target = str(lease['entity_id'])
    if store.held('write',target):
        # Preserve unresolved intents; a denied recovery must still block new writes.
        if op['state'] == 'prepared':
            store.record(lease,'held','result',{'reason':'persisted write hold'})
        return store.finish(lease,'held','persisted write hold')
    try:
        if op['state'] in ('sending','unknown'):
            return _recover(store,lease,adapter,op)
        current = adapter.read(lease['entity_id'])
        store.check(lease)
        if current != before:
            store.record(lease,'conflict','readback',current)
            return store.finish(lease,'conflict','snapshot changed before send')
        if dry_run:
            store.record(lease,'dry_run','result',{'sent':False,'diff_fields':changed})
            return _finish_write(store,lease,'dry_run')
        if crash == 'before_send':
            raise SimulatedCrash()
        store.record(lease,'sending','intent',{'operation_key':op['key'],'expected':digest(before),'send_status':'SENT_UNKNOWN'})
        if crash == 'after_intent':
            raise SimulatedCrash()
        # Backend must enforce expected snapshot and operation key atomically.
        result = adapter.compare_and_set(lease['entity_id'],before,planned,op['key'])
        if crash == 'after_send':
            raise SimulatedCrash()
        store.record(lease,'unknown','result',result)
        return _recover(store,lease,adapter,store.operation(lease['id']))
    except AccessHeld as error:
        store.hold(error.action,error.target,error.reason,{'task':lease['id']})
        current_op = store.operation(lease['id'])
        if current_op['state'] == 'prepared':
            store.record(lease,'held','result',{'reason':error.reason})
        # If send may have occurred, leave intent unresolved and prevent another writer.
        store.finish(lease,'held',error.reason)
    except Conflict as error:
        store.record(lease,'conflict','result',{'reason':str(error)})
        store.finish(lease,'conflict',str(error))
    except LeaseLost:
        raise
    except Exception as error:
        op = store.operation(lease['id'])
        if op['state'] in ('sending','unknown'):
            store.record(lease,'unknown','result',{'error':str(error)})
            # Keep leased until expiry; next claim can ONLY read back this operation.
        else:
            store.finish(lease,'failed',str(error))


class SimulatedCrash(BaseException):
    pass


def recover_one(store,lease,adapter):
    if getattr(adapter,'mode',None) != 'fixture' or lease.get('purpose') != 'readback':
        raise RuntimeError('fixture readback lease required')
    store.check(lease)
    op = store.operation(lease['id'])
    try:
        return _recover(store,lease,adapter,op)
    except AccessHeld as error:
        store.hold(error.action,error.target,error.reason,{'task':lease['id']})
        store.finish(lease,'held','readback denied; unresolved send preserved')
    except LeaseLost:
        raise
    except Exception as error:
        store.record(lease,'unknown','result',{'readback_error':str(error)})
        # No sending path exists here. Retry readback only after this lease expires.


def _recover(store,lease,adapter,op):
    if store.held('readback',str(lease['entity_id'])):
        return store.finish(lease,'held','persisted readback hold; global write gate remains closed')
    receipt = adapter.readback(lease['entity_id'],op['key'])
    store.check(lease)
    if isinstance(receipt,dict) and receipt.get('operation_key') == op['key'] and receipt.get('applied') is True and receipt.get('snapshot') == op['planned_json']:
        store.record(lease,'applied','readback',receipt)
        return _finish_write(store,lease,'applied')
    # Even an equal field value is not proof that this operation executed.
    store.record(lease,'unknown','readback',receipt)
    store.finish(lease,'held','uncertain outcome; no resend; explicit adjudication required')


def _finish_write(store,lease,state):
    outputs = []
    if state == 'applied':
        outputs = [('export',lease['dedupe'],lease['entity_id'],{'snapshot':lease['payload']['planned'],'master_replace':'held'})]
    store.finish(lease,outputs=outputs)


def drain(store,queue,owner,handler,shard=None,max_batches=None):
    """Select next batch from durable state, never from a prompt cursor."""
    batches = 0
    while max_batches is None or batches < max_batches:
        leases = store.claim(queue,owner,shard)
        if not leases:
            break
        batches += 1
        for lease in leases:
            try:
                handler(store,lease)
            except LeaseLost:
                continue  # New owner owns recovery; old owner may not mutate state.
            except Exception as error:
                try:
                    store.finish(lease,'failed',str(error))
                except LeaseLost:
                    pass
    return batches
