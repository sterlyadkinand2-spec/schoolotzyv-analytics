"""Durable connector handoff, no credentials/network and no CAS claim.

The coordinator calls allowed ChatGPT connectors OUTSIDE this module. Never loop
on an emitted write request: once armed its outcome is uncertain until readback.
"""
import copy
from .engine import validate_snapshot, validate_public_values
from .store import Conflict, digest

GET = 'mcp_v_1_6_get_institution'
UPDATE = 'mcp_v_1_6_update_institution'
SCALAR_FIELDS = {'director','former_name','header','text','title','website','educational_status'}


def minimal_patch(before,planned,evidence):
    validate_snapshot(before)
    validate_snapshot(planned)
    if {k:v for k,v in before.items() if k != 'fields'} != {k:v for k,v in planned.items() if k != 'fields'}:
        raise Conflict('identity, URL or card metadata changed')
    patch = {'id':before['id']}
    registry = []
    for field in sorted(set(before['fields']) | set(planned['fields'])):
        old,new = before['fields'].get(field),planned['fields'].get(field)
        if old == new:
            continue
        if field not in before['writable_fields'] or old not in (None,'',[],{}) or new in (None,'',[],{}):
            raise Conflict('only evidenced empty writable fields may be filled')
        if not any(c.get('field') == field and c.get('value') == new and c.get('evidence') for c in evidence):
            raise Conflict('missing field evidence')
        validate_public_values(new)
        if field.isdigit() and int(field)>0 and str(int(field)) == field:
            registry.append({'definition_id':int(field),'value':new})
        elif field in SCALAR_FIELDS:
            patch[field] = new
        else:
            raise ValueError('unmapped MCP field: '+field)
    if registry:
        patch['registry_fields'] = registry
    if len(patch) == 1:
        raise ValueError('empty patch')
    return patch


class MCPBridge:
    def __init__(self,store):
        self.store = store
        store.bind_transport('mcp_bridge')

    def request_before(self,lease):
        lease = self.store.check(lease)
        if lease['queue'] != 'write' or lease.get('purpose') == 'readback':
            raise ValueError('single writer lease required')
        existing = self.store.operation(lease['id'])
        if existing and existing['state'] != 'prepared':
            raise Conflict('existing send may only be reconciled, never reissued')
        return {'tool':GET,'arguments':{'id':lease['entity_id']},'purpose':'fresh_before'}

    def arm(self,lease,current,dry_run=True,handover_evidence=None,hold_dependencies=()):
        """Persist outbox before returning a write. Caller must NOT replay this return.

        current is a fresh canonical GET snapshot, not the old cached snapshot.
        Normalization must retain the whole card and original fields/IDs/URLs.
        """
        lease = self.store.check(lease)
        self.request_before(lease)
        before,planned = lease['payload']['snapshot'],lease['payload']['planned']
        patch = minimal_patch(before,planned,lease['payload'].get('evidence',[]))
        op = self.store.prepare(lease,before,planned)
        with self.store.tx():
            self.store.check(lease)
            self.store.log(lease['id'],'bridge_fresh_get',current)
        if current != before:
            self.store.record(lease,'conflict','readback',current)
            self.store.finish(lease,'conflict','fresh MCP GET differs from before')
            return None
        dependencies = [('write',str(lease['entity_id']))]+list(hold_dependencies)
        for action,target in dependencies:
            if self.store.held(action,target):
                self.store.record(lease,'held','result',{'action':action,'target':target,'reason':'dependency held; no split or alternative route'})
                self.store.finish(lease,'held','dependency held')
                return None
        request = {'tool':UPDATE,'arguments':patch,'operation_key':op['key'],'cas':False,'server_idempotency':False}
        if dry_run:
            with self.store.tx():
                self.store.check(lease)
                self.store.log(lease['id'],'bridge_dry_run',request)
            return {'dry_run':True,'send_allowed':False,'planned_request':request}
        if not handover_evidence or not all(handover_evidence.get(k) for k in ('coordinator','legacy_writers_stopped','inventory_reconciled','authorization_ref')):
            raise ValueError('explicit reconciled coordinator handover required')
        # No connector is called. If caller crashes now, this is SENT_UNKNOWN.
        self.store.record(lease,'sending','intent',{'send_status':'SENT_UNKNOWN','request':request,'handover':handover_evidence,'warning':'MCP has no CAS; external edits may race'})
        return {'dry_run':False,'send_allowed':True,'request':request}

    def record_result(self,lease,result,acknowledged=False):
        lease = self.store.check(lease)
        op = self.store.operation(lease['id'])
        if not op or op['state'] != 'sending':
            raise Conflict('result accepted once for armed operation only')
        self.store.record(lease,'unknown','result',{'connector_result':copy.deepcopy(result),'acknowledged':acknowledged})
        return self.request_readback(lease)

    def request_readback(self,lease):
        lease = self.store.check(lease)
        op = self.store.operation(lease['id'])
        if not op or op['state'] not in ('sending','unknown'):
            raise Conflict('no unresolved send')
        if self.store.held('readback',str(lease['entity_id'])):
            self.store.finish(lease,'held','readback denied; uncertain send retained')
            return None
        return {'tool':GET,'arguments':{'id':lease['entity_id']},'purpose':'readback_only'}

    def record_readback(self,lease,current):
        lease = self.store.check(lease)
        op = self.store.operation(lease['id'])
        if not op or op['state'] not in ('sending','unknown'):
            raise Conflict('no unresolved send')
        acknowledged = bool(op['result_json'] and op['result_json'].get('acknowledged'))
        match = current == op['planned_json']
        # Successful response + GET observes desired state, not race-free attribution.
        state = 'observed_match' if acknowledged and match else 'unknown'
        self.store.record(lease,state,'readback',{'current':current,'desired_state_matches':match,'cas':False,'operation_attribution':'not guaranteed'})
        if state == 'observed_match':
            self.complete_observed(lease)
        else:
            self.store.finish(lease,'held','SENT_UNKNOWN retained; readback alone does not authorize resend')
        return state

    def complete_observed(self,lease):
        """Resume a crash after readback persistence; this never emits a write."""
        lease = self.store.check(lease)
        op = self.store.operation(lease['id'])
        if not op or op['state'] != 'observed_match':
            raise Conflict('verified observed_match required')
        current = op['readback_json']['current']
        self.store.finish(lease,outputs=[('qa','bridge:'+op['key'],lease['entity_id'],{'snapshot':current,'operation_key':op['key'],'acceptance':'pending semantic QA','cas':False})])

    def record_denial(self,lease,action,target,reason,evidence):
        """Hold the EXACT denied action; never infer NOT_SENT for a compound denial."""
        lease = self.store.check(lease)
        self.store.hold(action,target,reason,evidence)
        self.store.finish(lease,'held','denied; no automatic retry or split')


def canonicalize_mcp(raw, verified_parent):
    """Lossless values + card context from v1.6 GET; narrowly scoped writable subset.

    verified_parent is supplied from reconciled inventory, never inferred from a
    peer title. Private/ambiguous cards must not be silently reclassified.
    Preserve original raw response in journal alongside this canonical view.
    """
    if verified_parent.get('type') != 'cluster' or verified_parent.get('city_id') != 530:
        raise ValueError('verified Moscow state-cluster parent required')
    parents = {edge.get('parent_id') for edge in raw.get('links',[]) if edge.get('child_id') == raw.get('id') and edge.get('parent_id') is not None}
    if parents != {verified_parent['id']}:
        raise ValueError('ambiguous or unverified parent relationship')
    city = raw.get('city')
    city_id = city.get('id') if isinstance(city,dict) else raw.get('city_id')
    if city_id not in (None,530):
        raise ValueError('non-Moscow building')
    fields,writable,metadata = {},[],[]
    for field in raw.get('registry_fields',[]):
        key = str(field['definition_id'])
        if key in fields:
            raise ValueError('duplicate registry definition')
        fields[key] = copy.deepcopy(field['value'])
        metadata.append({k:copy.deepcopy(v) for k,v in field.items() if k not in ('value','filled')})
        if field.get('writable') is True and field.get('storage') == 'custom_field' and (field.get('group') == 'building_info' or key == '63'):
            writable.append(key)
    context = {k:copy.deepcopy(v) for k,v in raw.items() if k != 'registry_fields'}
    context['registry_definitions'] = metadata
    snapshot = {'id':raw['id'],'city_id':530,'type':raw['type'],'parent':copy.deepcopy(verified_parent),
                'urls':copy.deepcopy(raw.get('urls',[])),'fields':fields,'writable_fields':writable,'card_context':context}
    validate_snapshot(snapshot)
    return snapshot
