import copy
import tempfile
import unittest
from pathlib import Path
from moscow_queue.store import Store, Conflict
from moscow_queue.bridge import MCPBridge


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name)/'queue.sqlite')
        self.bridge = MCPBridge(self.store)
        self.before = {'id':900000,'city_id':530,'type':'school_building','parent':{'id':900003,'city_id':530,'type':'cluster'},'urls':['/fixture'],'fields':{'63':None,'48':'старый факт'},'writable_fields':['63','48']}
        self.planned = copy.deepcopy(self.before)
        self.planned['fields']['63'] = 'fixture value'
        self.task = self.store.enqueue('write','bridge-fixture',900000,{'snapshot':self.before,'planned':self.planned,'evidence':[{'field':'63','value':'fixture value','evidence':'fixture://source'}]})
        self.lease = self.store.claim('write','coordinator')[0]
        self.authorization = {'coordinator':'fixture','legacy_writers_stopped':True,'inventory_reconciled':True,'authorization_ref':'fixture-only'}

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def arm(self):
        return self.bridge.arm(self.lease,self.before,False,self.authorization)

    def test_default_dry_run_exact_minimal_patch(self):
        self.assertEqual(self.bridge.request_before(self.lease)['arguments'],{'id':900000})
        result = self.bridge.arm(self.lease,self.before)
        self.assertFalse(result['send_allowed'])
        self.assertEqual(result['planned_request']['arguments'],{'id':900000,'registry_fields':[{'definition_id':63,'value':'fixture value'}]})
        self.assertEqual(self.store.operation(self.task)['state'],'prepared')

    def test_handover_required_and_reissue_prohibited(self):
        with self.assertRaises(ValueError):
            self.bridge.arm(self.lease,self.before,False)
        self.assertTrue(self.arm()['send_allowed'])
        with self.assertRaises(Conflict):
            self.arm()

    def test_ack_and_readback_are_observation_not_cas(self):
        self.arm()
        request = self.bridge.record_result(self.lease,{'success':True},True)
        self.assertEqual(request['purpose'],'readback_only')
        self.assertEqual(self.bridge.record_readback(self.lease,self.planned),'observed_match')
        self.assertFalse(self.store.operation(self.task)['readback_json']['cas'])
        self.assertEqual(self.store.metrics()['public'],1)
        self.assertEqual(self.store.metrics()['excel_merged'],0)
        self.assertEqual(self.store.task(self.task)['status'],'done')

    def test_null_never_proves_not_sent(self):
        self.arm()
        self.bridge.record_result(self.lease,{'TLS':'error'},False)
        self.assertEqual(self.bridge.record_readback(self.lease,None),'unknown')
        self.assertEqual(self.store.operation(self.task)['readback_json']['current'],None)
        self.assertEqual(self.store.claim('write','new-writer'),[])

    def test_fresh_get_conflict(self):
        other = copy.deepcopy(self.before)
        other['fields']['48'] = 'new human edit'
        self.assertIsNone(self.bridge.arm(self.lease,other,False,self.authorization))
        self.assertEqual(self.store.task(self.task)['status'],'conflict')

    def test_compound_hold_dependency_blocks_components(self):
        self.store.hold('preflight','292194:contract+GET+status','scope unknown; denied',{'source':'coordinator'})
        result = self.bridge.arm(self.lease,self.before,False,self.authorization,[('preflight','292194:contract+GET+status')])
        self.assertIsNone(result)
        self.assertEqual(self.store.task(self.task)['status'],'held')

    def test_denial_after_arm_retains_unknown_and_exact_reason(self):
        self.arm()
        reason = 'This Project file does not have an authorized raw-byte materialization path.'
        self.bridge.record_denial(self.lease,'archive-read','fixture://archive',reason,{'source':'fixture'})
        self.assertEqual(self.store.held('archive-read','fixture://archive')[0],reason)
        self.assertEqual(self.store.operation(self.task)['state'],'sending')
        self.assertEqual(self.store.claim('write','new'),[])

class CanonicalTest(unittest.TestCase):
    def test_top_level_fact_preserved_private_and_scope_fields_rejected(self):
        from moscow_queue.bridge import canonicalize_mcp
        raw = {'id':900000,'type':'school_building','city':None,'educational_status':'начальная школа (1-4 классы)',
               'urls':[{'url':'/fixture'}], 'links':[{'parent_id':900003,'child_id':900000}],
               'registry_fields':[{'definition_id':63,'value':None,'writable':True,'storage':'custom_field','group':None},
                                  {'definition_id':33,'value':None,'writable':True,'storage':'school_column','group':'geo'},
                                  {'definition_id':10,'value':None,'writable':True,'storage':'custom_field','group':'kindergarten_info'}]}
        parent = {'id':900003,'type':'cluster','city_id':530}
        snapshot = canonicalize_mcp(raw,parent)
        self.assertEqual(snapshot['writable_fields'],['63'])
        self.assertEqual(snapshot['card_context']['educational_status'],raw['educational_status'])
        self.assertEqual(snapshot['urls'],raw['urls'])
        raw['type'] = 'private_school_building'
        with self.assertRaises(ValueError):
            canonicalize_mcp(raw,parent)
