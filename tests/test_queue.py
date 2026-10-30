import concurrent.futures
import copy
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from moscow_queue.store import Store, LeaseLost, Conflict
from moscow_queue.engine import write_one, recover_one, research_one, drain, SimulatedCrash, AccessHeld, plan
from moscow_queue.adapters import FixtureDatabase, export_local_xlsx
from moscow_queue.handover import import_manifest, MASTER

FIXTURE = json.loads((Path(__file__).resolve().parent.parent/'fixtures/example.json').read_text())


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.now = 1000
        self.store = Store(self.path/'queue.sqlite',lambda:self.now)
        self.backend = FixtureDatabase(self.path/'backend.sqlite')
        self.before = copy.deepcopy(FIXTURE['snapshot'])
        self.backend.seed(self.before)
        self.planned = plan(self.before,FIXTURE['candidates'])[0]

    def tearDown(self):
        self.store.close()
        self.backend.close()
        self.tmp.cleanup()

    def task(self,queue='write',key='one',entity=900000):
        return self.store.enqueue(queue,key,entity,{'snapshot':self.before,'planned':self.planned,'evidence':FIXTURE['candidates'],'sources':FIXTURE['sources']})

    def claim(self):
        return self.store.claim('write','coordinator',ttl=10)[0]

    def restart(self):
        self.store.close()
        self.store = Store(self.path/'queue.sqlite',lambda:self.now)
        self.backend.close()
        self.backend = FixtureDatabase(self.path/'backend.sqlite')

    def test_crash_before_send(self):
        task = self.task()
        lease = self.claim()
        with self.assertRaises(SimulatedCrash):
            write_one(self.store,lease,self.backend,False,'before_send')
        self.assertEqual(self.backend.read(900000),self.before)
        self.now += 11
        self.restart()
        write_one(self.store,self.claim(),self.backend,False)
        self.assertEqual(self.store.task(task)['status'],'done')
        self.assertEqual(self.backend.db.execute('SELECT count(*) FROM receipts').fetchone()[0],1)

    def test_crash_after_send_readback_no_resend(self):
        task = self.task()
        with self.assertRaises(SimulatedCrash):
            write_one(self.store,self.claim(),self.backend,False,'after_send')
        self.now += 11
        self.restart()
        self.assertEqual(self.store.claim('write','new-writer'),[])
        with patch.object(self.backend,'compare_and_set',side_effect=AssertionError('must not resend')):
            recover_one(self.store,self.store.claim_recovery('readback'),self.backend)
        self.assertEqual(self.store.task(task)['status'],'done')
        self.assertEqual(self.store.operation(task)['state'],'applied')
        events = [r[0] for r in self.store.db.execute('SELECT event FROM journal WHERE task_id=?',(task,))]
        self.assertIn('before',events)
        self.assertIn('planned',events)
        self.assertIn('readback',events)

    def test_crash_after_intent_blocks_all_writes_without_receipt(self):
        task = self.task()
        with self.assertRaises(SimulatedCrash):
            write_one(self.store,self.claim(),self.backend,False,'after_intent')
        self.task(key='second',entity=900003)
        self.now += 11
        recover_one(self.store,self.store.claim_recovery('readback'),self.backend)
        self.assertEqual(self.store.task(task)['status'],'held')
        self.assertEqual(self.store.operation(task)['state'],'unknown')
        self.assertEqual(self.store.claim('write','other'),[])

    def test_stale_lease_and_wrong_owner(self):
        task = self.task()
        old = self.claim()
        forged = dict(old,owner='other')
        with self.assertRaises(LeaseLost):
            self.store.finish(forged)
        self.now += 11
        new = self.claim()
        with self.assertRaises(LeaseLost):
            self.store.prepare(old,self.before,self.planned)
        with self.assertRaises(LeaseLost):
            self.store.heartbeat(old)
        self.store.finish(new)
        self.assertEqual(self.store.task(task)['status'],'done')

    def test_duplicate_task_and_different_payload(self):
        task = self.task()
        self.assertEqual(task,self.task())
        with self.assertRaises(Conflict):
            self.store.enqueue('write','one',900000,{'changed':True})

    def test_concurrent_claim(self):
        for i in range(60):
            self.task('research',str(i),900000+i*3)
        def claim_worker(n):
            store = Store(self.path/'queue.sqlite',lambda:self.now)
            try:
                return store.claim('research',str(n),0)
            finally:
                store.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            batches = list(pool.map(claim_worker,range(6)))
        ids = [t['id'] for batch in batches for t in batch]
        self.assertEqual(len(ids),60)
        self.assertEqual(len(set(ids)),60)
        self.assertTrue(all(len(b)<=20 for b in batches))

    def test_single_writer_and_independent_qa(self):
        self.task()
        self.task(key='two')
        self.task('qa','qa')
        self.claim()
        self.assertEqual(self.store.claim('write','second'),[])
        self.assertEqual(len(self.store.claim('qa','qa-worker')),1)

    def test_conflicting_edit(self):
        task = self.task()
        other = copy.deepcopy(self.before)
        other['fields']['programs'] = 'human edit'
        self.backend.compare_and_set(900000,self.before,other,'external')
        write_one(self.store,self.claim(),self.backend,False)
        self.assertEqual(self.store.task(task)['status'],'conflict')
        self.assertEqual(self.backend.read(900000),other)

    def test_edit_between_read_and_send(self):
        task = self.task()
        original = self.backend.compare_and_set
        other = copy.deepcopy(self.before)
        other['fields']['address'] = 'new human address'
        def race(entity,before,planned,key):
            original(entity,before,other,'external')
            return original(entity,before,planned,key)
        with patch.object(self.backend,'compare_and_set',side_effect=race):
            write_one(self.store,self.claim(),self.backend,False)
        self.assertEqual(self.store.task(task)['status'],'conflict')
        self.assertEqual(self.backend.read(900000),other)

    def test_held_source_other_source_continues(self):
        payload = {'snapshot':self.before,'sources':['held://archive','fixture://official']}
        self.store.hold('read','held://archive','denied',{'ticket':'fixture'})
        task = self.store.enqueue('research','research',900000,payload)
        seen = []
        def research(snapshot,source):
            seen.append(source)
            return FIXTURE['candidates']
        research_one(self.store,self.store.claim('research','r',0)[0],research)
        self.assertEqual(seen,['fixture://official'])
        self.assertEqual(self.store.task(task)['status'],'done')
        self.assertEqual(len(self.store.claim('write','w')),1)

    def test_new_access_denial_persisted(self):
        self.task('research')
        def denied(snapshot,source):
            raise AccessHeld('read',source,'CAPTCHA')
        research_one(self.store,self.store.claim('research','r',0)[0],denied)
        self.restart()
        self.assertTrue(self.store.held('read','fixture://official'))

    def test_failed_excel_does_not_block_research(self):
        task = self.task('export')
        with patch('moscow_queue.adapters.Path.mkdir',side_effect=OSError('disk full')):
            drain(self.store,'export','excel',lambda s,t:export_local_xlsx(s,t,self.path/'exports'))
        self.assertEqual(self.store.task(task)['status'],'failed')
        self.task('research')
        self.assertEqual(len(self.store.claim('research','r',0)),1)

    def test_excel_is_valid_local_artifact(self):
        self.task('export')
        lease = self.store.claim('export','excel')[0]
        path = export_local_xlsx(self.store,lease,self.path/'exports')
        import xml.etree.ElementTree as ET
        with zipfile.ZipFile(path) as z:
            self.assertIsNone(z.testzip())
            for file in z.namelist():
                ET.fromstring(z.read(file))
        self.assertNotIn('Корпуса_Москвы_единая_база',path.name)

    def test_dry_run_sends_nothing(self):
        task = self.task()
        with patch.object(self.backend,'compare_and_set',side_effect=AssertionError('dry run wrote')):
            write_one(self.store,self.claim(),self.backend)
        self.assertEqual(self.store.operation(task)['state'],'dry_run')
        self.assertEqual(self.backend.read(900000),self.before)
        self.assertEqual(self.store.claim('export','e'),[])

    def test_existing_values_and_ids_preserved(self):
        candidates = FIXTURE['candidates']+[{'field':'address','value':'replacement','evidence':'fixture'}]
        planned,reviews = plan(self.before,candidates)
        self.assertEqual(planned['fields']['address'],self.before['fields']['address'])
        self.assertEqual(planned['id'],self.before['id'])
        self.assertEqual(planned['urls'],self.before['urls'])
        self.assertEqual(len(reviews),1)
        self.assertIsNotNone(planned['fields']['programs'])

    def test_next_batches_and_shards(self):
        for i in range(125):
            self.task('research',str(i),900000+i)
        for shard in range(3):
            seen = []
            def done(s,t):
                seen.append(t['entity_id'])
                s.finish(t)
            batches = drain(self.store,'research',f'r{shard}',done,shard)
            self.assertEqual(batches,3)
            self.assertTrue(all(i%3==shard for i in seen))
        self.assertEqual(self.store.summary(),[{'queue':'research','status':'done','count':125}])

    def test_production_adapter_disabled(self):
        self.task()
        with self.assertRaisesRegex(RuntimeError,'production transport disabled'):
            write_one(self.store,self.claim(),object(),False)

    def test_ambiguous_equal_values_not_proof(self):
        task = self.task()
        with self.assertRaises(SimulatedCrash):
            write_one(self.store,self.claim(),self.backend,False,'after_intent')
        self.backend.compare_and_set(900000,self.before,self.planned,'someone-else')
        self.now += 11
        recover_one(self.store,self.store.claim_recovery('readback'),self.backend)
        self.assertEqual(self.store.operation(task)['state'],'unknown')

    def test_crash_after_applied_before_finish_recovers_exports(self):
        task = self.task()
        lease = self.claim()
        with patch.object(self.store,'finish',side_effect=SimulatedCrash()):
            with self.assertRaises(SimulatedCrash):
                write_one(self.store,lease,self.backend,False)
        self.assertEqual(self.store.operation(task)['state'],'applied')
        self.now += 11
        write_one(self.store,self.claim(),self.backend,False)
        self.assertEqual(len(self.store.claim('export','e')),1)

    def test_import_requires_exact_reconciliation_and_keeps_holds(self):
        with self.assertRaises(ValueError):
            import_manifest(self.store,{})
        manifest = {'key':'fixture-import','version':1,'reconciled_by':'fixture-coordinator','evidence':'fixture only',
                    'collectors':{n:{'active':False,'leases':[],'reserves':[],'holds':[],'journal_ref':'fixture'} for n in ('МСК3','МСК4','МСК5')},
                    'master':{'id':MASTER,'replace':'held'},'holds':[{'action':'read','target':'fixture://denied','reason':'denied','evidence':'fixture'}],
                    'tasks':[{'key':'legacy','queue':'research','entity_id':900000,'snapshot':self.before,'status':'reserved'}]}
        self.assertTrue(import_manifest(self.store,manifest))
        self.assertFalse(import_manifest(self.store,manifest))
        self.assertEqual(self.store.claim('research','r',0),[])
        self.assertTrue(self.store.held('replace',MASTER))
        self.assertTrue(self.store.held('read','fixture://denied'))

    def test_null_readback_preserves_historical_sent_unknown(self):
        task = self.task()
        with self.assertRaises(SimulatedCrash):
            write_one(self.store,self.claim(),self.backend,False,'after_send')
        self.now += 11
        with patch.object(self.backend,'readback',return_value=None):
            recover_one(self.store,self.store.claim_recovery('readback'),self.backend)
        intent = self.store.db.execute("SELECT seq,body FROM journal WHERE task_id=? AND event='intent'",(task,)).fetchone()
        self.assertEqual(json.loads(intent['body'])['send_status'],'SENT_UNKNOWN')
        self.assertEqual(self.store.operation(task)['state'],'unknown')
        raw = self.store.db.execute("SELECT body FROM journal WHERE task_id=? AND event='readback'",(task,)).fetchone()[0]
        self.assertEqual(raw,'null')
        self.store.reconciliation(task,intent['seq'],'SUCCESS',{'guarded_CAS':'fixture journal restoration only'})
        self.assertEqual(self.store.operation(task)['state'],'unknown')
        self.assertEqual(self.store.claim('write','new-writer'),[])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM journal WHERE event='reconciliation'").fetchone()[0],1)

    def test_readback_lease_cannot_send(self):
        self.task()
        with self.assertRaises(SimulatedCrash):
            write_one(self.store,self.claim(),self.backend,False,'after_intent')
        self.now += 11
        lease = self.store.claim_recovery('recovery')
        with self.assertRaisesRegex(RuntimeError,'readback lease cannot send'):
            write_one(self.store,lease,self.backend,False)

    def test_research_annotations_rejected_before_commit(self):
        for field in ('48','51'):
            snapshot = copy.deepcopy(self.before)
            snapshot['writable_fields'].append(field)
            with self.assertRaisesRegex(ValueError,'research annotations forbidden'):
                plan(snapshot,[{'field':field,'value':'ВЫВОД по данным сайта: 1–4','evidence':'fixture'}])
        self.task()
        payload = self.store.task(1)['payload']
        payload['planned']['fields']['programs'] = 'ВЫВОД по описанию'
        payload['evidence'][0]['value'] = 'ВЫВОД по описанию'
        self.store.db.execute('UPDATE tasks SET payload=? WHERE id=1',(json.dumps(payload),))
        with self.assertRaises(ValueError):
            write_one(self.store,self.claim(),self.backend,False)
        self.assertEqual(self.backend.read(900000),self.before)

    def test_full_card_facts_survive_shorter_description(self):
        snapshot = copy.deepcopy(self.before)
        snapshot['educational_status'] = 'начальная школа (1-4 классы)'
        snapshot['raw_card'] = {'educational_status':'начальная школа (1-4 классы)','text':'Продлёнка','registry_fields':[{'definition_id':48,'value':'1–4 классы'}]}
        planned,reviews = plan(snapshot,FIXTURE['candidates'])
        self.assertEqual(planned['educational_status'],snapshot['educational_status'])
        self.assertEqual(planned['raw_card'],snapshot['raw_card'])
        self.assertEqual(reviews,[])

    def test_metrics_do_not_count_replay_as_change_or_excel_merge(self):
        self.task()
        write_one(self.store,self.claim(),self.backend,False)
        before = self.store.metrics()
        self.task()  # duplicate enqueue
        self.assertEqual(self.store.metrics(),before)
        self.assertEqual(before['changed'],1)
        self.assertEqual(before['readback'],1)
        self.assertEqual(before['public'],0)
        self.assertEqual(before['excel_merged'],0)
        self.assertEqual(before['qa'],0)

    def test_readback_denial_preserves_global_gate(self):
        self.task()
        with self.assertRaises(SimulatedCrash):
            write_one(self.store,self.claim(),self.backend,False,'after_send')
        self.now += 11
        with patch.object(self.backend,'readback',side_effect=AccessHeld('readback','900000','security warning')):
            recover_one(self.store,self.store.claim_recovery('recovery'),self.backend)
        self.assertTrue(self.store.held('readback','900000'))
        self.assertEqual(self.store.claim('write','new'),[])
        self.assertIsNone(self.store.claim_recovery('another'))

    def test_append_only_journal(self):
        self.task()
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.db.execute('DELETE FROM journal')


if __name__ == '__main__':
    unittest.main()
