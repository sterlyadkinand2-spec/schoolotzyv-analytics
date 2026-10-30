import argparse
import json
from pathlib import Path
from .store import Store
from .engine import drain, research_one, write_one
from .adapters import FixtureDatabase, export_local_xlsx
from .handover import import_manifest


def main():
    parser = argparse.ArgumentParser(description='Moscow staging queue. No production transport.')
    sub = parser.add_subparsers(dest='command',required=True)
    demo = sub.add_parser('demo',help='isolated synthetic fixture pipeline')
    demo.add_argument('--directory',required=True)
    demo.add_argument('--simulate-write',action='store_true',help='write ONLY local fixture backend; default dry-run')
    status = sub.add_parser('status')
    status.add_argument('--db',required=True)
    imp = sub.add_parser('import-staging')
    imp.add_argument('--db',required=True)
    imp.add_argument('--manifest',required=True)
    args = parser.parse_args()
    if args.command in ('status','import-staging'):
        store = Store(args.db)
        try:
            if args.command == 'import-staging':
                print(json.dumps({'imported':import_manifest(store,json.loads(Path(args.manifest).read_text()))}))
            print(json.dumps(store.summary(),ensure_ascii=False,indent=2))
        finally:
            store.close()
        return
    directory = Path(args.directory)
    directory.mkdir(parents=True,exist_ok=True)
    fixture = json.loads((Path(__file__).resolve().parent.parent/'fixtures/example.json').read_text())
    store = Store(directory/'queue.sqlite')
    backend = FixtureDatabase(directory/'fixture.sqlite')
    try:
        snapshot = fixture['snapshot']
        backend.seed(snapshot)
        store.enqueue('research','fixture-example-v1',snapshot['id'],{'snapshot':snapshot,'sources':fixture['sources']})
        for shard in range(3):
            drain(store,'research',f'research-{shard}',lambda s,t:research_one(s,t,lambda snapshot,source:fixture['candidates']),shard)
        drain(store,'write','single-coordinator',lambda s,t:write_one(s,t,backend,dry_run=not args.simulate_write))
        drain(store,'export','local-export',lambda s,t:export_local_xlsx(s,t,directory/'exports'))
        print(json.dumps({'mode':'fixture-only','summary':store.summary(),'qa':'left ready for independent semantic review','production_writes':0},ensure_ascii=False,indent=2))
    finally:
        backend.close()
        store.close()


if __name__ == '__main__':
    main()
