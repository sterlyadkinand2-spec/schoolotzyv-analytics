"""Local fixtures only: never calls schoolotzyv or replaces a Library file."""
import json
import sqlite3
from pathlib import Path
from .store import Conflict, encode


class FixtureDatabase:
    mode = 'fixture'

    def __init__(self,path):
        self.db = sqlite3.connect(path,isolation_level=None,timeout=30)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''CREATE TABLE IF NOT EXISTS cards(id INTEGER PRIMARY KEY, snapshot TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS receipts(key TEXT PRIMARY KEY, id INTEGER NOT NULL, snapshot TEXT NOT NULL);''')

    def seed(self,snapshot):
        self.db.execute('INSERT OR IGNORE INTO cards VALUES(?,?)',(snapshot['id'],encode(snapshot)))

    def read(self,entity_id):
        return json.loads(self.db.execute('SELECT snapshot FROM cards WHERE id=?',(entity_id,)).fetchone()[0])

    def compare_and_set(self,entity_id,before,planned,key):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            receipt = self.db.execute('SELECT id,snapshot FROM receipts WHERE key=?',(key,)).fetchone()
            if receipt:
                if receipt != (entity_id,encode(planned)):
                    raise Conflict('operation key reused')
                result = {'operation_key':key,'deduplicated':True}
            else:
                if self.read(entity_id) != before:
                    raise Conflict('concurrent edit rejected atomically')
                self.db.execute('UPDATE cards SET snapshot=? WHERE id=?',(encode(planned),entity_id))
                self.db.execute('INSERT INTO receipts VALUES(?,?,?)',(key,entity_id,encode(planned)))
                result = {'operation_key':key,'deduplicated':False}
            self.db.execute('COMMIT')
            return result
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def readback(self,entity_id,key):
        row = self.db.execute('SELECT id,snapshot FROM receipts WHERE key=?',(key,)).fetchone()
        return {'operation_key':key,'applied':bool(row and row[0] == entity_id),'snapshot':self.read(entity_id)}

    def close(self):
        self.db.close()


def export_local_xlsx(store,lease,directory):
    """New local per-operation artifact. Does not touch the single master workbook."""
    import os
    import tempfile
    import zipfile
    from xml.sax.saxutils import escape
    from .store import digest
    store.check(lease)
    snapshot = lease['payload']['snapshot']
    directory = Path(directory)
    directory.mkdir(parents=True,exist_ok=True)
    path = directory / ('staging-' + digest(lease['payload']) + '.xlsx')
    rows = [['id','urls','field','value']]
    rows += [[str(snapshot['id']),encode(snapshot['urls']),k,encode(v)] for k,v in snapshot['fields'].items()]
    sheet = '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
    for i,row in enumerate(rows,1):
        sheet += f'<row r="{i}">'
        for j,value in enumerate(row):
            # inlineStr prevents formulas in externally sourced values from executing.
            sheet += f'<c r="{chr(65+j)}{i}" t="inlineStr"><is><t xml:space="preserve">{escape(value)}</t></is></c>'
        sheet += '</row>'
    sheet += '</sheetData></worksheet>'
    fd,temp = tempfile.mkstemp(prefix='.export-',suffix='.xlsx',dir=directory)
    os.close(fd)
    try:
        with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED) as z:
            z.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
            z.writestr('_rels/.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
            z.writestr('xl/workbook.xml','<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Staging" sheetId="1" r:id="rId1"/></sheets></workbook>')
            z.writestr('xl/_rels/workbook.xml.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
            z.writestr('xl/worksheets/sheet1.xml',sheet)
        with open(temp,'rb') as f:
            os.fsync(f.fileno())
        store.check(lease)
        os.replace(temp,path)
        directory_fd = os.open(directory,os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)
    store.finish(lease,reason='local staging artifact: '+str(path))
    return path
