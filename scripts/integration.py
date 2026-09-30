"""Real isolated MySQL 9.7.2 / Doris 4.1.4 end-to-end acceptance.

Use the project .venv Python environment. Only dunnelean_test tables are changed.
"""
import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
import socket
import threading
from decimal import Decimal

import pymysql
import requests
import psutil

ROOT = Path(__file__).resolve().parents[1]
ENV = dict(line.split('=', 1) for line in (ROOT / 'deploy/.env').read_text(encoding='utf-8').splitlines() if line and not line.startswith('#'))
PASSWORD = ENV['DUNNELEAN_TEST_PASSWORD']
BASE = 'http://127.0.0.1:9877'
HTTP = requests.Session()
HTTP.trust_env = False
REPORT = []

def db(doris=False):
    c = pymysql.connect(host='127.0.0.1', port=9030 if doris else 3308, user='dunnelean', password=PASSWORD, database='dunnelean_test', charset='utf8mb4', autocommit=True)
    with c.cursor() as cur:
        cur.execute("SET time_zone='+00:00'")
        if doris:
            cur.execute('SET enable_decimal256=true')
    return c

def sql(statement, args=None, doris=False):
    with db(doris) as c, c.cursor() as cur:
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else cur.rowcount

def job(name):
    spec = json.loads((ROOT / 'examples' / f'{name}.json').read_text(encoding='utf-8'))
    spec['request_id'] = str(uuid.uuid4())
    return spec

def request(method, path, payload=None, expected=200):
    r = HTTP.request(method, BASE + path, json=payload, timeout=150)
    if r.status_code != expected:
        raise AssertionError(f'{method} {path}: {r.status_code}: {r.text[:5000]}')
    return r.json()

def wait(run_id, limit=1800, process=None):
    start = time.monotonic()
    peak = 0
    while time.monotonic() - start < limit:
        run = request('GET', '/v1/runs/' + run_id)
        if process:
            peak = max(peak, psutil.Process(process.pid).memory_info().rss)
        if run['state'] in ('SUCCEEDED', 'FAILED', 'CANCELLED', 'INTERRUPTED'):
            return run, peak
        time.sleep(0.15)
    raise AssertionError('Run did not reach terminal state')

def execute(spec, name, process=None, expect='SUCCEEDED'):
    request('POST', '/v1/validate', spec)
    start = time.monotonic()
    run = request('POST', '/v1/runs', spec, 202)
    run, peak = wait(run['run_id'], process=process)
    assert run['state'] == expect, json.dumps(run, ensure_ascii=False)
    result = {'case': name, 'state': run['state'], 'rows': run['rows_committed'], 'seconds': round(time.monotonic()-start, 3), 'peak_rss_mib': round(peak/1024**2, 1), 'run_id': run['run_id']}
    REPORT.append(result)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return run

def create_fixtures():
    for name in ('source_orders', 'returned_orders'):
        sql(f'DROP TABLE IF EXISTS `{name}`')
        sql(f'''CREATE TABLE `{name}` (
            id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            amount DECIMAL(30,8), precise_value DECIMAL(65,5),
            Name VARCHAR(512), event_time DATETIME(6), event_instant TIMESTAMP(6) NULL,
            enabled TINYINT, notes TEXT
        ) ENGINE=InnoDB''')
    sql('DROP TABLE IF EXISTS target_orders', doris=True)
    sql('''CREATE TABLE target_orders (
        id DECIMAL(20,0) NOT NULL,
        amount DECIMAL(30,10), precise_value DECIMAL(65,5),
        Name VARCHAR(512), event_time DATETIME(6), event_instant DATETIME(6),
        enabled TINYINT, notes STRING
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")''', doris=True)
    rows=[]
    for i in range(47):
        rows.append((2**64-1 if i==46 else i,
                     Decimal('-12345678901234567890.12345678') if i%3 else None,
                     Decimal('123456789012345678901234567890123456789012345678901234567890.12345'),
                     None if i%4==0 else '中文🚀\n"quote"\\'+str(i),
                     '2026-09-29 12:34:56.123456', '2026-09-29 04:34:56.654321', i%2,
                     '' if i%2 else 'line1\nline2\tNULL\\N'))
    with db() as c, c.cursor() as cur:
        cur.executemany('INSERT INTO source_orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',rows)

def functional(process):
    create_fixtures()
    forward=job('mysql-to-doris')
    forward['reader']['batch']={'rows':7,'bytes':65536}
    forward['writer']['options']={'batch':{'rows':5,'bytes':65536}}
    first=execute(forward,'mysql_to_doris_exact_types',process)
    assert first['rows_committed']==47 and first['batches_committed']>1
    duplicate=request('POST','/v1/runs',forward,202)
    assert duplicate['run_id']==first['run_id']
    conflicting=copy.deepcopy(forward);conflicting['writer']['table']='another'
    request('POST','/v1/runs',conflicting,409)
    reverse=job('doris-to-mysql')
    reverse['writer']['options']={'batch':{'rows':6,'bytes':65536}}
    back=execute(reverse,'doris_to_mysql_exact_types',process)
    assert back['rows_committed']==47
    assert sql('SELECT * FROM source_orders ORDER BY id')==sql('SELECT * FROM returned_orders ORDER BY id'), 'Roundtrip mismatch'
    reverse['request_id']=str(uuid.uuid4());reverse['writer']['mode']='upsert';reverse['writer']['key_columns']=['id']
    again=execute(reverse,'mysql_upsert_counts',process)
    assert again['rows_committed']==47 and again['server_affected_rows']==0
    # Empty result still carries source schema and executes hooks once.
    empty=job('mysql-to-doris');empty['reader']['source']['where']='id < 0'
    assert execute(empty,'empty_result',process)['rows_committed']==0
    renamed=job('mysql-to-doris');renamed['reader']['source']={'query':'SELECT Name AS old_name,id AS old_id FROM source_orders WHERE id < 2'}
    renamed['mapping']=[{'source':'old_id','target':'id'},{'source':'old_name','target':'Name'}]
    assert execute(renamed,'column_selection_rename',process)['rows_committed']==2
    # Reordered mapping and custom SQL with typed bind parameters.
    filtered=job('mysql-to-doris')
    filtered['reader']['source']={'query':'SELECT notes,enabled,event_instant,event_time,Name,precise_value,amount,id FROM source_orders WHERE id >= ? AND id < ?', 'params':[{'type':'u64','value':'10'},{'type':'u64','value':'20'}]}
    filtered['mapping']=[{'source':c,'target':c} for c in ['id','amount','precise_value','Name','event_time','event_instant','enabled','notes']]
    filtered['writer']['options']={'pre_sql':['TRUNCATE TABLE target_orders'],'post_sql':['SELECT COUNT(*) FROM target_orders']}
    assert execute(filtered,'mapping_filter_hooks',process)['rows_committed']==10
    # Integer splitting includes keys below/above its stride bounds and unsigned max.
    split=job('mysql-to-doris');split['reader']['split']={'column':'id','lower_bound':'10','upper_bound':'30','partitions':4,'parallelism':2}
    split['writer']['options']={'pre_sql':['TRUNCATE TABLE target_orders'],'parallelism':2}
    assert execute(split,'parallel_range_split_tails',process)['rows_committed']==47
    # Cancellation with backpressure/rate limiting.
    slow=job('mysql-to-doris');slow['execution']={'rows_per_second':1}
    r=request('POST','/v1/runs',slow,202);time.sleep(0.4)
    request('POST',f"/v1/runs/{r['run_id']}/cancel")
    r,_=wait(r['run_id']);assert r['state']=='CANCELLED',r
    REPORT.append({'case':'cancel_rate_limited_run','state':r['state'],'rows':r['rows_committed']})
    # Read/query failure is a visible failure, never a successful zero-row run.
    bad=job('mysql-to-doris');bad['reader']['source']={'query':'SELECT missing_column FROM source_orders'}
    r=request('POST','/v1/runs',bad,202);r,_=wait(r['run_id']);assert r['state']=='FAILED' and r['rows_committed']==0,r
    # No plaintext password is stored in the API response.
    inline=job('mysql-to-doris');inline['reader']['source']['where']='id < 0'
    credentials=inline['reader']['connection']['credentials'];credentials.pop('password_env');credentials['password']=PASSWORD
    r=execute(inline,'redacted_inline_credential',process)
    assert PASSWORD not in json.dumps(r)
    # Doris full-row UNIQUE KEY upsert, native BOOLEAN and LARGEINT wire handling.
    sql('DROP TABLE IF EXISTS special_source')
    sql('CREATE TABLE special_source(id BIGINT UNSIGNED PRIMARY KEY, enabled TINYINT NOT NULL) ENGINE=InnoDB')
    sql('INSERT INTO special_source VALUES(0,0),(18446744073709551615,1)')
    sql('DROP TABLE IF EXISTS special_target',doris=True)
    sql('CREATE TABLE special_target(id LARGEINT NOT NULL, enabled BOOLEAN NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1","enable_unique_key_merge_on_write"="true")',doris=True)
    special=job('mysql-to-doris');special['reader']['source']={'table':'special_source'};special['writer']['table']='special_target';special['writer']['mode']='upsert'
    execute(special,'doris_upsert_largeint_boolean',process)
    sql('UPDATE special_source SET enabled=1-enabled')
    special['request_id']=str(uuid.uuid4());execute(special,'doris_upsert_repeat',process)
    sql('DROP TABLE IF EXISTS special_returned');sql('CREATE TABLE special_returned LIKE special_source')
    back=job('doris-to-mysql');back['reader']['source']={'table':'special_target'};back['writer']['table']='special_returned'
    execute(back,'largeint_boolean_roundtrip',process)
    assert sql('SELECT * FROM special_source ORDER BY id')==sql('SELECT * FROM special_returned ORDER BY id')
    # Same run: post-SQL failure must retain confirmed writes and fail at post_sql.
    failed_post=job('mysql-to-doris');failed_post['reader']['source']['where']='id < 3'
    failed_post['writer']['options']={'post_sql':['SELECT missing_column FROM target_orders']}
    r=execute(failed_post,'post_sql_failure_preserves_commits',process,expect='FAILED')
    assert r['stage']=='post_sql' and r['rows_committed']==3 and r['partial_write']
    # MySQL receives and commits COMMIT, but its OK packet never reaches Dunnelean.
    sql('TRUNCATE TABLE special_returned')
    with CommitResponseLossProxy() as proxy:
        lost=copy.deepcopy(back);lost['request_id']=str(uuid.uuid4());lost['writer']['connection']['port']=proxy.port
        r=execute(lost,'mysql_commit_response_loss',process,expect='FAILED')
        assert proxy.commits==1 and r['commit_unknown'] and r['rows_committed']==0,r
        assert sql('SELECT COUNT(*) FROM special_returned')[0][0]==2

class CommitResponseLossProxy:
    """MySQL packet relay dropping exactly the server reply to COM_QUERY COMMIT."""
    def __enter__(self):
        self.listener=socket.socket();self.listener.bind(('127.0.0.1',0));self.listener.listen()
        self.port=self.listener.getsockname()[1];self.commits=0;self.closed=False
        threading.Thread(target=self.accept,daemon=True).start();return self
    @staticmethod
    def packet(sock):
        def exact(n):
            out=b''
            while len(out)<n:
                part=sock.recv(n-len(out))
                if not part:raise EOFError()
                out+=part
            return out
        head=exact(4);return head+exact(int.from_bytes(head[:3],'little'))
    def accept(self):
        while not self.closed:
            try:
                client,_=self.listener.accept();upstream=socket.create_connection(('127.0.0.1',3308));drop=threading.Event()
                def forward(src,dst,client_side,drop=drop,client=client,upstream=upstream):
                    try:
                        while True:
                            packet=self.packet(src)
                            if client_side and packet[4:5]==b'\x03' and packet[5:].strip().upper()==b'COMMIT':
                                self.commits+=1;drop.set()
                            if not client_side and drop.is_set():break
                            dst.sendall(packet)
                    except (OSError,EOFError):pass
                    finally:
                        for conn in (client,upstream):
                            try:conn.shutdown(socket.SHUT_RDWR)
                            except OSError:pass
                            conn.close()
                threading.Thread(target=forward,args=(client,upstream,True),daemon=True).start()
                threading.Thread(target=forward,args=(upstream,client,False),daemon=True).start()
            except OSError:
                if not self.closed:raise
    def __exit__(self,*_):
        self.closed=True;self.listener.close()

def scale(rows,process):
    # Fixed-width, repeatable fixture generation inside MySQL, not Python memory.
    sql('DROP TABLE IF EXISTS bench_source')
    sql('CREATE TABLE bench_source (id BIGINT NOT NULL PRIMARY KEY, amount DECIMAL(20,4), payload VARCHAR(80)) ENGINE=InnoDB')
    sql('DROP TABLE IF EXISTS bench_returned')
    sql('CREATE TABLE bench_returned LIKE bench_source')
    sql('DROP TABLE IF EXISTS bench_target',doris=True)
    sql('CREATE TABLE bench_target (id BIGINT NOT NULL, amount DECIMAL(20,4), payload VARCHAR(80)) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES("replication_num"="1")',doris=True)
    digits='(SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)'
    joined=' CROSS JOIN '.join(f'{digits} d{i}' for i in range(6))
    number='+'.join(f'd{i}.n*{10**i}' for i in range(6))
    for start in range(0,rows,1_000_000):
        sql(f"INSERT INTO bench_source SELECT {number}+{start}, ({number}+{start})/10000, CONCAT('Dunnelean-', {number}+{start}) FROM {joined} WHERE {number} < {min(1_000_000,rows-start)}")
    f=job('mysql-to-doris');f['reader']['source']={'table':'bench_source'};f['writer']['table']='bench_target'
    r=execute(f,f'scale_{rows}_mysql_to_doris',process);assert r['rows_committed']==rows
    b=job('doris-to-mysql');b['reader']['source']={'table':'bench_target'};b['writer']['table']='bench_returned'
    r=execute(b,f'scale_{rows}_doris_to_mysql',process);assert r['rows_committed']==rows
    assert sql('SELECT COUNT(*),SUM(id),SUM(amount),SUM(LENGTH(payload)) FROM bench_source')==sql('SELECT COUNT(*),SUM(id),SUM(amount),SUM(LENGTH(payload)) FROM bench_returned')

def start(binary):
    (ROOT/'var').mkdir(exist_ok=True)
    config=ROOT/'var/integration.toml'
    config.write_text('listen="127.0.0.1:9877"\nstate_path="var/integration.sqlite"\nmax_running=2\nmax_queued=16\n',encoding='utf-8')
    env=os.environ.copy();env['DUNNELEAN_TEST_PASSWORD']=PASSWORD
    log=(ROOT/'var/integration-server.log').open('ab')
    p=subprocess.Popen([str(binary),'serve','--config',str(config)],cwd=ROOT,env=env,stdout=log,stderr=log,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    for _ in range(100):
        if p.poll() is not None:raise RuntimeError((ROOT/'var/integration-server.log').read_text(encoding='utf-8')[-5000:])
        try:
            if request('GET','/readyz'):return p,log
        except requests.RequestException:pass
        time.sleep(.1)
    raise RuntimeError('Service startup timeout')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--rows',type=int,default=0);parser.add_argument('--only-scale',action='store_true');parser.add_argument('--binary',default='target/debug/dunnelean.exe')
    args=parser.parse_args();process,log=start(ROOT/args.binary)
    try:
        if not args.only_scale:functional(process)
        if args.rows:scale(args.rows,process)
        (ROOT/'var/acceptance.json').write_text(json.dumps(REPORT,ensure_ascii=False,indent=2),encoding='utf-8')
        print('All requested acceptance cases passed.',flush=True)
    finally:
        process.terminate();process.wait(timeout=15);log.close()
