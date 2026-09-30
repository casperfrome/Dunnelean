"""Extended real-DB acceptance; run after integration.py in the isolated test environment."""
import copy
import json
import os
import sqlite3
import subprocess
import time
import uuid
import threading
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import requests
import integration as t

class DelayedReceipt:
    """Commit to the actual Doris BE, then hold its receipt until cancellation is requested."""
    def __init__(self,drop=False):self.drop=drop;self.requests=0
    def __enter__(self):
        self.committed=threading.Event();self.release=threading.Event();owner=self
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self,*_):pass
            def do_GET(self):
                body=b'{"status":"OK"}'
                self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
            def do_PUT(self):
                owner.requests+=1
                body=self.rfile.read(int(self.headers['Content-Length']))
                headers={k:v for k,v in self.headers.items() if k.lower() not in ('host','connection','expect','content-length')}
                with requests.Session() as session:
                    session.trust_env=False
                    response=session.put('http://127.0.0.1:8040'+self.path,headers=headers,data=body,timeout=30)
                owner.committed.set()
                if owner.drop:
                    self.close_connection=True
                    self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
                owner.release.wait(15)
                self.send_response(response.status_code);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(response.content)));self.send_header('Connection','close');self.end_headers();self.wfile.write(response.content)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.url=f'http://127.0.0.1:{self.server.server_port}'
        threading.Thread(target=self.server.serve_forever,daemon=True).start();return self
    def __exit__(self,*_):
        self.release.set();self.server.shutdown();self.server.server_close()

binary=t.ROOT/'target/release/dunnelean.exe'
process,log=t.start(binary)
try:
    t.sql('DROP TABLE IF EXISTS partition_source')
    t.sql('CREATE TABLE partition_source(id INT PRIMARY KEY, part INT) ENGINE=InnoDB')
    t.sql('INSERT INTO partition_source VALUES(1,NULL),(2,-100),(3,0),(4,10),(5,100),(6,1000)')
    t.sql('DROP TABLE IF EXISTS partition_target',doris=True)
    t.sql('CREATE TABLE partition_target(id INT NOT NULL, part INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")',doris=True)
    split=t.job('mysql-to-doris');split['reader']['source']={'table':'partition_source'};split['reader']['split']={'column':'part','lower_bound':'0','upper_bound':'20','partitions':4,'parallelism':2};split['writer']['table']='partition_target'
    assert t.execute(split,'nullable_integer_partitions_cover_all_tails',process)['rows_committed']==6
    assert t.sql('SELECT * FROM partition_source ORDER BY id')==t.sql('SELECT * FROM partition_target ORDER BY id',doris=True)
    # Small decimal precisions and DATE exercise native Doris Arrow physical types.
    t.sql('DROP TABLE IF EXISTS scalar_source')
    t.sql('CREATE TABLE scalar_source(id BIGINT PRIMARY KEY, small DECIMAL(9,2), medium DECIMAL(18,4), day DATE, duration TIME(6), raw VARBINARY(64)) ENGINE=InnoDB')
    t.sql("INSERT INTO scalar_source VALUES(1,-1234567.89,12345678901234.1234,'2026-09-29','-25:00:00.000001',X'FF00'),(2,NULL,NULL,NULL,NULL,NULL)")
    t.sql('DROP TABLE IF EXISTS scalar_target',doris=True)
    t.sql('CREATE TABLE scalar_target(id BIGINT NOT NULL, small DECIMAL(9,2), medium DECIMAL(18,4), day DATE, duration BIGINT, raw STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")',doris=True)
    s=t.job('mysql-to-doris');s['reader']['source']={'table':'scalar_source'};s['writer']['table']='scalar_target'
    t.request('POST','/v1/validate',s,400)  # Neither binary nor signed duration is implicitly stringified.
    s['reader']['source']={'query':"SELECT id,small,medium,day,CAST(TIME_TO_SEC(duration)*1000000 + IF(LEFT(CAST(duration AS CHAR),1)='-',-1,1)*MICROSECOND(duration) AS SIGNED) AS duration,HEX(raw) AS raw FROM scalar_source"}
    t.execute(s,'small_decimals_date_explicit_negative_time_binary',process)
    assert t.sql('SELECT duration,raw FROM scalar_target WHERE id=1',doris=True)==((-90000000001,'FF00'),)
    t.sql('DROP TABLE IF EXISTS scalar_returned')
    t.sql('CREATE TABLE scalar_returned(id BIGINT PRIMARY KEY, small DECIMAL(9,2), medium DECIMAL(18,4), day DATE, duration BIGINT, raw VARCHAR(128)) ENGINE=InnoDB')
    b=t.job('doris-to-mysql');b['reader']['source']={'table':'scalar_target'};b['writer']['table']='scalar_returned'
    t.execute(b,'small_scalar_roundtrip',process)
    assert t.sql(s['reader']['source']['query']+' ORDER BY id')==t.sql('SELECT * FROM scalar_returned ORDER BY id')
    # Reduced decimal scale may never silently round nonzero digits.
    bad=copy.deepcopy(s);bad['request_id']=str(uuid.uuid4());bad['reader']['source']={'query':'SELECT id, CAST(1.001 AS DECIMAL(9,3)) AS small, medium,day,CAST(0 AS SIGNED) AS duration,HEX(raw) AS raw FROM scalar_source'}
    assert t.execute(bad,'decimal_scale_loss_rejected',process,expect='FAILED')['rows_committed']==0
    # Zero dates must fail, not turn into NULL.
    with t.db() as c,c.cursor() as cur:
        cur.execute("SET sql_mode=''")
        cur.execute("UPDATE scalar_source SET day='0000-00-00' WHERE id=1")
    zero=copy.deepcopy(s);zero['request_id']=str(uuid.uuid4())
    assert t.execute(zero,'zero_date_rejected',process,expect='FAILED')['rows_committed']==0
    # Interrupt a real active read query; an aborted query must not look successful.
    read=t.job('mysql-to-doris');read['reader']['source']={'query':'SELECT id,amount,payload FROM bench_source WHERE id < 1000 AND SLEEP(0.01)=0 /* dunnelean_read_fault */'}
    read['reader']['batch']={'rows':10,'bytes':65536};read['writer']['table']='bench_target';read['writer']['options']={'batch':{'rows':10,'bytes':65536}}
    r=t.request('POST','/v1/runs',read,202);deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        connections=t.sql('SHOW PROCESSLIST')
        ids=[row[0] for row in connections if row[7] and 'dunnelean_read_fault' in row[7]]
        if ids:
            t.sql(f'KILL CONNECTION {int(ids[0])}');break
        time.sleep(.05)
    else:raise AssertionError('Reading connection not found')
    r,_=t.wait(r['run_id']);assert r['state']=='FAILED',r
    t.REPORT.append({'case':'read_connection_interruption','state':r['state'],'rows':r['rows_committed']})
    with DelayedReceipt(drop=True) as proxy:
        lost=t.job('mysql-to-doris');lost['reader']['source']['where']='id < 2';lost['writer']['be_http_urls']=[proxy.url]
        r=t.execute(lost,'doris_real_committed_response_loss',process)
        assert r['rows_committed']==2 and proxy.requests==1 and not r['commit_unknown'],r
    with DelayedReceipt() as proxy:
        racing=t.job('mysql-to-doris');racing['reader']['source']['where']='id < 2';racing['writer']['be_http_urls']=[proxy.url]
        run=t.request('POST','/v1/runs',racing,202)
        assert proxy.committed.wait(30),'Doris did not respond through receipt-delay proxy'
        r=t.request('POST','/v1/runs/'+run['run_id']+'/cancel')
        assert r['state']=='CANCELLING',r
        time.sleep(.2)
        assert t.request('GET','/v1/runs/'+run['run_id'])['state']=='CANCELLING'
        proxy.release.set();r,_=t.wait(run['run_id'])
        assert r['state']=='CANCELLED' and r['rows_committed']==2 and r['partial_write'] and not r['commit_unknown'],r
        t.REPORT.append({'case':'cancel_during_real_committed_load_waits_for_receipt','state':r['state'],'rows':r['rows_committed']})
    # SQLite refuses the receipt after the target acknowledged a real commit.
    state=sqlite3.connect(t.ROOT/'var/integration.sqlite')
    state.execute("CREATE TRIGGER inject_receipt_failure BEFORE UPDATE ON runs WHEN json_extract(NEW.data,'$.rows_committed') > json_extract(OLD.data,'$.rows_committed') BEGIN SELECT RAISE(FAIL,'injected receipt persistence failure'); END")
    state.commit()
    try:
        lost=t.job('mysql-to-doris');lost['reader']['source']['where']='id < 3'
        r=t.execute(lost,'sqlite_receipt_failure_after_real_commit',process,expect='FAILED')
        assert r['commit_unknown'] and r['rows_committed']==0,r
    finally:
        state.execute('DROP TRIGGER inject_receipt_failure');state.commit();state.close()
    # Standard Java HttpClient really submits, polls, deduplicates and cancels.
    classes=t.ROOT/'var/java';classes.mkdir(exist_ok=True)
    subprocess.run(['javac','-encoding','UTF-8','-d',str(classes),str(t.ROOT/'examples/java/DunneleanClient.java')],check=True)
    env=os.environ.copy();env['DUNNELEAN_URL']=t.BASE
    for option in ['--deduplicate','--cancel']:
        spec=t.job('mysql-to-doris');spec['reader']['source']['where']='id < 10'
        if option=='--cancel':spec['execution']={'rows_per_second':1}
        path=t.ROOT/'var/java-job.json';path.write_text(json.dumps(spec),encoding='utf-8')
        result=subprocess.run(['java','-cp',str(classes),'DunneleanClient',str(path),option],env=env,text=True,encoding='utf-8',capture_output=True,timeout=60)
        assert result.returncode==0,result.stdout+result.stderr
        (t.ROOT/f'var/java{option}.log').write_text(result.stdout,encoding='utf-8')
        t.REPORT.append({'case':'java_'+option[2:],'state':'PASSED'})
    # The separate Maven consumer uses the locally installed typed SDK, not SDK source files.
    subprocess.run(['mvn.cmd' if os.name=='nt' else 'mvn','-B','-ntp','-f',str(t.ROOT/'sdk/java/pom.xml'),'install'],check=True)
    subprocess.run(['mvn.cmd' if os.name=='nt' else 'mvn','-B','-ntp','-f',str(t.ROOT/'examples/java/sdk/pom.xml'),'package'],check=True)
    sdk_classes=t.ROOT/'examples/java/sdk/target/classes'
    sdk_deps=t.ROOT/'examples/java/sdk/target/dependency/*'
    sdk_classpath=str(sdk_classes)+os.pathsep+str(sdk_deps)
    # Use fresh SDK-owned targets: previous acceptance cases have already filled target_orders/returned_orders.
    t.sql('DROP TABLE IF EXISTS java_sdk_target',doris=True)
    t.sql('''CREATE TABLE java_sdk_target (
        id DECIMAL(20,0) NOT NULL, amount DECIMAL(30,10), precise_value DECIMAL(65,5),
        Name VARCHAR(512), event_time DATETIME(6), event_instant DATETIME(6), enabled TINYINT, notes STRING
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")''',doris=True)
    t.sql('DROP TABLE IF EXISTS java_sdk_returned')
    t.sql('CREATE TABLE java_sdk_returned LIKE returned_orders')
    for direction,option in [('mysql-to-doris','--deduplicate'),('doris-to-mysql','--async'),('mysql-to-doris','--cancel')]:
        spec=t.job(direction)
        spec['reader']['source']['where']='id < 10'
        spec['writer']['table']='java_sdk_target' if direction=='mysql-to-doris' else 'java_sdk_returned'
        if direction=='doris-to-mysql':spec['reader']['source']['table']='java_sdk_target'
        if option=='--cancel':spec['execution']={'rows_per_second':1}
        path=t.ROOT/'var/java-sdk-job.json';path.write_text(json.dumps(spec),encoding='utf-8')
        result=subprocess.run(['java','-cp',sdk_classpath,'io.github.casperfrome.dunnelean.example.SdkExample',str(path),option],env=env,text=True,encoding='utf-8',capture_output=True,timeout=90)
        assert result.returncode==0,result.stdout+result.stderr
        (t.ROOT/f'var/java-sdk-{direction}{option}.log').write_text(result.stdout,encoding='utf-8')
        t.REPORT.append({'case':'java_sdk_'+direction+'_'+option[2:],'state':'PASSED'})
        if direction=='doris-to-mysql':
            assert t.sql('SELECT * FROM source_orders WHERE id < 10 ORDER BY id')==t.sql('SELECT * FROM java_sdk_returned ORDER BY id'),'SDK roundtrip mismatch'
    # Force-kill the actual process, retain progress and prevent automatic resume.
    slow=t.job('mysql-to-doris');slow['reader']['source']={'table':'bench_source','where':'id < 10000'};slow['writer']['table']='bench_target'
    slow['reader']['batch']={'rows':5,'bytes':65536};slow['writer']['options']={'batch':{'rows':5,'bytes':65536}};slow['execution']={'rows_per_second':25,'queue_capacity':1}
    r=t.request('POST','/v1/runs',slow,202)
    for _ in range(100):
        before=t.request('GET','/v1/runs/'+r['run_id'])
        if before['rows_committed']>0:break
        time.sleep(.05)
    else:raise AssertionError('No batch committed before crash')
    process.kill();process.wait(timeout=15);log.close()
    process,log=t.start(binary)
    after=t.request('GET','/v1/runs/'+r['run_id'])
    assert after['state']=='INTERRUPTED' and after['rows_committed']>=before['rows_committed'] and after['partial_write'],after
    assert t.request('POST','/v1/runs',slow,202)['run_id']==r['run_id']
    t.REPORT.append({'case':'process_kill_restart_no_resume','state':after['state'],'rows':after['rows_committed']})
    (t.ROOT/'docs/acceptance-extended.json').write_text(json.dumps(t.REPORT,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Extended acceptance passed.',flush=True)
finally:
    process.terminate();process.wait(timeout=15);log.close()
