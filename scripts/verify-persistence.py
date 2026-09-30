"""Recreate only Dunnelean FE/BE; verify accounts, data and configuration survive."""
import hashlib
import json
import subprocess
import time
import integration as t

def snapshot():
    fe=t.sql('SHOW FRONTENDS',doris=True)
    be=t.sql('SHOW BACKENDS',doris=True)
    configs={}
    for service in ['fe','be']:
        body=subprocess.check_output(['docker','exec',f'dunnelean-{service}-1','cat',f'/opt/apache-doris/{service}/conf/{service}.conf'])
        effective=b'\n'.join(sorted(line.strip() for line in body.splitlines() if line.strip() and not line.lstrip().startswith(b'#')))
        configs[service]=hashlib.sha256(effective).hexdigest()
    return {'rows':t.sql('SELECT COUNT(*),SUM(id),SUM(amount) FROM bench_target',doris=True), 'config_sha256':configs, 'frontends':fe, 'backends':be}

before=snapshot()
subprocess.run(['docker','compose','--env-file',str(t.ROOT/'deploy/.env'),'-f',str(t.ROOT/'deploy/compose.yml'),'up','-d','--force-recreate','--wait','--wait-timeout','300','fe','be'],check=True)
for attempt in range(60):
    try:
        after=snapshot()
        assert after['rows']==before['rows'],(before['rows'],after['rows'])
        assert after['config_sha256']==before['config_sha256']
        break
    except Exception:
        if attempt==59:raise
        time.sleep(2)
report={'case':'fe_be_recreate_preserves_data_account_config','passed':True,'before':before,'after':after}
(t.ROOT/'docs/acceptance-environment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
print('Container recreation verified: business account, data and config preserved.',flush=True)
