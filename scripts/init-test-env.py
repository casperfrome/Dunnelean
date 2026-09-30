"""Initialize only the isolated Dunnelean test database and account."""
from pathlib import Path
import pymysql

ROOT = Path(__file__).resolve().parents[1]

def settings():
    return dict(line.split('=', 1) for line in (ROOT / 'deploy/.env').read_text(encoding='utf-8').splitlines() if line and not line.startswith('#'))

def initialize():
    password = settings()['DUNNELEAN_TEST_PASSWORD']
    # FE bootstrap account is reachable only on the local development interface.
    with pymysql.connect(host='127.0.0.1', port=9030, user='root', password='', autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE DATABASE IF NOT EXISTS dunnelean_test')
            cur.execute("CREATE USER IF NOT EXISTS 'dunnelean' IDENTIFIED BY %s", (password,))
            cur.execute("GRANT SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV ON dunnelean_test.* TO 'dunnelean'")
            cur.execute('SHOW BACKENDS')
            columns = [c[0] for c in cur.description]
            rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            assert len(rows) == 1 and rows[0]['Alive'] in (1, True, 'true'), rows
    print('Isolated Doris database and business account initialized; credentials stay in deploy/.env')

if __name__ == '__main__':
    initialize()
