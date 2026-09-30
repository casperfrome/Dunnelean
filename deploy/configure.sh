#!/usr/bin/env bash
set -euo pipefail
role="$1"
conf="/opt/apache-doris/$role/conf/$role.conf"
test -s "$conf" || { echo "Image configuration missing: $conf" >&2; exit 1; }
set_conf() {
  local key="$1" value="$2"
  if grep -q "^[[:space:]]*$key[[:space:]]*=" "$conf"; then
    sed -i "s|^[[:space:]]*$key[[:space:]]*=.*|$key = $value|" "$conf"
  else
    printf '\n%s = %s\n' "$key" "$value" >> "$conf"
  fi
}
set_conf priority_networks 172.30.41.0/24
sed -i -E 's/-XX:-UseContainerSupport //g; s/-XX:ActiveProcessorCount=[0-9]+ //g' "$conf"
if [[ "$role" == fe ]]; then
  sed -i -E 's/-Xmx[0-9]+[mg]/-Xmx2048m/g; s/-Xms[0-9]+[mg]/-Xms2048m/g' "$conf"
  sed -i 's/-Dfile.encoding=UTF-8 /-Dfile.encoding=UTF-8 -XX:-UseContainerSupport -XX:ActiveProcessorCount=2 /' "$conf"
  set_conf arrow_flight_sql_port 8070
  exec bash /usr/local/bin/init_fe.sh
else
  sed -i -E 's/-Xmx[0-9]+[mg]/-Xmx1024m/g' "$conf"
  sed -i 's/-Dfile.encoding=UTF-8 /-Dfile.encoding=UTF-8 -XX:-UseContainerSupport -XX:ActiveProcessorCount=4 /' "$conf"
  set_conf mem_limit 4G
  set_conf arrow_flight_sql_port 8050
  set_conf public_host 127.0.0.1
  set_conf arrow_flight_sql_proxy_port 8050
  exec bash /usr/local/bin/entry_point.sh
fi
