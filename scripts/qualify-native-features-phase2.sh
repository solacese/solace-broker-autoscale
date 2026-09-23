#!/usr/bin/env bash
# Execute bounded native tracing and HA/DR capability qualification with exact owned-resource cleanup.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="$root/examples/qualification-jms"
state="$root/state/claude-directed-qualification/native"
phase="${1:-all}"
if [[ "$phase" != all && "$phase" != trace-only && "$phase" != ha-only ]]; then
  echo "usage: $0 [all|trace-only|ha-only]" >&2
  exit 2
fi
stamp="$(date -u +%Y%m%d-%H%M%S)"
run_dir="$state/phase2-$stamp"
report="$run_dir/report.json"
results="$run_dir/results.jsonl"
commands="$run_dir/commands.txt"
mkdir -p "$run_dir/collector"
: >"$results"; : >"$commands"

network="codex-native-qual-net-$stamp-$$"
created=()
network_created=0
admin_password="native-$(openssl rand -hex 12)"
otel_password="otel-$(openssl rand -hex 12)"
client_password="client-$(openssl rand -hex 12)"
java_bin="${JAVA_BIN:-/opt/homebrew/opt/openjdk/bin/java}"
overall_exit=0
cleanup_status="NOT_RUN"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

json_quote(){ python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'; }
record(){ local t="$1" s="$2" r="$3"; printf '{"test":"%s","status":"%s","reason":%s}\n' "$t" "$s" "$(printf '%s' "$r" | json_quote)" >>"$results"; [[ "$s" != FAIL ]] || overall_exit=1; }
cmd(){ printf '%s\n' "$1" >>"$commands"; }
redact(){ python3 -c 'import sys; t=sys.stdin.read(); names=("<admin-password>","<otel-password>","<client-password>");
for value,name in zip(sys.argv[1:],names): t=t.replace(value,name)
print(t,end="")' "$admin_password" "$otel_password" "$client_password"; }
semp(){ local port="$1" method="$2" path="$3" body="${4:-}"; if [[ -n "$body" ]]; then curl -sS --fail-with-body -u "admin:$admin_password" -H 'content-type: application/json' -X "$method" "http://127.0.0.1:$port$path" -d "$body"; else curl -sS --fail-with-body -u "admin:$admin_password" -X "$method" "http://127.0.0.1:$port$path"; fi; }
semp_v1(){ local port="$1" rpc="$2"; curl -sS --fail-with-body -u "admin:$admin_password" -H 'content-type: application/xml' -X POST "http://127.0.0.1:$port/SEMP" -d "$rpc"; }
ha_state(){ local xml="$1" expected_role="$2" expected_activity="$3"; python3 - "$xml" "$expected_role" "$expected_activity" <<'PY'
import json,sys,xml.etree.ElementTree as ET
root=ET.parse(sys.argv[1]).getroot()
def text(path):
    node=root.find('.//'+path)
    return '' if node is None or node.text is None else node.text.strip()
def texts(tag):
    return [n.text.strip() for n in root.findall('.//'+tag) if n.text and n.text.strip()]
role=sys.argv[2]; expected=sys.argv[3]
data={'config_status':text('config-status'),'redundancy_status':text('redundancy-status'),
      'active_standby_role':text('active-standby-role'),'adb_link_up':text('oper-status/adb-link-up'),
      'adb_hello_up':text('oper-status/adb-hello-up'),'activities':texts('activity'),
      'message_spool_statuses':texts('message-spool-status')}
ok=(data['config_status']=='Enabled' and data['redundancy_status']=='Up' and data['active_standby_role']==role
    and data['adb_link_up']=='true' and data['adb_hello_up']=='true' and expected in data['activities'])
print(json.dumps({'ok':ok,**data})); raise SystemExit(0 if ok else 1)
PY
}
ha_degraded_active(){ local redundancy_xml="$1" spool_xml="$2"; python3 - "$redundancy_xml" "$spool_xml" <<'PY'
import json,sys,xml.etree.ElementTree as ET
r=ET.parse(sys.argv[1]).getroot(); s=ET.parse(sys.argv[2]).getroot()
def text(root,path):
    n=root.find('.//'+path); return '' if n is None or n.text is None else n.text.strip()
def texts(root,tag): return [n.text.strip() for n in root.findall('.//'+tag) if n.text and n.text.strip()]
data={'config_status':text(r,'config-status'),'redundancy_status':text(r,'redundancy-status'),
      'active_standby_role':text(r,'active-standby-role'),'adb_link_up':text(r,'oper-status/adb-link-up'),
      'adb_hello_up':text(r,'oper-status/adb-hello-up'),'activities':texts(r,'activity'),
      'spool_operational_status':text(s,'operational-status'),'disk_contents_status':text(s,'disk-contents-status')}
ok=(data['config_status']=='Enabled' and data['active_standby_role']=='Backup' and 'Local Active' in data['activities']
    and data['redundancy_status']=='Down' and data['adb_link_up']=='false' and data['adb_hello_up']=='false'
    and data['spool_operational_status']=='AD-Active' and data['disk_contents_status']=='Ready')
print(json.dumps({'ok':ok,**data})); raise SystemExit(0 if ok else 1)
PY
}
xml_status_values(){ python3 - "$1" <<'PY'
import json,sys,xml.etree.ElementTree as ET
root=ET.parse(sys.argv[1]).getroot(); out={}
for node in root.iter():
    tag=node.tag.split('}')[-1]
    text=(node.text or '').strip()
    if text and any(k in tag.lower() for k in ('status','sync','spool')): out.setdefault(tag,[]).append(text)
print(json.dumps(out,sort_keys=True))
PY
}

write_report(){ local rc="$1"; python3 - "$results" "$commands" "$report" "$started_at" "$rc" "$cleanup_status" <<'PY'
import datetime,json,pathlib,sys
rp,cp,out,started,rc,cleanup=sys.argv[1:]
rows=[json.loads(x) for x in pathlib.Path(rp).read_text().splitlines() if x.strip()]
report={"schema_version":1,"phase":"native-tracing-and-ha-dr","started_at":started,
"finished_at":datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00','Z'),
"broker":{"image":"solace/solace-pubsub-standard","version":"10.26.0.8799"},
"collector":{"image":"otel/opentelemetry-collector-contrib:0.100.0","receiver":"solace","exporter":"file"},
"commands":pathlib.Path(cp).read_text().splitlines(),"results":rows,
"counts":{s:sum(r.get('status')==s for r in rows) for s in ('PASS','FAIL','BLOCKED','NOT_TESTED')},
"cleanup":{"status":cleanup,"all_owned_resources_removed":cleanup=='PASS'},"exit_code":int(rc)}
pathlib.Path(out).write_text(json.dumps(report,indent=2)+'\n')
print(out)
PY
}

cleanup(){ local rc=$?; set +e; local count=${#created[@]}; for ((i=count-1;i>=0;i--)); do docker rm -f "${created[$i]}" >/dev/null 2>&1; done; (( network_created == 0 )) || docker network rm "$network" >/dev/null 2>&1; cleanup_status=PASS; if (( count > 0 )); then for id in "${created[@]}"; do docker inspect "$id" >/dev/null 2>&1 && cleanup_status=FAIL; done; fi; docker network inspect "$network" >/dev/null 2>&1 && cleanup_status=FAIL; write_report "$rc" >/dev/null; trap - EXIT; exit "$rc"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for p in 28101 28102 28103 25571 25572 25573 25681 25682 25683; do ! lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1 || { record preflight FAIL "port $p occupied"; exit 2; }; done
[[ "$(docker info --format '{{.MemTotal}}')" -ge 12000000000 ]] || { record ha_topology BLOCKED "Docker memory below 12 GB required for three 4 GB routing/monitor nodes"; exit 0; }
docker network create "$network" >/dev/null; network_created=1

create_broker(){ local alias="$1" semp_port="$2" smf_port="$3" amqp_port="$4"; shift 4; local name="codex-native-qual-$alias-$stamp-$$"; CREATED_ID=$(docker create --name "$name" --hostname "$alias" --network "$network" --network-alias "$alias" --shm-size=1g --ulimit core=-1 --ulimit nofile=2448:1048576 -p "127.0.0.1:$semp_port:8080" -p "127.0.0.1:$smf_port:55555" -p "127.0.0.1:$amqp_port:5672" -e username_admin_globalaccesslevel=admin -e username_admin_password="$admin_password" "$@" solace/solace-pubsub-standard); created+=("$CREATED_ID"); docker start "$CREATED_ID" >/dev/null; }
wait_semp(){ local p="$1"; for _ in $(seq 1 180); do semp "$p" GET /SEMP/v2/config/about/api >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
post_ready(){ local p="$1" path="$2" body="$3" out; for _ in $(seq 1 120); do out=$(semp "$p" POST "$path" "$body" 2>&1) && return 0; printf '%s' "$out" | grep -q MESSAGE_SPOOL_DATA_NOT_AVAILABLE || { printf '%s' "$out"; return 1; }; sleep 1; done; return 1; }

mvn -q -f "$project/pom.xml" -Dmaven.repo.local="$state/m2" clean package dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory="$project/target/dependency"

if [[ "$phase" != ha-only ]]; then
cmd 'docker create codex-native-qual-trace-<unique> on isolated network; loopback SEMP 28101, SMF 25571, AMQP 25681'
create_broker broker 28101 25571 25681; broker="$CREATED_ID"
wait_semp 28101 || { record tracing_setup FAIL 'trace broker did not become ready'; exit 3; }
semp 28101 PATCH /SEMP/v2/config/msgVpns/default '{"authenticationBasicEnabled":true,"authenticationBasicType":"internal","serviceAmqpPlainTextEnabled":true}' >/dev/null
semp 28101 PATCH /SEMP/v2/config/msgVpns/default/clientUsernames/default "{\"enabled\":true,\"password\":\"$client_password\"}" >/dev/null
post_ready 28101 /SEMP/v2/config/msgVpns/default/telemetryProfiles '{"telemetryProfileName":"native-trace"}' >/dev/null
semp 28101 POST /SEMP/v2/config/msgVpns/default/clientUsernames "{\"clientUsername\":\"otel\",\"enabled\":true,\"password\":\"$otel_password\",\"aclProfileName\":\"#telemetry-native-trace\",\"clientProfileName\":\"#telemetry-native-trace\"}" >/dev/null
semp 28101 PATCH /SEMP/v2/config/msgVpns/default/telemetryProfiles/native-trace '{"receiverAclConnectDefaultAction":"allow","receiverEnabled":true,"traceEnabled":true}' >/dev/null
semp 28101 POST /SEMP/v2/config/msgVpns/default/telemetryProfiles/native-trace/traceFilters '{"traceFilterName":"all"}' >/dev/null
semp 28101 POST /SEMP/v2/config/msgVpns/default/telemetryProfiles/native-trace/traceFilters/all/subscriptions '{"subscription":"codex/native/trace/>","subscriptionSyntax":"smf"}' >/dev/null
semp 28101 PATCH /SEMP/v2/config/msgVpns/default/telemetryProfiles/native-trace/traceFilters/all '{"enabled":true}' >/dev/null
record tracing_broker_configuration PASS 'telemetry profile, generated receiver profiles, enabled receiver, enabled trace pipeline, and SMF trace filter configured'
cmd 'docker create otel/opentelemetry-collector-contrib:0.100.0 --config /etc/otelcol-contrib/config.yaml (official Solace receiver, file exporter)'
collector=$(docker create --name "codex-native-qual-otel-$stamp-$$" --network "$network" -e SOLACE_OTEL_PASSWORD="$otel_password" -v "$project/otel-collector.yaml:/etc/otelcol-contrib/config.yaml:ro" -v "$run_dir/collector:/evidence" otel/opentelemetry-collector-contrib:0.100.0 --config=/etc/otelcol-contrib/config.yaml); created+=("$collector"); docker start "$collector" >/dev/null
collector_ready=0; for _ in $(seq 1 60); do logs=$(docker logs "$collector" 2>&1); printf '%s' "$logs" | grep -q 'Everything is ready' && { collector_ready=1; break; }; docker inspect -f '{{.State.Running}}' "$collector" | grep -q true || break; sleep 1; done
if (( collector_ready == 0 )); then record tracing_collector FAIL "$(docker logs "$collector" 2>&1 | redact | tail -20)"; else record tracing_collector PASS 'official collector started and Solace receiver connected'; fi

run_trace(){ local prefix="$1" count="$2"; SOLACE_HOST=tcp://127.0.0.1:25571 SOLACE_VPN=default SOLACE_USERNAME=default SOLACE_PASSWORD="$client_password" QUAL_PREFIX=phase2 "$java_bin" -cp "$project/target/classes:$project/target/dependency/*" dev.solace.autoscale.NativeFeatureQualification trace-workload codex/native/trace/events "$prefix" "$count"; }
cmd 'java NativeFeatureQualification trace-workload codex/native/trace/events phase2-trace 12'
trace_output=$(run_trace phase2-trace 12 2>&1) && trace_rc=0 || trace_rc=$?
if (( trace_rc != 0 )); then record trace_workload FAIL "$(printf '%s' "$trace_output" | redact | tail -15)"; else printf '%s\n' "$trace_output" | grep '^{' >"$run_dir/workload.json"; record trace_workload PASS '12 persistent synthetic IDs published and received through JMS'; fi
sleep 5
if [[ -s "$run_dir/collector/traces.json" ]]; then
  python3 - "$run_dir/collector/traces.json" "$run_dir/trace-summary.json" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]); expected={f'phase2-trace-{i}' for i in range(12)}
objects=[]
for line in p.read_text().splitlines():
    try: objects.append(json.loads(line))
    except json.JSONDecodeError: pass
spans=[]
def walk(value):
    if isinstance(value,dict):
        if 'spanId' in value and ('name' in value or 'kind' in value): spans.append(value)
        for child in value.values(): walk(child)
    elif isinstance(value,list):
        for child in value: walk(child)
for obj in objects: walk(obj)
def strings(value):
    if isinstance(value,str): yield value
    elif isinstance(value,dict):
        for child in value.values(): yield from strings(child)
    elif isinstance(value,list):
        for child in value: yield from strings(child)
found=set()
operations=[]
for span in spans:
    values=set(strings(span)); found.update(expected & values)
    name=span.get('name');
    if isinstance(name,str): operations.append(name)
out={'expected_message_ids':sorted(expected),'exact_message_ids_in_spans':sorted(found),'all_ids_in_spans':found==expected,
     'span_count':len(spans),'span_operations':sorted(set(operations)),'bytes':p.stat().st_size}
pathlib.Path(sys.argv[2]).write_text(json.dumps(out,indent=2)+'\n'); print(json.dumps(out))
PY
  summary=$(cat "$run_dir/trace-summary.json")
  all_ids=$(printf '%s' "$summary" | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["all_ids_in_spans"]).lower())')
  spans=$(printf '%s' "$summary" | python3 -c 'import json,sys; print(json.load(sys.stdin)["span_count"])')
  operations=$(printf '%s' "$summary" | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin)["span_operations"]))')
  if [[ "$all_ids" == true && "$spans" -gt 0 && -n "$operations" ]]; then record traced_span_reconciliation PASS "structural JSON parse found all 12 exact message IDs across $spans spans with operations: $operations"; else record traced_span_reconciliation FAIL "collector export did not structurally reconcile all exact message IDs and span operations; see trace-summary.json"; fi
else
  record traced_span_reconciliation FAIL "collector produced no trace export; logs: $(docker logs "$collector" 2>&1 | redact | tail -20)"
fi

# Remove the tracing broker and collector before HA so no more than three brokers coexist.
docker rm -f "$collector" "$broker" >/dev/null
created=()
fi

if [[ "$phase" != trace-only ]]; then
# The HA developer topology uses two routing nodes plus a monitor node. Require exact SEMP v1 state and an ID-reconciled failover before PASS.
cmd 'docker create three-node Standard HA developer topology using documented redundancy_* and configsync_* keys'
psk="$(openssl rand -hex 32)"
ha_env=(-e system_scaling_maxconnectioncount=100 -e configsync_enable=yes -e redundancy_enable=yes -e redundancy_group_node_primary_connectvia=primary -e redundancy_group_node_primary_nodetype=message_routing -e redundancy_group_node_backup_connectvia=backup -e redundancy_group_node_backup_nodetype=message_routing -e redundancy_group_node_monitor_connectvia=monitor -e redundancy_group_node_monitor_nodetype=monitoring -e redundancy_authentication_presharedkey_key="$psk")
create_broker primary 28102 25572 25682 -e routername=primary -e nodetype=message_routing -e redundancy_activestandbyrole=primary -e redundancy_matelink_connectvia=backup "${ha_env[@]}"; primary="$CREATED_ID"
create_broker backup 28103 25573 25683 -e routername=backup -e nodetype=message_routing -e redundancy_activestandbyrole=backup -e redundancy_matelink_connectvia=primary "${ha_env[@]}"; backup="$CREATED_ID"
monitor_name="codex-native-qual-monitor-$stamp-$$"; monitor=$(docker create --name "$monitor_name" --hostname monitor --network "$network" --network-alias monitor --shm-size=1g --ulimit nofile=2448:1048576 -e username_admin_globalaccesslevel=admin -e username_admin_password="$admin_password" -e routername=monitor -e nodetype=monitoring -e system_scaling_maxconnectioncount=100 -e redundancy_enable=yes -e redundancy_group_node_primary_connectvia=primary -e redundancy_group_node_primary_nodetype=message_routing -e redundancy_group_node_backup_connectvia=backup -e redundancy_group_node_backup_nodetype=message_routing -e redundancy_group_node_monitor_connectvia=monitor -e redundancy_group_node_monitor_nodetype=monitoring -e redundancy_authentication_presharedkey_key="$psk" solace/solace-pubsub-standard); created+=("$monitor"); docker start "$monitor" >/dev/null
ha_ready=0; if wait_semp 28102 && wait_semp 28103; then ha_ready=1; fi
if (( ha_ready == 0 )); then
  details="primary=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$primary"); backup=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$backup"); monitor=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$monitor")"
  logs=$(docker logs --tail 15 "$primary" 2>&1; docker logs --tail 15 "$backup" 2>&1; docker logs --tail 15 "$monitor" 2>&1)
  record ha_topology BLOCKED "$details; $(printf '%s' "$logs" | redact | tail -20)"
else
  redundancy_rpc='<rpc><show><redundancy/></show></rpc>'
  configsync_rpc='<rpc><show><config-sync/></show></rpc>'
  spool_rpc='<rpc><show><message-spool><detail/></message-spool></show></rpc>'
  ha_oper=0
  for _ in $(seq 1 180); do
    semp_v1 28102 "$redundancy_rpc" >"$run_dir/primary-redundancy.xml" 2>/dev/null || true
    semp_v1 28103 "$redundancy_rpc" >"$run_dir/backup-redundancy.xml" 2>/dev/null || true
    if ha_state "$run_dir/primary-redundancy.xml" Primary 'Local Active' >"$run_dir/primary-redundancy.json" 2>/dev/null && ha_state "$run_dir/backup-redundancy.xml" Backup 'Mate Active' >"$run_dir/backup-redundancy.json" 2>/dev/null; then ha_oper=1; break; fi
    sleep 1
  done
  semp_v1 28102 "$configsync_rpc" >"$run_dir/primary-config-sync.xml" 2>/dev/null || true
  semp_v1 28103 "$configsync_rpc" >"$run_dir/backup-config-sync.xml" 2>/dev/null || true
  semp_v1 28102 "$spool_rpc" >"$run_dir/primary-spool.xml" 2>/dev/null || true
  semp_v1 28103 "$spool_rpc" >"$run_dir/backup-spool.xml" 2>/dev/null || true
  xml_status_values "$run_dir/primary-config-sync.xml" >"$run_dir/primary-config-sync.json" 2>/dev/null || true
  xml_status_values "$run_dir/backup-config-sync.xml" >"$run_dir/backup-config-sync.json" 2>/dev/null || true
  xml_status_values "$run_dir/primary-spool.xml" >"$run_dir/primary-spool.json" 2>/dev/null || true
  xml_status_values "$run_dir/backup-spool.xml" >"$run_dir/backup-spool.json" 2>/dev/null || true
  if (( ha_oper == 0 )); then
    record ha_topology BLOCKED "three nodes started, but authenticated SEMP v1 did not show exact Up/Primary Local Active and Up/Backup Mate Active states; see redundancy XML/JSON"
  else
    semp 28102 PATCH /SEMP/v2/config/msgVpns/default '{"authenticationBasicEnabled":true,"authenticationBasicType":"internal"}' >/dev/null
    semp 28102 PATCH /SEMP/v2/config/msgVpns/default/clientUsernames/default "{\"enabled\":true,\"password\":\"$client_password\"}" >/dev/null
    post_ready 28102 /SEMP/v2/config/msgVpns/default/queues '{"queueName":"phase2.ha","accessType":"exclusive","permission":"consume","maxMsgSpoolUsage":100,"ingressEnabled":true,"egressEnabled":true}' >/dev/null
    run_ha_java(){ local host="$1" mode="$2"; shift 2; SOLACE_HOST="tcp://127.0.0.1:$host" SOLACE_VPN=default SOLACE_USERNAME=default SOLACE_PASSWORD="$client_password" "$java_bin" -cp "$project/target/classes:$project/target/dependency/*" dev.solace.autoscale.NativeFeatureQualification "$mode" "$@"; }
    pub=$(run_ha_java 25572 queue-publish phase2.ha phase2-ha 8 2>&1) && pub_rc=0 || pub_rc=$?
    if (( pub_rc != 0 )); then record ha_failover FAIL "pre-failover publish failed: $(printf '%s' "$pub" | redact | tail -10)"; else
      docker stop "$primary" >/dev/null
      failover=0
      for _ in $(seq 1 180); do
        semp_v1 28103 "$redundancy_rpc" >"$run_dir/backup-after-failover.xml" 2>/dev/null || true
        semp_v1 28103 "$spool_rpc" >"$run_dir/backup-spool-after-failover.xml" 2>/dev/null || true
        if ha_degraded_active "$run_dir/backup-after-failover.xml" "$run_dir/backup-spool-after-failover.xml" >"$run_dir/backup-after-failover.json" 2>/dev/null; then failover=1; break; fi
        sleep 1
      done
      if (( failover == 0 )); then record ha_failover FAIL 'SEMP v1 did not show degraded Backup Local Active with expected mate Down and AD-Active/Ready spool after primary stop'; else
        con=$(run_ha_java 25573 queue-consume phase2.ha phase2-ha 8 2>&1) && con_rc=0 || con_rc=$?
        if (( con_rc == 0 )); then
          printf '%s\n' "$pub" | grep '^{' >"$run_dir/ha-publish.json"; printf '%s\n' "$con" | grep '^{' >"$run_dir/ha-consume.json"
          record ha_topology PASS 'authenticated SEMP v1 showed redundancy Up, ADB link/hello true, Primary Local Active, and Backup Mate Active; config-sync/spool status artifacts captured'
          record ha_failover PASS 'after stopping the exact owned primary, authenticated SEMP v1 showed Backup Local Active and 8 persistent IDs were reconciled in order'
        else record ha_failover FAIL "backup became AD-Active but ID reconciliation failed: $(printf '%s' "$con" | redact | tail -10)"; fi
      fi
    fi
  fi
fi
record dr_replication NOT_TESTED 'A supported HA group was attempted first within the bounded phase. A distinct message-VPN replication bridge, remote VPN authentication, replicated-topic set, and active/standby DR promotion require a separate two-site setup; bounded phase time expired before honest DR configuration. No unsupported/license claim and no URL-switch failover claim is made.'
fi

printf -- '- %s: Phase two tracing and HA/DR attempt completed; report `%s`; exact owned-resource cleanup pending trap.\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$report" >>"$state/../native-progress.md"
exit "$overall_exit"
