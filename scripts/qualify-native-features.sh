#!/usr/bin/env bash
# Run bounded, local-only native broker feature checks and remove the exact broker this script creates.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="$root/examples/qualification-jms"
state="$root/state/claude-directed-qualification/native"
progress="$root/state/claude-directed-qualification/native-progress.md"
report="$state/report.json"
results="$state/results.jsonl"
commands="$state/commands.txt"
container_name="codex-native-qual-$(date +%Y%m%d%H%M%S)-$$"
container_id=""
admin_password="native-$(openssl rand -hex 12)"
client_password="client-$(openssl rand -hex 12)"
qual_prefix="codex-native-qual-$$_$(date +%s)"
semp="http://127.0.0.1:28081"
java_bin="${JAVA_BIN:-/opt/homebrew/opt/openjdk/bin/java}"
maven_repo="$state/m2"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cleanup_status="NOT_RUN"
overall_exit=0

mkdir -p "$state"
: >"$results"
: >"$commands"

json_quote() {
  python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

record() {
  local test="$1" status="$2" reason="$3"
  printf '{"test":"%s","status":"%s","reason":%s}\n' \
    "$test" "$status" "$(printf '%s' "$reason" | json_quote)" >>"$results"
  if [[ "$status" == "FAIL" ]]; then overall_exit=1; fi
}

record_command() {
  printf '%s\n' "$1" >>"$commands"
}

redact() {
  python3 -c 'import sys; text=sys.stdin.read(); print(text.replace(sys.argv[1], "<redacted-admin-password>").replace(sys.argv[2], "<redacted-client-password>"), end="")' \
    "$admin_password" "$client_password"
}

cleanup() {
  local rc=$?
  set +e
  if [[ -n "$container_id" ]]; then
    docker rm -f "$container_id" >/dev/null 2>&1
    if docker container inspect "$container_id" >/dev/null 2>&1; then
      cleanup_status="FAIL"
    else
      cleanup_status="PASS"
    fi
  else
    cleanup_status="PASS"
  fi
  write_report "$rc"
  printf -- '- %s: Run finished (exit %d); exact owned container cleanup %s; report `%s`.\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$rc" "$cleanup_status" "$report" >>"$progress"
  trap - EXIT
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

write_report() {
  local exit_code="${1:-0}"
  python3 - "$results" "$commands" "$report" "$started_at" "$exit_code" "$cleanup_status" \
    "$container_name" "${broker_version:-unknown}" "${sdk_version:-10.27.2}" <<'PY'
import json, pathlib, sys
results_path, commands_path, report_path, started, exit_code, cleanup, name, broker, sdk = sys.argv[1:]
results = []
for line in pathlib.Path(results_path).read_text().splitlines():
    if line.strip():
        results.append(json.loads(line))
counts = {key: sum(r.get("status") == key for r in results) for key in ("PASS", "FAIL", "BLOCKED")}
report = {
    "schema_version": 1,
    "started_at": started,
    "finished_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "scope": "one disposable local Solace Standard broker; bounded semantic checks, not capacity",
    "broker": {"container_name": name, "image": "solace/solace-pubsub-standard", "version": broker,
               "ports": {"semp": "127.0.0.1:28081", "smf": "127.0.0.1:25555", "amqp": "127.0.0.1:25672"}},
    "sdk": {"maven": "com.solacesystems:sol-jms", "version": sdk},
    "commands": pathlib.Path(commands_path).read_text().splitlines(),
    "results": results,
    "counts": counts,
    "cleanup": {"status": cleanup, "owned_container_removed": cleanup == "PASS"},
    "exit_code": int(exit_code),
}
pathlib.Path(report_path).write_text(json.dumps(report, indent=2) + "\n")
PY
}

semp_request() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl --silent --show-error --fail-with-body --user "admin:$admin_password" \
      -H 'content-type: application/json' -X "$method" "$semp$path" -d "$body"
  else
    curl --silent --show-error --fail-with-body --user "admin:$admin_password" -X "$method" "$semp$path"
  fi
}

create_queue() {
  local queue_name="$1" output rc
  for _ in $(seq 1 120); do
    output="$(semp_request POST '/SEMP/v2/config/msgVpns/default/queues' \
      "{\"queueName\":\"$queue_name\",\"accessType\":\"exclusive\",\"permission\":\"consume\",\"maxMsgSpoolUsage\":100,\"ingressEnabled\":true,\"egressEnabled\":true}" 2>&1)" && rc=0 || rc=$?
    if (( rc == 0 )); then return 0; fi
    if ! printf '%s' "$output" | grep -q 'MESSAGE_SPOOL_DATA_NOT_AVAILABLE'; then
      printf '%s\n' "$output" | redact >&2
      return "$rc"
    fi
    sleep 1
  done
  printf 'message spool did not become ready while creating %s\n' "$queue_name" >&2
  return 1
}

run_java() {
  SOLACE_HOST='tcp://127.0.0.1:25555' SOLACE_VPN='default' SOLACE_USERNAME='default' \
    SOLACE_PASSWORD="$client_password" QUAL_PREFIX="$qual_prefix" \
    "$java_bin" -cp "$project/target/classes:$project/target/dependency/*" \
    dev.solace.autoscale.NativeFeatureQualification "$@"
}

capture_java() {
  local test="$1"; shift
  local output rc=0
  output="$(run_java "$@" 2>&1)" || rc=$?
  printf '%s\n' "$output" | redact
  if (( rc == 0 )); then
    printf '%s\n' "$output" | grep '^{' >>"$results"
  else
    record "$test" "FAIL" "$(printf '%s' "$output" | redact | tail -12)"
  fi
  return "$rc"
}

for port in 28081 25555 25672; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    record "preflight_loopback_ports" "FAIL" "port $port is occupied; no broker was created"
    exit 2
  fi
done
if docker container inspect "$container_name" >/dev/null 2>&1; then
  record "preflight_unique_container" "FAIL" "generated container name already exists"
  exit 2
fi
if [[ ! -x "$java_bin" ]]; then
  java_bin="$(/usr/libexec/java_home -v 1.8)/bin/java"
fi

printf -- '- %s: Creating one disposable broker `%s`; loopback ports rechecked free.\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$container_name" >>"$progress"
record_command 'docker create --name codex-native-qual-<unique> --shm-size=1g --ulimit core=-1 --ulimit nofile=2448:1048576 -p 127.0.0.1:28081:8080 -p 127.0.0.1:25555:55555 -p 127.0.0.1:25672:5672 -e username_admin_globalaccesslevel=admin -e username_admin_password=<redacted> solace/solace-pubsub-standard'
container_id="$(docker create --name "$container_name" --shm-size=1g --ulimit core=-1 --ulimit nofile=2448:1048576 \
  -p 127.0.0.1:28081:8080 -p 127.0.0.1:25555:55555 -p 127.0.0.1:25672:5672 \
  -e username_admin_globalaccesslevel=admin -e username_admin_password="$admin_password" \
  solace/solace-pubsub-standard)"
docker start "$container_id" >/dev/null

ready=0
for _ in $(seq 1 180); do
  if semp_request GET '/SEMP/v2/config/about/api' >/dev/null 2>&1; then ready=1; break; fi
  if [[ "$(docker inspect -f '{{.State.Running}}' "$container_id" 2>/dev/null)" != true ]]; then break; fi
  sleep 1
done
if (( ready == 0 )); then
  logs="$(docker logs --tail 40 "$container_id" 2>&1 | redact)"
  record "broker_startup" "FAIL" "$logs"
  exit 3
fi
record "broker_startup" "PASS" "SEMP v2 became ready"

about="$(semp_request GET '/SEMP/v2/config/about')"
broker_version="$(docker image inspect solace/solace-pubsub-standard --format '{{index .Config.Labels "version"}}')"
record_command 'PATCH /SEMP/v2/config/msgVpns/default {authenticationBasicEnabled:true,authenticationBasicType:internal,maxMsgSpoolUsage:1500,serviceAmqpPlainTextEnabled:true}'
semp_request PATCH '/SEMP/v2/config/msgVpns/default' '{"authenticationBasicEnabled":true,"authenticationBasicType":"internal","maxMsgSpoolUsage":1500,"serviceAmqpPlainTextEnabled":true}' >/dev/null
record_command 'PATCH /SEMP/v2/config/msgVpns/default/clientUsernames/default {enabled:true,password:<redacted>}'
semp_request PATCH '/SEMP/v2/config/msgVpns/default/clientUsernames/default' \
  "{\"enabled\":true,\"password\":\"$client_password\"}" >/dev/null

queues=(tx.commit tx.rollback tx.redelivery tx.crash xa.commit xa.rollback xa.recovery replay)
for suffix in "${queues[@]}"; do
  q="$qual_prefix.$suffix"
  record_command "POST /SEMP/v2/config/msgVpns/default/queues {queueName:$q,accessType:exclusive,permission:consume,maxMsgSpoolUsage:100,ingressEnabled:true,egressEnabled:true}"
  create_queue "$q"
done

record_command 'mvn -f examples/qualification-jms/pom.xml -Dmaven.repo.local=state/claude-directed-qualification/native/m2 clean package dependency:copy-dependencies'
mvn -q -f "$project/pom.xml" -Dmaven.repo.local="$maven_repo" clean package dependency:copy-dependencies \
  -DincludeScope=runtime -DoutputDirectory="$project/target/dependency"
sdk_version="$(mvn -q -f "$project/pom.xml" help:evaluate -Dexpression=solace.jms.version -DforceStdout -Dmaven.repo.local="$maven_repo")"

record_command 'java NativeFeatureQualification local'
capture_java local_jms_transactions local || true

record_command 'java NativeFeatureQualification open-tx-crash <queue>; java NativeFeatureQualification assert-empty <queue>'
set +e
crash_output="$(run_java open-tx-crash "$qual_prefix.tx.crash" 2>&1)"
crash_rc=$?
set -e
printf '%s\n' "$crash_output" | redact
if (( crash_rc == 73 )) && printf '%s' "$crash_output" | grep -q 'OPEN_TX_READY count=4'; then
  capture_java open_transaction_client_crash assert-empty "$qual_prefix.tx.crash" || true
else
  record "open_transaction_client_crash" "FAIL" "producer did not halt with expected open-transaction marker and exit 73 (exit=$crash_rc)"
fi

record_command 'java NativeFeatureQualification xa-basic'
xa_basic_output="$(run_java xa-basic 2>&1)" && xa_basic_rc=0 || xa_basic_rc=$?
printf '%s\n' "$xa_basic_output" | redact
if (( xa_basic_rc == 0 )); then
  printf '%s\n' "$xa_basic_output" | grep '^{' >>"$results"
  xid_file="$state/recovery.xid"
  record_command 'java NativeFeatureQualification xa-prepare <ignored-state-xid>; docker restart <owned-id>; java NativeFeatureQualification xa-recover <ignored-state-xid>'
  if capture_java xa_restart_prepare xa-prepare "$xid_file"; then
    docker restart "$container_id" >/dev/null
    ready=0
    for _ in $(seq 1 180); do
      if semp_request GET '/SEMP/v2/config/about/api' >/dev/null 2>&1; then ready=1; break; fi
      sleep 1
    done
    if (( ready == 1 )); then
      smf_ready=0
      for _ in $(seq 1 120); do
        if run_java assert-empty "$qual_prefix.tx.crash" >/dev/null 2>&1; then smf_ready=1; break; fi
        sleep 1
      done
      if (( smf_ready == 1 )); then
        capture_java xa_restart_recovery xa-recover "$xid_file" || true
      else
        record "xa_restart_recovery" "FAIL" "SEMP returned after restart, but SMF did not accept an authenticated JMS connection"
      fi
    else
      record "xa_restart_recovery" "FAIL" "owned broker did not return after restart"
    fi
  fi
else
  reason="$(printf '%s' "$xa_basic_output" | redact | tail -12)"
  if printf '%s' "$xa_basic_output" | grep -Eqi 'not supported|not licensed|permission|not allowed|feature'; then
    record "xa_prepare_commit_rollback" "BLOCKED" "$reason"
    record "xa_restart_recovery" "BLOCKED" "XA API exists in sol-jms $sdk_version, but broker capability rejected basic XA: $reason"
  else
    record "xa_prepare_commit_rollback" "FAIL" "$reason"
    record "xa_restart_recovery" "BLOCKED" "basic XA failed before restart recovery could be meaningfully tested"
  fi
fi

replay_log="$qual_prefix-replay-log"
replay_topic="$qual_prefix/replay"
replay_queue="$qual_prefix.replay"
record_command "POST /SEMP/v2/config/msgVpns/default/replayLogs {replayLogName:$replay_log,maxSpoolUsage:100}"
replay_create="$(semp_request POST '/SEMP/v2/config/msgVpns/default/replayLogs' \
  "{\"replayLogName\":\"$replay_log\",\"maxSpoolUsage\":100}" 2>&1)" && replay_rc=0 || replay_rc=$?
if (( replay_rc == 0 )); then
  record_command "PATCH /SEMP/v2/config/msgVpns/default/replayLogs/$replay_log {ingressEnabled:true,egressEnabled:true,topicFilterEnabled:true}"
  semp_request PATCH "/SEMP/v2/config/msgVpns/default/replayLogs/$replay_log" \
    '{"ingressEnabled":true,"egressEnabled":true,"topicFilterEnabled":true}' >/dev/null
  record "replay_log_configuration" "PASS" "bounded 100 MB replay log created with ingress, egress, and topic filter enabled through SEMP v2"
  record_command "POST /SEMP/v2/config/msgVpns/default/replayLogs/$replay_log/topicFilterSubscriptions {topicFilterSubscription:$replay_topic}"
  replay_filter_rc=1
  for _ in $(seq 1 120); do
    replay_filter_output="$(semp_request POST "/SEMP/v2/config/msgVpns/default/replayLogs/$replay_log/topicFilterSubscriptions" \
      "{\"topicFilterSubscription\":\"$replay_topic\"}" 2>&1)" && replay_filter_rc=0 || replay_filter_rc=$?
    (( replay_filter_rc == 0 )) && break
    if ! printf '%s' "$replay_filter_output" | grep -q 'MESSAGE_SPOOL_DATA_NOT_AVAILABLE'; then break; fi
    sleep 1
  done
  if (( replay_filter_rc != 0 )); then
    record "ordinary_queue_replay_with_live_traffic" "FAIL" "replay topic filter configuration failed: $(printf '%s' "$replay_filter_output" | redact)"
  else
  record_command "POST /SEMP/v2/config/msgVpns/default/queues/$replay_queue/subscriptions {subscriptionTopic:$replay_topic}"
  semp_request POST "/SEMP/v2/config/msgVpns/default/queues/$replay_queue/subscriptions" \
    "{\"subscriptionTopic\":\"$replay_topic\"}" >/dev/null
  capture_java replay_topic_publish publish-topic "$replay_topic" "$qual_prefix-replayed" 5 || true
  record_command "PUT /SEMP/v2/action/msgVpns/default/queues/$replay_queue/startReplay {replayLogName:$replay_log}"
  replay_start="$(semp_request PUT "/SEMP/v2/action/msgVpns/default/queues/$replay_queue/startReplay" \
    "{\"replayLogName\":\"$replay_log\"}" 2>&1)" && replay_start_rc=0 || replay_start_rc=$?
  if (( replay_start_rc == 0 )); then
    capture_java ordinary_queue_replay_with_live_traffic replay-assert "$replay_queue" \
      "$qual_prefix-replayed" 5 "$qual_prefix-live" 3 || true
  else
    record "ordinary_queue_replay_with_live_traffic" "FAIL" "SEMP startReplay failed: $(printf '%s' "$replay_start" | redact)"
  fi
  fi
else
  replay_reason="$(printf '%s' "$replay_create" | redact)"
  record "replay_log_configuration" "BLOCKED" "$replay_reason"
  record "ordinary_queue_replay_with_live_traffic" "BLOCKED" "replay log creation unavailable, so executable replay assertion could not start"
fi

telemetry_profile="native-$$_trace"
telemetry_profile="${telemetry_profile:0:21}"
record_command "POST /SEMP/v2/config/msgVpns/default/telemetryProfiles {telemetryProfileName:$telemetry_profile}"
telemetry_rc=1
for _ in $(seq 1 120); do
  telemetry_probe="$(semp_request POST '/SEMP/v2/config/msgVpns/default/telemetryProfiles' \
    "{\"telemetryProfileName\":\"$telemetry_profile\"}" 2>&1)" && telemetry_rc=0 || telemetry_rc=$?
  (( telemetry_rc == 0 )) && break
  if ! printf '%s' "$telemetry_probe" | grep -q 'MESSAGE_SPOOL_DATA_NOT_AVAILABLE'; then break; fi
  sleep 1
done
if (( telemetry_rc == 0 )); then
  record "tracing_configuration_probe" "PASS" "created a native telemetry profile through SEMP v2; no collector/export destination was provisioned"
else
  record "tracing_configuration_probe" "BLOCKED" "telemetry profile creation failed: $(printf '%s' "$telemetry_probe" | redact)"
fi
record "traced_span_assertion" "BLOCKED" "no isolated OpenTelemetry collector/export destination was provisioned; telemetry profile creation is not a span assertion"

vpn_monitor="$(semp_request GET '/SEMP/v2/monitor/msgVpns/default')"
ha_role="$(docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' "$container_id")"
replication_enabled="$(printf '%s' "$vpn_monitor" | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["data"].get("replicationEnabled", False)).lower())')"
record "dr_ha_topology" "BLOCKED" "one Standard container only; replicationEnabled=$replication_enabled; no mate or DR topology exists (container restart policy=$ha_role)"

printf -- '- %s: Executed local transactions, client crash, XA capability/recovery, ordinary queue replay, tracing configuration probe, and topology checks. Cleanup pending trap.\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$progress"
exit "$overall_exit"
