#!/usr/bin/env python3
"""Bounded live AMQP workload, process supervisor, and truthful dashboard."""
from __future__ import annotations
import base64, collections, hashlib, hmac, http.cookies, json, os, signal, subprocess, threading, time, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path
from live_state import BoundedSeen, PendingTimes, Pacer

ROOT=Path(os.getenv("SWLB_ROOT","/opt/swlb"));CONFIG=os.getenv("SWLB_CONFIG","/etc/swlb/config.yaml");STATE=Path(os.getenv("SWLB_STATE","/var/lib/swlb/controller.json"));LOG_DIR=Path(os.getenv("SWLB_LOG_DIR","/var/log/swlb"));PORT=int(os.getenv("SWLB_PUBLIC_PORT","8080"));ADMIN_TOKEN=os.environ["SWLB_ADMIN_TOKEN"];MAX_BACKLOG=int(os.getenv("SWLB_MAX_BACKLOG","1000"));SAMPLE_LIMIT=int(os.getenv("SWLB_SAMPLE_LIMIT","200"));NAMESPACE=os.getenv("SWLB_NAMESPACE","swlb-live");GROUP_A=os.getenv("SWLB_GROUP_A","events-a");GROUP_B=os.getenv("SWLB_GROUP_B","events-b");PUB_A=os.getenv("SWLB_PUBLISHER_A","events-a-publisher-1");PUB_B=os.getenv("SWLB_PUBLISHER_B","events-b-publisher-1");SUB_A=os.getenv("SWLB_SUBSCRIBER_A","events-a-subscriber-1");SUB_B=os.getenv("SWLB_SUBSCRIBER_B","events-b-subscriber-1");SET_A=os.getenv("SWLB_SET_A","set-a");SET_B=os.getenv("SWLB_SET_B","set-b");STARTED=time.time();SDKPERF_HARNESS=os.getenv("SWLB_SDKPERF_HARNESS","");SDKPERF_VERSION=os.getenv("SWLB_SDKPERF_VERSION","");SDKPERF_MAX_MESSAGES=min(1000,max(1,int(os.getenv("SWLB_SDKPERF_MAX_MESSAGES","500"))));SDKPERF_MAX_RATE=min(100,max(1,int(os.getenv("SWLB_SDKPERF_MAX_RATE","50"))));SDKPERF_EVIDENCE_DIR=Path(os.getenv("SWLB_SDKPERF_EVIDENCE_DIR","/var/log/swlb/sdkperf-evidence"))
def latest_sdkperf_status():
 base={"installed":bool(SDKPERF_HARNESS and SDKPERF_VERSION),"version":SDKPERF_VERSION or "not installed","transport":"SMF/JCSMP via optional Go test adapter","mode":"test environment only","state":"idle","messages":0,"published":0,"delivered":0,"consumer_received":0,"error":""}
 try:
  evidence=max(SDKPERF_EVIDENCE_DIR.glob("*.json"),key=lambda p:p.stat().st_mtime);data=json.loads(evidence.read_text());base.update({"state":"passed","version":str(data["version"]),"messages":int(data["messages"]),"published":int(data["native_published"]),"delivered":int(data["native_delivered"]),"consumer_received":int(data["sdkperf_consumer_unique"])})
 except (OSError,ValueError,KeyError):pass
 return base
lock=threading.RLock();sdkperf_lock=threading.Lock();sdkperf_write_lock=threading.Lock();stop=threading.Event();started=PendingTimes(MAX_BACKLOG*2,600);seen=BoundedSeen(max(10000,MAX_BACKLOG*4));last_seq={};sessions={};processes={}
metrics={"rate_target":max(0,min(500,int(os.getenv("SWLB_RATE","100")))),"paused":False,"submitted":0,"accepted":0,"unique_delivered":0,"delivery_observations":0,"duplicates":0,"late_duplicates":0,"ordering_errors":0,"errors":0,"last_error":"","by_group":{g:{"submitted":0,"accepted":0,"delivered":0} for g in (GROUP_A,GROUP_B)},"by_broker":{},"latency":collections.deque(maxlen=SAMPLE_LIMIT),"rates":collections.deque(maxlen=60),"samples":collections.deque(maxlen=SAMPLE_LIMIT),"timeline":collections.deque(maxlen=SAMPLE_LIMIT),"queue_depths":{},"queue_fresh_at":0,"circuit_open":False,"sdkperf":latest_sdkperf_status()}

def metric_error(text):
 with lock:metrics["errors"]+=1;metrics["last_error"]=text
class Managed:
 def __init__(self,name,argv,kind):self.name=name;self.argv=argv;self.kind=kind;self.proc=None;self.generation=0;self.write_lock=threading.Lock();self.start()
 def start(self):
  path=LOG_DIR/f"{self.name}.log";RotatingFileHandler(path,maxBytes=5*1024*1024,backupCount=3).close();log=open(path,"a",buffering=1)
  self.proc=subprocess.Popen(self.argv,stdin=subprocess.PIPE if self.kind else subprocess.DEVNULL,stdout=subprocess.PIPE if self.kind else log,stderr=log,text=True,bufsize=1);self.generation+=1;generation=self.generation
  with lock:old=metrics.setdefault("processes",{}).get(self.name,{});metrics["processes"][self.name]={"pid":self.proc.pid,"state":"running","restarts":old.get("restarts",-1)+1,"generation":generation}
  if self.kind=="publisher":threading.Thread(target=self.read_receipts,args=(self.proc,generation),daemon=True).start()
  if self.kind=="subscriber":threading.Thread(target=self.read_deliveries,args=(self.proc,generation),daemon=True).start()
 def write(self,value,proc=None,generation=None):
  with self.write_lock:
   if proc is not None and (proc is not self.proc or generation!=self.generation):raise RuntimeError("stale process generation")
   if self.proc.poll() is not None:raise RuntimeError(f"{self.name} stopped")
   self.proc.stdin.write(json.dumps(value,separators=(",",":"))+"\n");self.proc.stdin.flush()
 def read_receipts(self,proc,generation):
  for line in proc.stdout:
   if proc is not self.proc or generation!=self.generation:return
   try:r=json.loads(line)
   except:continue
   eid=r.get("event_id");group=r.get("group")
   with lock:
    if eid and r.get("durably_accepted"):
     metrics["accepted"]+=1;metrics["by_group"][group]["accepted"]+=1;broker=r.get("broker") or "unassigned";metrics["by_broker"][broker]=metrics["by_broker"].get(broker,0)+1
    else:metric_error("invalid publisher receipt")
   if eid and eid.startswith("sdkperf-") and sdkperf_process and sdkperf_process.poll() is None:
    try:
     with sdkperf_write_lock:sdkperf_process.stdin.write(json.dumps({**r,"kind":"receipt"})+"\n");sdkperf_process.stdin.flush()
    except Exception as e:metric_error(str(e))
 def read_deliveries(self,proc,generation):
  group=GROUP_A if self.name.startswith("events-a") else GROUP_B
  for line in proc.stdout:
   if proc is not self.proc or generation!=self.generation:return
   try:d=json.loads(line)
   except:continue
   eid,did=d.get("event_id"),d.get("delivery_id");headers=d.get("headers") or {}
   if not eid or not did:continue
   with lock:
    metrics["delivery_observations"]+=1;is_new=seen.add(eid)
    if is_new:metrics["unique_delivered"]+=1;metrics["by_group"][group]["delivered"]+=1
    else:metrics["duplicates"]+=1
    key=headers.get("entity_id") or "unknown"
    try:seq=int(eid.rsplit("-",1)[1])
    except:seq=-1
    prior=last_seq.get((group,key),-1)
    if is_new and seq>=0 and seq<=prior:metrics["ordering_errors"]+=1
    if is_new and seq>=0:last_seq[(group,key)]=seq
    at=started.pop(eid)
    if at:metrics["latency"].append(round((time.time()-at)*1000,2))
    elif not is_new:metrics["late_duplicates"]+=1
    metrics["samples"].append({"at":time.time(),"group":group,"event_id":eid,"key":key,"duplicate":not is_new})
   if eid.startswith("sdkperf-") and sdkperf_process and sdkperf_process.poll() is None:
    try:
     with sdkperf_write_lock:sdkperf_process.stdin.write(json.dumps({**d,"kind":"delivery"})+"\n");sdkperf_process.stdin.flush()
     continue
    except Exception as e:metric_error(str(e))
   try:self.write({"delivery_id":did,"outcome":"ack"},proc,generation)
   except Exception as e:metric_error(str(e))
 def monitor(self):
  while not stop.wait(2):
   if self.proc.poll() is None:continue
   metric_error(f"{self.name} exited {self.proc.returncode}")
   with lock:metrics["processes"][self.name]["state"]="restarting"
   if not stop.wait(3):self.start()
 def close(self):
  if self.proc and self.proc.poll() is None:
   self.proc.terminate()
   try:self.proc.wait(timeout=10)
   except subprocess.TimeoutExpired:self.proc.kill();self.proc.wait(timeout=5)
BIN=ROOT/"bin"
processes.update({"controller":Managed("controller",[str(BIN/"controller"),"-config",CONFIG],""),"events-a-publisher":Managed("events-a-publisher",[str(BIN/"publisher"),"-config",CONFIG,"-participant",PUB_A],"publisher"),"events-b-publisher":Managed("events-b-publisher",[str(BIN/"publisher"),"-config",CONFIG,"-participant",PUB_B],"publisher"),"events-a-subscriber":Managed("events-a-subscriber",[str(BIN/"subscriber"),"-config",CONFIG,"-participant",SUB_A],"subscriber"),"events-b-subscriber":Managed("events-b-subscriber",[str(BIN/"subscriber"),"-config",CONFIG,"-participant",SUB_B],"subscriber")})
for p in processes.values():threading.Thread(target=p.monitor,daemon=True).start()
def membership():
 try:return json.loads(STATE.read_text()).get("membership",{})
 except:return {}
def ready():return all(membership().get(g,{}).get("phase")=="ACTIVE" for g in (GROUP_A,GROUP_B))
def generate():
 pacer=Pacer();sequence=0
 while not stop.is_set():
  if not ready():stop.wait(.5);continue
  with lock:
   rate=metrics["rate_target"];paused=metrics["paused"];observations=list(metrics["queue_depths"].values());unknown=not observations or any(q.get("current_depth") is None or not q.get("online") for q in observations);depth=sum(q.get("current_depth") or 0 for q in observations);unacked=sum(q.get("unacked") or 0 for q in observations);unresolved=len(started);metrics["circuit_open"]=unknown or depth+unacked>=MAX_BACKLOG or unresolved>=MAX_BACKLOG
  if paused or rate==0 or metrics["circuit_open"]:stop.wait(.2);continue
  group=GROUP_A if sequence%2==0 else GROUP_B;kind="events-a" if group==GROUP_A else "events-b";idx=sequence//2;eid=f"{kind}-{int(STARTED)}-{idx:09d}";key=idx%64;headers={"scaling-group":group,"entity_id":f"entity-{key:03d}","event_type":"updated","sequence":str(idx),"timestamp":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())}
  payload=base64.b64encode(json.dumps({"synthetic":True,"sequence":idx,"generated_at":time.time(),"padding":"x"*768}).encode()).decode()
  try:processes[kind+"-publisher"].write({"event_id":eid,"topic":"synthetic/events","headers":headers,"payload_base64":payload});started.add(eid,time.time());
  except Exception as e:metric_error(str(e));stop.wait(.2);continue
  with lock:metrics["submitted"]+=1;metrics["by_group"][group]["submitted"]+=1
  sequence+=1;stop.wait(pacer.delay(rate))
threading.Thread(target=generate,daemon=True).start()
def sample_rates():
 prior=(0,0,time.monotonic())
 while not stop.wait(5):
  with lock:now=time.monotonic();dt=max(.01,now-prior[2]);cur=(metrics["accepted"],metrics["unique_delivered"],now);metrics["rates"].append({"at":time.time(),"accepted":round((cur[0]-prior[0])/dt,2),"delivered":round((cur[1]-prior[1])/dt,2)});prior=cur
threading.Thread(target=sample_rates,daemon=True).start()
def semp_loop():
 while not stop.is_set():
  result={};fresh=time.time()
  for role in "ABC":
   host=os.getenv(f"SWLB_SEMP_{role}_HOST");vpn=os.getenv(f"SWLB_SEMP_{role}_VPN");ue=os.getenv(f"SWLB_SEMP_{role}_USERNAME_ENV");pe=os.getenv(f"SWLB_SEMP_{role}_PASSWORD_ENV");user=os.getenv(ue or "");password=os.getenv(pe or "")
   for group,setid in ((GROUP_A,SET_A),(GROUP_B,SET_B)):
    q=f"{NAMESPACE}.data.{group}.broker-{role.lower()}.{setid}.e1";key=f"broker-{role.lower()}:{group}"
    try:
     auth="Basic "+base64.b64encode(f"{user}:{password}".encode()).decode();base=f"https://{host}:943/SEMP/v2/monitor/msgVpns/{urllib.parse.quote(vpn,safe='')}/queues/{urllib.parse.quote(q,safe='')}";req=urllib.request.Request(base);req.add_header("Authorization",auth);queue=json.load(urllib.request.urlopen(req,timeout=5)).get("data",{});count_req=urllib.request.Request(base+"/msgs?count=1");count_req.add_header("Authorization",auth);current=json.load(urllib.request.urlopen(count_req,timeout=5)).get("meta",{}).get("count");result[key]={"broker":f"broker-{role.lower()}","group":group,"current_depth":current,"unacked":queue.get("txUnackedMsgCount"),"rx_rate":queue.get("rxMsgRate"),"tx_rate":queue.get("txMsgRate"),"fresh_at":fresh,"online":True}
    except Exception as e:result[key]={"broker":f"broker-{role.lower()}","group":group,"current_depth":None,"unacked":None,"fresh_at":fresh,"online":False,"error":type(e).__name__}
  with lock:metrics["queue_depths"]=result;metrics["queue_fresh_at"]=fresh
  stop.wait(5)
threading.Thread(target=semp_loop,daemon=True).start()

sdkperf_process=None
def sdkperf_start(messages,rate,group):
 global sdkperf_process
 if not sdkperf_lock.acquire(blocking=False):raise RuntimeError("SDKPerf test action is busy")
 try:
  if not SDKPERF_HARNESS or not SDKPERF_VERSION:raise RuntimeError("SDKPerf test tooling is not installed")
  if sdkperf_process and sdkperf_process.poll() is None:raise RuntimeError("SDKPerf test already running")
  messages=int(messages);rate=int(rate)
  if not 1<=messages<=SDKPERF_MAX_MESSAGES or not 1<=rate<=SDKPERF_MAX_RATE:raise RuntimeError("SDKPerf test exceeds configured bounds")
  if group not in (GROUP_A,GROUP_B):raise RuntimeError("unsupported SDKPerf test group")
  path=LOG_DIR/"sdkperf-harness.log";RotatingFileHandler(path,maxBytes=2*1024*1024,backupCount=3).close();log=open(path,"a",buffering=1);sdkperf_process=subprocess.Popen([SDKPERF_HARNESS,"--messages",str(messages),"--rate",str(rate),"--group",group],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1,start_new_session=True)
  with lock:metrics["sdkperf"].update({"state":"running","messages":messages,"delivered":0,"error":""})
  def watch(proc):
   global sdkperf_process
   for line in proc.stdout:
    try:event=json.loads(line)
    except:continue
    if event.get("kind")=="publish":
     group_key="events-a" if group==GROUP_A else "events-b";processes[group_key+"-publisher"].write(event["publish"]);continue
    if event.get("kind")=="delivery-result":
     process_name="events-a-subscriber" if group==GROUP_A else "events-b-subscriber";processes[process_name].write({"delivery_id":event["delivery_id"],"outcome":"ack" if not event.get("error") else "retry"});continue
    with lock:
     if event.get("kind")=="progress":metrics["sdkperf"].update({k:v for k,v in event.items() if k in ("published","delivered","expected")})
     elif event.get("kind")=="status":metrics["sdkperf"].update({k:v for k,v in event.items() if k in ("state","version","messages","published","delivered","consumer_received","error")})
   proc.wait()
   with lock:
    if metrics["sdkperf"]["state"]=="running":metrics["sdkperf"].update({"state":"failed","error":f"harness exited {proc.returncode}"})
  threading.Thread(target=watch,args=(sdkperf_process,),daemon=True).start()
 finally:sdkperf_lock.release()
def sdkperf_stop():
 global sdkperf_process
 if sdkperf_process and sdkperf_process.poll() is None:
  try:os.killpg(sdkperf_process.pid,signal.SIGTERM)
  except ProcessLookupError:pass
 with lock:metrics["sdkperf"]["state"]="stopped"

last_membership_state={}
def update_timeline(membership_state):
    for group,state in membership_state.items():
        key=(state.get("revision"),state.get("phase"),tuple(state.get("current_membership") or []))
        if last_membership_state.get(group)!=key:
            metrics["timeline"].append({"at":time.time(),"event":"membership","group":group,"revision":key[0],"phase":key[1],"brokers":list(key[2])})
            last_membership_state[group]=key

def public_group(group): return "events-a" if group==GROUP_A else "events-b" if group==GROUP_B else "events"
def public_snapshot_map(values): return {public_group(k):v for k,v in values.items()}
def snapshot():
 with lock:
  l=sorted(metrics["latency"]);p95=l[min(len(l)-1,int(len(l)*.95))] if l else 0;now=time.time();m=membership();update_timeline(m);queue_depth=sum(q.get("current_depth") or 0 for q in metrics["queue_depths"].values());queue_unknown=not metrics["queue_depths"] or any(q.get("current_depth") is None for q in metrics["queue_depths"].values());stale=now-metrics["queue_fresh_at"]>15 or any(v.get("state")!="running" for v in metrics.get("processes",{}).values())
  base={**{k:v for k,v in metrics.items() if not isinstance(v,collections.deque)},"rates":list(metrics["rates"]),"p95_latency_ms":p95,"unresolved":len(started),"broker_queue_depth":None if queue_unknown else queue_depth,"stale":stale or queue_unknown,"dedup_window":{"size":len(seen),"limit":max(10000,MAX_BACKLOG*4)}}
  base["by_group"]=public_snapshot_map(metrics["by_group"]);base["membership"]=public_snapshot_map(m);base["samples"]=[{**e,"group":public_group(e.get("group")),"event_id":f"event-{i:06d}"} for i,e in enumerate(metrics["samples"])];base["queue_depths"]={f"{v.get('broker')}:{public_group(v.get('group'))}":{**v,"group":public_group(v.get("group"))} for v in metrics["queue_depths"].values()};base["processes"]={name:{**value,"name":name} for name,value in metrics["processes"].items()};return base

PAGE=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Solace Workload Balancer</title><style>:root{--navy:#102d48;--teal:#1c687f;--cyan:#d9f3fb;--ink:#10243a;--muted:#60758c}*{box-sizing:border-box}body{margin:0;background:#f7fafc;color:var(--ink);font:15px system-ui}header{padding:18px 24px;background:var(--navy);color:white;display:flex;justify-content:space-between;gap:16px;align-items:center}main{max-width:1120px;margin:auto;padding:20px}.stats,.brokers,.groups{display:grid;gap:12px}.stats{grid-template-columns:repeat(6,1fr)}.brokers{grid-template-columns:repeat(3,1fr)}.groups{grid-template-columns:repeat(2,1fr)}.card,.panel{background:white;border:1px solid #dbe5ee;border-radius:12px;padding:14px}.value{font-size:25px;font-weight:750}.muted{color:var(--muted)}.ok{color:#087b47}.bad{color:#c43a3a}.graph{background:var(--cyan);border-radius:18px;padding:24px;margin:18px 0}.shim{background:var(--navy);color:white;border-radius:14px;padding:18px;text-align:center;max-width:650px;margin:auto}.broker-row{display:grid;grid-template-columns:repeat(3,1fr);gap:24px;margin:36px auto;max-width:850px}.broker-node{background:var(--teal);color:white;border-radius:13px;padding:18px;min-height:125px}.connector{height:44px;width:70%;margin:auto;border-left:4px solid #17647c;border-right:4px solid #17647c;border-bottom:4px solid #17647c}.connector.down{border-bottom:0;border-top:4px solid #17647c}.badge{font-size:12px;padding:3px 7px;border-radius:999px;background:#ffffff2a}.bar{height:7px;background:#dce8ef;border-radius:5px;overflow:hidden}.bar span{display:block;height:100%;background:#55d3c3}.table{width:100%;border-collapse:collapse}.table td,.table th{padding:7px;border-bottom:1px solid #edf1f4;text-align:left}.table-wrap{overflow:auto}details{margin-top:14px}summary{cursor:pointer;font-weight:700}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#eef4f7;padding:10px;border-radius:8px}button,input{font:inherit}.login,.controls{display:flex;gap:8px;align-items:center}.controls input[type=range]{width:180px}@media(max-width:780px){header{align-items:flex-start}.stats{grid-template-columns:repeat(2,1fr)}.brokers,.groups{grid-template-columns:1fr}.broker-row{grid-template-columns:1fr;gap:10px}.connector{width:4px;border:0;background:#17647c}.graph{padding:14px}.login{flex-wrap:wrap}}@media(prefers-reduced-motion:reduce){*{animation:none!important}}</style></head><body><header><div><b>Solace Workload Balancer</b><div style="opacity:.75">Live generic synthetic events over AMQP 1.0</div></div><div id=auth></div></header><main><section class=stats id=stats></section><section class=graph><div class=shim><h2>Publisher shim</h2><div>Request/reply discovers broker IDs and URLs</div><div>Entity affinity + rendezvous selects Broker A, B, or C</div></div><div class=connector></div><div class=broker-row id=brokerNodes></div><div class="connector down"></div><div class=shim><h2>Subscriber shim</h2><div>Maintains consumers on active brokers</div><div>Delivers by entity key · ACK after processing</div></div></section><section class=brokers id=brokers></section><section class=groups id=groups></section><section class=groups><div class=panel><h2>Process health</h2><div id=processes></div></div><div class=panel><h2>Membership</h2><div id=membership></div></div></section><section class=panel><h2>SDKPerf test environment</h2><div id=sdkperf></div><p class=muted>The authenticated button runs the complete 100-message source → native Go shims → SDKPerf consumer test. No terminal is required. SDKPerf is optional JCSMP/SMF test tooling and is not part of production startup.</p><div id=sdkperfControls></div><details><summary>SDKPerf CLI examples</summary><p>Prerequisites: Java, the <a href="https://products.solace.com/download/SDKPERF_JAVA" rel="noreferrer">official SDKPerf Java download</a>, and private values exported as <code>SDKPERF_CMD</code>, <code>SMF_URL</code>, <code>SDKPERF_USERNAME</code>, <code>VPN</code>, and <code>SDKPERF_PASSWORD</code>. Start the subscriber first.</p><p><b>Direct broker smoke test — bypasses the shims</b></p><pre id=directConsumer>"$SDKPERF_CMD" "-cip=$SMF_URL" "-cu=$SDKPERF_USERNAME@$VPN" "-cp=$SDKPERF_PASSWORD" -mt=persistent -stl=swlb-sdkperf/direct-smoke -md</pre><button onclick="copyText('directConsumer')">Copy subscriber</button><pre id=directProducer>"$SDKPERF_CMD" "-cip=$SMF_URL" "-cu=$SDKPERF_USERNAME@$VPN" "-cp=$SDKPERF_PASSWORD" -mt=persistent -ptl=swlb-sdkperf/direct-smoke -mn=100 -mr=25 -msa=256 -ped=2</pre><button onclick="copyText('directProducer')">Copy producer</button><p><b>Managed end-to-end source/sink</b>: the same verified SDKPerf flags target the dedicated ingress topic and durable result queue, but the authenticated workflow also runs the required Go adapter and connects it to the already-running shims. These two commands alone are not an end-to-end run.</p><pre id=e2eConsumer>"$SDKPERF_CMD" "-cip=$SMF_URL" "-cu=$SDKPERF_USERNAME@$VPN" "-cp=$SDKPERF_PASSWORD" -mt=persistent -sql=swlb-neutral-20260928.sdkperf.results -md -nsr</pre><button onclick="copyText('e2eConsumer')">Copy managed sink</button><pre id=e2eProducer>"$SDKPERF_CMD" "-cip=$SMF_URL" "-cu=$SDKPERF_USERNAME@$VPN" "-cp=$SDKPERF_PASSWORD" -mt=persistent -ptl=swlb-neutral-20260928/sdkperf/ingress -mn=100 -mr=25 -msa=256 -ped=2</pre><button onclick="copyText('e2eProducer')">Copy managed source</button></details></section><section class=panel><h2>Recent generic events</h2><div class=table-wrap><table class=table><thead><tr><th>Time</th><th>Group</th><th>Event ID</th><th>Entity</th><th>Duplicate</th></tr></thead><tbody id=events></tbody></table></div></section></main><script>let session=false;async function copyText(id){await navigator.clipboard.writeText(document.getElementById(id).textContent)}const labels={'events-a':'Events A','events-b':'Events B'};function esc(x){return String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}async function login(){let password=document.querySelector('#password').value;let r=await fetch('/admin/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password})});session=r.ok;renderAuth()}async function control(rate){let r=await fetch('/admin/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({rate})});if(!r.ok)alert('Login required')}async function controlSDKPerf(action){let r=await fetch("/admin/sdkperf",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action,messages:100,rate:25,group:"events-a"})});if(!r.ok)alert("SDKPerf test action failed")}function renderAuth(){document.querySelector('#auth').innerHTML=session?`<div class=controls><span>Total rate</span><input id=rate type=range min=0 max=500 value=100 oninput="rv.textContent=this.value"><b id=rv>100</b><button onclick="control(+rate.value)">Apply</button><button onclick="control(0)">Pause</button></div>`:`<div class=login><input id=password type=password placeholder="Admin password"><button onclick=login()>Unlock controls</button></div>`}renderAuth();async function tick(){let s=await(await fetch('/api/status')).json(),r=s.rates.at(-1)||{};let stats=[['Target',s.rate_target+'/s'],['Accepted',r.accepted||0+'/s'],['Delivered',r.delivered||0+'/s'],['Broker backlog',s.broker_queue_depth],['p95 latency',s.p95_latency_ms+' ms'],['Order / dup',s.ordering_errors+' / '+s.duplicates]];document.querySelector('#stats').innerHTML=stats.map(x=>`<div class=card><div class=value>${esc(x[1])}</div><div class=muted>${esc(x[0])}</div></div>`).join('');let ids=['broker-a','broker-b','broker-c'],max=Math.max(1,...Object.values(s.by_broker));document.querySelector('#brokerNodes').innerHTML=ids.map((id,i)=>`<div class=broker-node><b>Broker ${'ABC'[i]}</b> ${i==0?'<span class=badge>Broker 0 + data</span>':''}<div>${esc(id)}</div><div>${esc(s.by_broker[id]||0)} assigned</div></div>`).join('');document.querySelector('#brokers').innerHTML=ids.map((id,i)=>{let qs=Object.values(s.queue_depths).filter(q=>q.broker==id),unknown=!qs.length||qs.some(q=>q.current_depth==null);depth=unknown?'unknown':qs.reduce((a,q)=>a+q.current_depth,0),online=qs.length&&qs.every(q=>q.online);return `<div class=card><h3>Broker ${'ABC'[i]} ${online?'<span class=ok>● online</span>':'<span class=bad>● stale</span>'}</h3><div class=muted>${i==0?'Logical Broker 0 cohosted · ':''}AMQP TLS</div><div>${esc(s.by_broker[id]||0)} assigned · ${depth} queued</div><div class=bar><span style="width:${100*(s.by_broker[id]||0)/max}%"></span></div></div>`}).join('');document.querySelector('#groups').innerHTML=Object.entries(s.by_group).map(([g,v])=>`<div class=card><h3>${labels[g]||'Events'}</h3><div class=muted>${v.accepted} accepted · ${v.delivered} delivered</div></div>`).join('');document.querySelector('#processes').innerHTML=Object.entries(s.processes).map(([n,p])=>`<div>${p.state=='running'?'<span class=ok>●</span>':'<span class=bad>●</span>'} process · restarts ${p.restarts}</div>`).join('');document.querySelector('#membership').innerHTML=Object.entries(s.membership).map(([g,m])=>`<div><b>${labels[g]||'Event group'}</b> · ${esc(m.phase)} · epoch ${esc(m.epoch)} · ${(m.current_membership||[]).map(esc).join(' + ')}</div>`).join('');document.querySelector('#sdkperf').innerHTML=`<b>${esc(s.sdkperf.version)}</b> · ${esc(s.sdkperf.transport)} · ${esc(s.sdkperf.state)} · ${esc(s.sdkperf.delivered)}/${esc(s.sdkperf.messages)}`;document.querySelector('#sdkperfControls').innerHTML=session?'<button onclick=controlSDKPerf("start")>Run bounded 100-message test</button> <button onclick=controlSDKPerf("stop")>Stop test</button>':'';document.querySelector('#events').innerHTML=s.samples.slice(-12).reverse().map(e=>`<tr><td>${new Date(e.at*1000).toLocaleTimeString()}</td><td>${labels[e.group]||'Events'}</td><td>${esc(e.event_id)}</td><td>${esc(e.key)}</td><td>${e.duplicate?'yes':'no'}</td></tr>`).join('')}setInterval(tick,2000);tick()</script></body></html>'''

def cookie_ok(handler):
 raw=handler.headers.get("Cookie","");cookie=http.cookies.SimpleCookie();cookie.load(raw);value=cookie.get("swlb_session");return bool(value and hmac.compare_digest(value.value,hmac.new(ADMIN_TOKEN.encode(),b"session",hashlib.sha256).hexdigest()))
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*_):pass
 def send(self,status,data,kind="application/json",headers=None):
  self.send_response(status);self.send_header("Content-Type",kind);self.send_header("Cache-Control","no-store");
  for k,v in (headers or {}).items():self.send_header(k,v)
  self.end_headers();self.wfile.write(data if isinstance(data,bytes) else data.encode())
 def do_GET(self):
  if self.path=="/api/status":self.send(200,json.dumps(snapshot()));return
  self.send(200,PAGE,"text/html; charset=utf-8")
 def do_POST(self):
  try:data=json.loads(self.rfile.read(int(self.headers.get("Content-Length","0"))))
  except:self.send(400,"{}");return
  if self.path=="/admin/login":
   if not hmac.compare_digest(str(data.get("password","")),ADMIN_TOKEN):self.send(401,"{}");return
   session=hmac.new(ADMIN_TOKEN.encode(),b"session",hashlib.sha256).hexdigest();self.send(204,b"",headers={"Set-Cookie":f"swlb_session={session}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=28800"});return
  if self.path=="/admin/sdkperf":
   if not cookie_ok(self):self.send(401,"{}");return
   try:
    if data.get("action")=="start":sdkperf_start(data.get("messages",100),data.get("rate",25),data.get("group",GROUP_A))
    elif data.get("action")=="stop":sdkperf_stop()
    else:raise RuntimeError("unsupported action")
   except Exception as e:self.send(409,json.dumps({"error":str(e)}));return
   self.send(204,b"");return
  if self.path=="/admin/control":
   if not cookie_ok(self):self.send(401,"{}");return
   try:rate=max(0,min(500,int(data.get("rate"))))
   except:self.send(400,"{}");return
   with lock:metrics["rate_target"]=rate;metrics["paused"]=rate==0;metrics["timeline"].append({"at":time.time(),"event":"rate","value":rate})
   self.send(204,b"");return
  self.send(404,"{}")
server=ThreadingHTTPServer(("0.0.0.0",PORT),Handler)
def shutdown(*_):stop.set();threading.Thread(target=server.shutdown,daemon=True).start()
signal.signal(signal.SIGTERM,shutdown);signal.signal(signal.SIGINT,shutdown)
try:server.serve_forever()
finally:
 stop.set()
 sdkperf_stop()
 # Give accepted records a bounded window to complete before terminating the
 # publisher processes. If completion remains ambiguous, the durable outbox still
 # recovers fail-closed as ACK-uncertain rather than silently retrying.
 deadline=time.monotonic()+15
 while time.monotonic()<deadline:
  with lock:pending=len(started)
  if pending==0:break
  time.sleep(.1)
 for p in processes.values():p.close()
