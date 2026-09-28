#!/usr/bin/env python3
"""Bounded live AMQP workload, process supervisor, and truthful dashboard."""
from __future__ import annotations
import base64, collections, hashlib, hmac, http.cookies, json, os, signal, subprocess, threading, time, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path
from live_state import BoundedSeen, FamilyTracker, PendingTimes, Pacer
from sdkperf_control import TEMPLATES, harness_argv, info_argv, managed_preview, managed_spec, parse_console, redact, template_spec

ROOT=Path(os.getenv("SWLB_ROOT","/opt/swlb"));CONFIG=os.getenv("SWLB_CONFIG","/etc/swlb/config.yaml");STATE=Path(os.getenv("SWLB_STATE","/var/lib/swlb/controller.json"));LOG_DIR=Path(os.getenv("SWLB_LOG_DIR","/var/log/swlb"));PORT=int(os.getenv("SWLB_PUBLIC_PORT","8080"));ADMIN_TOKEN=os.environ["SWLB_ADMIN_TOKEN"];MAX_BACKLOG=int(os.getenv("SWLB_MAX_BACKLOG","1000"));SAMPLE_LIMIT=int(os.getenv("SWLB_SAMPLE_LIMIT","200"));NAMESPACE=os.getenv("SWLB_NAMESPACE","swlb-live");GROUP_A=os.getenv("SWLB_GROUP_A","events-a");GROUP_B=os.getenv("SWLB_GROUP_B","events-b");PUB_A=os.getenv("SWLB_PUBLISHER_A","events-a-publisher-1");PUB_B=os.getenv("SWLB_PUBLISHER_B","events-b-publisher-1");SUB_A=os.getenv("SWLB_SUBSCRIBER_A","events-a-subscriber-1");SUB_B=os.getenv("SWLB_SUBSCRIBER_B","events-b-subscriber-1");SET_A=os.getenv("SWLB_SET_A","set-a");SET_B=os.getenv("SWLB_SET_B","set-b");STARTED=time.time();SDKPERF_HARNESS=os.getenv("SWLB_SDKPERF_HARNESS","");SDKPERF_VERSION=os.getenv("SWLB_SDKPERF_VERSION","");SDKPERF_MAX_MESSAGES=min(1000,max(1,int(os.getenv("SWLB_SDKPERF_MAX_MESSAGES","1000"))));SDKPERF_MAX_RATE=min(100,max(1,int(os.getenv("SWLB_SDKPERF_MAX_RATE","100"))));SDKPERF_EVIDENCE_DIR=Path(os.getenv("SWLB_SDKPERF_EVIDENCE_DIR","/var/log/swlb/sdkperf-evidence"))
def latest_sdkperf_status():
 base={"installed":bool(SDKPERF_HARNESS and SDKPERF_VERSION),"version":SDKPERF_VERSION or "not installed","transport":"SMF/JCSMP via optional Go test adapter","mode":"test environment only","state":"idle","messages":0,"producer_transmitted":0,"published":0,"delivered":0,"consumer_received":0,"error":""}
 try:
  evidence=max(SDKPERF_EVIDENCE_DIR.glob("*.json"),key=lambda p:p.stat().st_mtime);data=json.loads(evidence.read_text());base.update({"state":"passed","version":str(data["version"]),"run_id":str(data.get("run_id","")),"started_at":data.get("started_at",0),"finished_at":data.get("finished_at",0),"elapsed_seconds":round(float(data.get("finished_at",0))-float(data.get("started_at",0)),3) if data.get("finished_at") and data.get("started_at") else None,"duration":data.get("duration",data.get("duration_limit")),"messages":int(data["messages"]),"payload_bytes":data.get("payload_bytes"),"entities":data.get("entities"),"producer_transmitted":int(data.get("sdkperf_producer_transmitted",0)),"published":int(data["native_published"]),"delivered":int(data["native_delivered"]),"consumer_received":int(data["sdkperf_consumer_unique"])})
 except (OSError,ValueError,KeyError):pass
 return base
FAMILY_KEYS={"A":"family-key-00","B":"family-key-01","C":"family-key-02","D":"family-key-03","E":"family-key-04"};FAMILY_EXPECTED={"A":"broker-b","B":"broker-c","C":"broker-b","D":"broker-a","E":"broker-a"}
lock=threading.RLock();family_tracker=FamilyTracker(FAMILY_KEYS);sdkperf_execution_lock=threading.Lock();sdkperf_write_lock=threading.Lock();stop=threading.Event();started=PendingTimes(MAX_BACKLOG*2,600);seen=BoundedSeen(max(10000,MAX_BACKLOG*4));last_seq={};sessions={};processes={}
metrics={"rate_target":max(0,min(500,int(os.getenv("SWLB_RATE","100")))),"state":"starting","state_detail":"Waiting for current broker and process observations","paused":False,"submitted":0,"accepted":0,"unique_delivered":0,"delivery_observations":0,"duplicates":0,"late_duplicates":0,"ordering_errors":0,"errors":0,"last_error":"","by_group":{g:{"submitted":0,"accepted":0,"delivered":0} for g in (GROUP_A,GROUP_B)},"by_broker":{},"latency":collections.deque(maxlen=SAMPLE_LIMIT),"rates":collections.deque(maxlen=60),"samples":collections.deque(maxlen=SAMPLE_LIMIT),"timeline":collections.deque(maxlen=SAMPLE_LIMIT),"queue_depths":{},"queue_fresh_at":0,"circuit_open":False,"sdkperf":latest_sdkperf_status()}
sdkperf_process=None;console_process=None;sdkperf_origin="";console_output=collections.deque(maxlen=400);console_state={"state":"idle","run_id":"","command":"","exit_status":None,"started_at":0,"finished_at":0,"error":""}

def sdkperf_run_matches(event_id):
 with lock:run_id=str(metrics["sdkperf"].get("run_id", ""))
 return bool(run_id and event_id.startswith(f"sdkperf-{run_id}-"))

def terminate_process_group(proc,timeout=12):
 if not proc or proc.poll() is not None:return
 try:os.killpg(proc.pid,signal.SIGTERM)
 except ProcessLookupError:return
 try:proc.wait(timeout=timeout)
 except subprocess.TimeoutExpired:
  try:os.killpg(proc.pid,signal.SIGKILL)
  except ProcessLookupError:pass
  try:proc.wait(timeout=5)
  except subprocess.TimeoutExpired:pass

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
   eid=r.get("event_id");group=r.get("group");family=(r.get("properties") or {}).get("event_family") or (eid.split("-")[2] if eid and eid.startswith("family-") else "")
   if eid and eid.startswith("sdkperf-"):
    if sdkperf_process and sdkperf_process.poll() is None and sdkperf_run_matches(eid):
     try:
      with sdkperf_write_lock:sdkperf_process.stdin.write(json.dumps({**r,"kind":"receipt"})+"\n");sdkperf_process.stdin.flush()
     except Exception as e:metric_error(str(e))
    continue
   with lock:
    if eid and r.get("durably_accepted"):
     metrics["accepted"]+=1;metrics["by_group"][group]["accepted"]+=1;broker=r.get("broker") or "unassigned";metrics["by_broker"][broker]=metrics["by_broker"].get(broker,0)+1
     if family in FAMILY_KEYS:family_tracker.accepted(family,broker)
    else:metric_error("invalid publisher receipt")
 def read_deliveries(self,proc,generation):
  group=GROUP_A if self.name.startswith("events-a") else GROUP_B
  for line in proc.stdout:
   if proc is not self.proc or generation!=self.generation:return
   try:d=json.loads(line)
   except:continue
   eid,did=d.get("event_id"),d.get("delivery_id");headers=d.get("headers") or {}
   if not eid or not did:continue
   if eid.startswith("sdkperf-"):
    if sdkperf_process and sdkperf_process.poll() is None and sdkperf_run_matches(eid):
     try:
      with sdkperf_write_lock:sdkperf_process.stdin.write(json.dumps({**d,"kind":"delivery"})+"\n");sdkperf_process.stdin.flush()
      continue
     except Exception as e:metric_error(str(e))
   else:
    with lock:
     metrics["delivery_observations"]+=1;is_new=seen.add(eid)
     if is_new:metrics["unique_delivered"]+=1;metrics["by_group"][group]["delivered"]+=1
     else:metrics["duplicates"]+=1
     key=headers.get("entity_id") or "unknown";family=headers.get("event_family","");source=headers.get("event_source","");family_run=eid.split("-")[1] if source=="family-demo" and eid.startswith("family-") else ""
     try:seq=int(headers.get("family_sequence","-1"))
     except:seq=-1
     if source=="family-demo" and family in FAMILY_KEYS and seq>=0:
      if family_tracker.delivered(family,family_run,seq,not is_new):metrics["ordering_errors"]+=1
     at=started.pop(eid)
     if at:metrics["latency"].append(round((time.time()-at)*1000,2))
     elif not is_new:metrics["late_duplicates"]+=1
     metrics["samples"].append({"at":time.time(),"group":group,"event_id":eid,"key":key,"duplicate":not is_new})
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
 pacer=Pacer();sequence=0;family_sequences={family:0 for family in FAMILY_KEYS};run_id=str(int(STARTED))
 while not stop.is_set():
  if not ready():stop.wait(.5);continue
  with lock:
   rate=metrics["rate_target"];paused=metrics["paused"];observations=list(metrics["queue_depths"].values());unknown=not observations or any(q.get("current_depth") is None or not q.get("online") for q in observations);depth=sum(q.get("current_depth") or 0 for q in observations);unacked=sum(q.get("unacked") or 0 for q in observations);unresolved=len(started);metrics["circuit_open"]=unknown or depth+unacked>=MAX_BACKLOG or unresolved>=MAX_BACKLOG
  if paused or rate==0 or metrics["circuit_open"]:stop.wait(.2);continue
  family="ABCDE"[sequence%5];family_seq=family_sequences[family];family_sequences[family]+=1;eid=f"family-{run_id}-{family}-{family_seq:09d}";headers={"scaling-group":GROUP_A,"entity_id":FAMILY_KEYS[family],"event_source":"family-demo","event_family":family,"family_sequence":str(family_seq),"timestamp":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())}
  payload=base64.b64encode(json.dumps({"demo":"ordered-families","family":family,"sequence":family_seq,"generated_at":time.time(),"padding":"x"*700}).encode()).decode()
  try:processes["events-a-publisher"].write({"event_id":eid,"topic":"synthetic/families","headers":headers,"payload_base64":payload});started.add(eid,time.time())
  except Exception as e:metric_error(str(e));stop.wait(.2);continue
  with lock:metrics["submitted"]+=1;metrics["by_group"][GROUP_A]["submitted"]+=1;family_tracker.submitted(family,run_id)
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

def bounded_sdkperf_spec(spec):
 if spec.messages>SDKPERF_MAX_MESSAGES:raise ValueError(f"messages must be 1..{SDKPERF_MAX_MESSAGES} in this environment")
 if spec.rate>SDKPERF_MAX_RATE:raise ValueError(f"rate must be 1..{SDKPERF_MAX_RATE} in this environment")
 return spec

def sdkperf_start(spec,origin="template",command=""):
 global sdkperf_process,sdkperf_origin
 spec=bounded_sdkperf_spec(spec)
 if not sdkperf_execution_lock.acquire(blocking=False):raise RuntimeError("SDKPerf test action is busy")
 proc=None;prior_rate=None
 try:
  if not SDKPERF_HARNESS or not SDKPERF_VERSION:raise RuntimeError("SDKPerf test tooling is not installed")
  with lock:
   prior_rate=metrics["rate_target"];metrics["rate_target"]=0;metrics["paused"]=True;sdkperf_origin=origin
   metrics["sdkperf"].update({"state":"starting","run_id":"","messages":spec.messages,"rate":spec.rate,"payload_bytes":spec.payload_bytes,"entities":spec.entities,"duration":spec.duration,"producer_transmitted":0,"published":0,"delivered":0,"consumer_received":0,"error":"","prior_rate":prior_rate})
   if origin=="console":console_state.update({"state":"starting","run_id":"","command":command or managed_preview(spec),"exit_status":None,"started_at":time.time(),"finished_at":0,"error":""});console_output.clear();console_output.append("Managed command delegated to the bounded end-to-end harness.")
  path=LOG_DIR/"sdkperf-harness.log";RotatingFileHandler(path,maxBytes=2*1024*1024,backupCount=3).close();log=open(path,"a",buffering=1);proc=subprocess.Popen(harness_argv(SDKPERF_HARNESS,spec),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,bufsize=1,start_new_session=True);sdkperf_process=proc
  def watch(managed):
   global sdkperf_process,sdkperf_origin
   try:
    for line in managed.stdout:
     try:event=json.loads(line)
     except json.JSONDecodeError:continue
     if event.get("kind")=="publish":
      group_key="events-a" if spec.group==GROUP_A else "events-b";processes[group_key+"-publisher"].write(event["publish"]);continue
     if event.get("kind")=="delivery-result":
      process_name="events-a-subscriber" if spec.group==GROUP_A else "events-b-subscriber";processes[process_name].write({"delivery_id":event["delivery_id"],"outcome":"ack" if not event.get("error") else "retry"});continue
     with lock:
      if event.get("kind")=="progress":metrics["sdkperf"].update({k:v for k,v in event.items() if k in ("published","delivered","expected")})
      elif event.get("kind")=="status":
       metrics["sdkperf"].update({k:v for k,v in event.items() if k in ("state","version","run_id","started_at","finished_at","elapsed_seconds","messages","rate","payload_bytes","entities","duration","producer_transmitted","published","delivered","consumer_received","error")})
       if origin=="console":console_state.update({"state":event.get("state",console_state["state"]),"run_id":event.get("run_id",console_state["run_id"]),"exit_status":0 if event.get("state")=="passed" else console_state["exit_status"],"finished_at":event.get("finished_at",console_state["finished_at"]),"error":event.get("error","")})
    rc=managed.wait()
    with lock:
     if metrics["sdkperf"]["state"] in ("starting","running","stopping"):metrics["sdkperf"].update({"state":"failed","error":f"harness exited {rc}"})
     if origin=="console" and console_state["state"] in ("starting","running","stopping"):console_state.update({"state":"failed","exit_status":rc,"finished_at":time.time(),"error":metrics["sdkperf"].get("error",f"harness exited {rc}")})
   except Exception as exc:
    terminate_process_group(managed)
    with lock:
     metrics["sdkperf"].update({"state":"failed","error":str(exc),"finished_at":time.time()})
     if origin=="console":console_state.update({"state":"failed","exit_status":managed.poll(),"finished_at":time.time(),"error":str(exc)})
   finally:
    terminate_process_group(managed)
    with lock:
     restore=metrics["sdkperf"].pop("prior_rate",prior_rate);metrics["rate_target"]=restore;metrics["paused"]=restore==0
     if sdkperf_process is managed:sdkperf_process=None
     sdkperf_origin=""
    sdkperf_execution_lock.release()
  threading.Thread(target=watch,args=(proc,),daemon=True).start()
 except Exception as exc:
  terminate_process_group(proc)
  with lock:
   restore=metrics["sdkperf"].pop("prior_rate",prior_rate)
   if restore is not None:metrics["rate_target"]=restore;metrics["paused"]=restore==0
   metrics["sdkperf"].update({"state":"failed","error":str(exc),"finished_at":time.time()})
   if origin=="console":console_state.update({"state":"failed","exit_status":proc.poll() if proc else None,"finished_at":time.time(),"error":str(exc)})
   sdkperf_process=None;sdkperf_origin=""
  sdkperf_execution_lock.release();raise

def sdkperf_stop(origin=None):
 if not sdkperf_process or sdkperf_process.poll() is not None:raise RuntimeError("no managed SDKPerf test is running")
 if origin and sdkperf_origin!=origin:raise RuntimeError("this control does not own the active SDKPerf test")
 with lock:metrics["sdkperf"]["state"]="stopping"
 terminate_process_group(sdkperf_process)

def console_public():
 with lock:return {**console_state,"output":list(console_output)}
def console_start(command,ui_mode="managed",ui_options=None):
 global console_process
 mode,parsed=parse_console(command,ui_mode,ui_options);run_id=f"console-{int(time.time())}";secrets=(os.getenv("SWLB_SDKPERF_PASSWORD","") ,)
 if mode=="managed":sdkperf_start(parsed,"console",managed_preview(parsed));return
 if not sdkperf_execution_lock.acquire(blocking=False):raise RuntimeError("SDKPerf execution is busy")
 proc=None
 try:
  argv=info_argv(os.environ["SWLB_SDKPERF_CMD"],parsed);env=dict(os.environ);env["JAVA_HOME"]=os.environ["SWLB_SDKPERF_JAVA_HOME"]
  proc=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1,start_new_session=True,env=env);console_process=proc
  with lock:console_state.update({"state":"running","run_id":run_id,"command":"SDKPerf "+parsed[0],"exit_status":None,"started_at":time.time(),"finished_at":0,"error":""});console_output.clear()
  def watch(info):
   global console_process
   try:
    total=0
    for line in info.stdout:
     line=redact(line.rstrip(),secrets);total+=len(line)
     with lock:
      if total<=131072:console_output.append(line)
    rc=info.wait()
    with lock:console_state.update({"state":"passed" if rc==0 else "failed","exit_status":rc,"finished_at":time.time(),"error":"" if rc==0 else f"SDKPerf exited {rc}"})
   finally:
    terminate_process_group(info)
    if console_process is info:console_process=None
    sdkperf_execution_lock.release()
  threading.Thread(target=watch,args=(proc,),daemon=True).start()
 except Exception:
  terminate_process_group(proc);console_process=None;sdkperf_execution_lock.release();raise
def console_stop():
 if console_process and console_process.poll() is None:
  with lock:console_state["state"]="stopping"
  terminate_process_group(console_process)
 elif sdkperf_process and sdkperf_process.poll() is None:sdkperf_stop("console")
 else:raise RuntimeError("no SDKPerf console execution is running")

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
  current_state="error" if metrics["last_error"] else "degraded" if stale or queue_unknown or metrics["circuit_open"] else "paused" if metrics["paused"] or metrics["rate_target"]==0 else "healthy";detail=metrics["last_error"] or ("Metrics or a supervised process are stale" if stale or queue_unknown else "Backpressure circuit is open" if metrics["circuit_open"] else "Synthetic generation is paused" if current_state=="paused" else "All supervised processes and queue metrics are current")
  base={**{k:v for k,v in metrics.items() if not isinstance(v,collections.deque)},"state":current_state,"state_detail":detail,"rates":list(metrics["rates"]),"p95_latency_ms":p95,"unresolved":len(started),"broker_queue_depth":None if queue_unknown else queue_depth,"stale":stale or queue_unknown,"dedup_window":{"size":len(seen),"limit":max(10000,MAX_BACKLOG*4)}}
  base["by_group"]=public_snapshot_map(metrics["by_group"]);base["membership"]=public_snapshot_map(m);base["samples"]=[{**e,"group":public_group(e.get("group")),"event_id":f"event-{i:06d}"} for i,e in enumerate(metrics["samples"])];base["queue_depths"]={f"{v.get('broker')}:{public_group(v.get('group'))}":{**v,"group":public_group(v.get("group"))} for v in metrics["queue_depths"].values()};base["processes"]={name:{**value,"name":name} for name,value in metrics["processes"].items()};base["families"]={family:{**value,"expected_broker":FAMILY_EXPECTED[family],"affinity_ok":bool(value["broker"] and value["broker"]==FAMILY_EXPECTED[family] and value["broker_mismatches"]==0),"rate_target":round(metrics["rate_target"]/5,1)} for family,value in family_tracker.values.items()};base["family_design"]={"scaling_group":"events-a","keys_selected_for":"real rendezvous 2-2-1 placement","traffic_split":"40/40/20 at equal family rates"};return base

PAGE=(Path(__file__).with_name("dashboard.html").read_text())
CSS=(Path(__file__).with_name("dashboard.css").read_text())
JS=(Path(__file__).with_name("dashboard.js").read_text())
LOGO=(Path(__file__).with_name("solace-logo.svg").read_bytes())
def session_value(issued):return f"{issued}."+hmac.new(ADMIN_TOKEN.encode(),f"session:{issued}".encode(),hashlib.sha256).hexdigest()
def cookie_ok(handler):
 raw=handler.headers.get("Cookie","");cookie=http.cookies.SimpleCookie();cookie.load(raw);value=cookie.get("swlb_session")
 if not value:return False
 try:issued_text,signature=value.value.split(".",1);issued=int(issued_text)
 except (ValueError,TypeError):return False
 return 0<=time.time()-issued<=28800 and hmac.compare_digest(value.value,session_value(issued))
def request_ok(handler):
 return handler.headers.get("X-SWLB-Request")=="dashboard" and handler.headers.get("Origin") in (None,"https://workload.sol-se-emea.com")
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*_):pass
 def send(self,status,data,kind="application/json",headers=None):
  self.send_response(status);self.send_header("Content-Type",kind);self.send_header("Cache-Control","no-store");
  for k,v in (headers or {}).items():self.send_header(k,v)
  self.end_headers();self.wfile.write(data if isinstance(data,bytes) else data.encode())
 def do_GET(self):
  if self.path=="/api/status":self.send(200,json.dumps(snapshot()));return
  if self.path=="/dashboard.css":self.send(200,CSS,"text/css; charset=utf-8");return
  if self.path=="/dashboard.js":self.send(200,JS,"text/javascript; charset=utf-8");return
  if self.path=="/solace-logo.svg":self.send(200,LOGO,"image/svg+xml");return
  if self.path=="/admin/sdkperf/status":
   if not request_ok(self):self.send(403,"{}");return
   if not cookie_ok(self):self.send(401,"{}");return
   self.send(200,json.dumps({"templates":TEMPLATES,"console":console_public(),"limits":{"messages":SDKPERF_MAX_MESSAGES,"rate":SDKPERF_MAX_RATE,"payload_bytes":8192,"entities":1000,"duration":300}}));return
  self.send(200,PAGE,"text/html; charset=utf-8")
 def do_POST(self):
  if not request_ok(self):self.send(403,json.dumps({"error":"request origin rejected"}));return
  try:
   length=int(self.headers.get("Content-Length","0"))
   if length<0 or length>8192:raise ValueError("request body exceeds 8192 bytes")
   data=json.loads(self.rfile.read(length))
  except Exception as e:self.send(400,json.dumps({"error":str(e)}));return
  if self.path=="/admin/login":
   if not hmac.compare_digest(str(data.get("password","")),ADMIN_TOKEN):self.send(401,"{}");return
   session=session_value(int(time.time()));self.send(204,b"",headers={"Set-Cookie":f"swlb_session={session}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=28800"});return
  if self.path=="/admin/sdkperf":
   if not cookie_ok(self):self.send(401,"{}");return
   try:
    if data.get("action")=="start":sdkperf_start(managed_spec(data))
    elif data.get("action")=="template":sdkperf_start(template_spec(str(data.get("template")),data.get("overrides") or {}))
    elif data.get("action")=="stop":sdkperf_stop()
    else:raise RuntimeError("unsupported action")
   except Exception as e:self.send(409,json.dumps({"error":str(e)}));return
   self.send(204,b"");return
  if self.path=="/admin/sdkperf/console":
   if not cookie_ok(self):self.send(401,"{}");return
   try:
    action=data.get("action")
    if action=="run":console_start(str(data.get("command","")),str(data.get("mode","managed")),data.get("options") or {})
    elif action=="stop":console_stop()
    elif action=="clear":
     with lock:console_output.clear()
    else:raise RuntimeError("unsupported console action")
   except Exception as e:self.send(409,json.dumps({"error":str(e)}));return
   self.send(204,b"");return
  if self.path=="/admin/control":
   if not cookie_ok(self):self.send(401,"{}");return
   if sdkperf_execution_lock.locked():self.send(409,json.dumps({"error":"synthetic rate is locked while SDKPerf is running"}));return
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
 if sdkperf_process and sdkperf_process.poll() is None:terminate_process_group(sdkperf_process)
 if console_process and console_process.poll() is None:terminate_process_group(console_process)
 # Give accepted records a bounded window to complete before terminating the
 # publisher processes. If completion remains ambiguous, the durable outbox still
 # recovers fail-closed as ACK-uncertain rather than silently retrying.
 deadline=time.monotonic()+15
 while time.monotonic()<deadline:
  with lock:pending=len(started)
  if pending==0:break
  time.sleep(.1)
 for p in processes.values():p.close()
