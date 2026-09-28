#!/usr/bin/env python3
"""Optional bounded SDKPerf test harness; never used by production binaries."""
from __future__ import annotations
import argparse,json,math,os,queue,re,shlex,signal,subprocess,sys,threading,time
from pathlib import Path

MAX_MESSAGES=1000;MAX_RATE=100;MAX_SECONDS=300;MAX_CAPTURE_BYTES=2*1024*1024
ALLOWED_PRODUCER_ARGS={"-soe","-psm","-l","-mt=persistent"}
def allowed_producer_arg(arg):
 if arg in ALLOWED_PRODUCER_ARGS:return True
 for prefix,low,high in (("-ped=",0,30),("-lb=",1,4096),("-lg=",0,1000),("-psv=",1,50)):
  if arg.startswith(prefix):
   try:value=int(arg[len(prefix):])
   except ValueError:return False
   return low<=value<=high
 return False
stopping=threading.Event();children=[]

def required(name):
 value=os.getenv(name,"")
 if not value:raise RuntimeError(f"missing environment variable {name}")
 return value

def command_text(argv):
 return shlex.join(["-cp=<password>" if a.startswith("-cp=") else a for a in argv])

def spawn(argv,**kwargs):
 proc=subprocess.Popen(argv,start_new_session=True,**kwargs);children.append(proc);return proc

def stop_children():
 for proc in reversed(children):
  try:os.killpg(proc.pid,signal.SIGTERM)
  except ProcessLookupError:pass
 deadline=time.monotonic()+8
 for proc in reversed(children):
  remaining=max(0,deadline-time.monotonic())
  try:proc.wait(timeout=remaining)
  except subprocess.TimeoutExpired:pass
  try:os.killpg(proc.pid,signal.SIGKILL)
  except ProcessLookupError:pass

def handle_signal(*_):stopping.set()

def version_of(sdk):
 env=dict(os.environ);env["JAVA_HOME"]=required("SWLB_SDKPERF_JAVA_HOME");ensure_running()
 proc=spawn([sdk,"-v"],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,env=env)
 returncode=wait_process(proc,20);text=proc.stdout.read()
 match=re.search(r"(?:SDKPerf|sdkperf)[^\n]*?([0-9]+\.[0-9]+\.[0-9]+)",text,re.I)
 if returncode or not match:raise RuntimeError("SDKPerf version check failed")
 return match.group(1),env

def redacted_lines(stream,destination,secrets,line_observer=None):
 total=0
 for line in stream:
  if line_observer:line_observer(line)
  for secret in secrets:
   if secret:line=line.replace(secret,"<redacted>")
  encoded=line.encode(errors="replace");total+=len(encoded)
  if total<=MAX_CAPTURE_BYTES:destination.write(line);destination.flush()

def ensure_running():
 if stopping.is_set():raise RuntimeError("SDKPerf test stopped")

def wait_consumer_ready(path,proc,timeout=20):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  ensure_running()
  if proc.poll() is not None:raise RuntimeError(f"SDKPerf consumer exited before readiness: {proc.returncode}")
  text=path.read_text(errors="replace") if path.exists() else ""
  if re.search(r"(connected|subscription|flow.*up|ready)",text,re.I):return
  stopping.wait(.2)
 raise RuntimeError("SDKPerf consumer readiness was not observed")

def wait_process(proc,timeout):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  ensure_running()
  result=proc.poll()
  if result is not None:return result
  stopping.wait(.1)
 raise subprocess.TimeoutExpired("SDKPerf producer",timeout)

class ConsumerEvidence:
 def __init__(self,run_id,limit):self.pattern=re.compile(rf"sdkperf-{re.escape(run_id)}-[0-9]{{6}}");self.limit=limit;self.ids=set();self.lock=threading.Lock()
 def observe(self,line):
  with self.lock:
   for event_id in self.pattern.findall(line):
    if len(self.ids)<self.limit:self.ids.add(event_id)
 def count(self):
  with self.lock:return len(self.ids)

def wait_consumer_evidence(evidence,proc,expected,timeout):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  ensure_running();received=evidence.count()
  if received>=expected:ensure_running();return received
  if proc.poll() is not None:raise RuntimeError(f"SDKPerf consumer disconnected before evidence: {proc.returncode}")
  stopping.wait(.25)
 ensure_running();return evidence.count()

def producer_transmitted(path):
 text=path.read_text(errors="replace") if path.exists() else ""
 matches=re.findall(r"Total Messages transmitted\s*=\s*([0-9]+)",text)
 return int(matches[-1]) if matches else None

def main():
 parser=argparse.ArgumentParser();parser.add_argument("--messages",type=int,default=100);parser.add_argument("--rate",type=int,default=25);parser.add_argument("--payload-bytes",type=int,default=256);parser.add_argument("--entities",type=int,default=64);parser.add_argument("--seconds",type=int,default=90);parser.add_argument("--group",choices=("events-a","events-b"),default="events-a");parser.add_argument("--producer-arg",action="append",default=[]);a=parser.parse_args()
 if not 1<=a.messages<=MAX_MESSAGES or not 1<=a.rate<=MAX_RATE or not 1<=a.payload_bytes<=8192 or not 1<=a.entities<=1000 or not 10<=a.seconds<=MAX_SECONDS:raise RuntimeError("bounded limits: messages 1..1000, rate 1..100, payload 1..8192, entities 1..1000, seconds 10..300")
 minimum=math.ceil(a.messages/a.rate)+20
 if a.seconds<minimum:raise RuntimeError(f"seconds must be at least {minimum} for the requested rate")
 if any(not allowed_producer_arg(arg) for arg in a.producer_arg):raise RuntimeError("unsupported producer argument")
 sdk=required("SWLB_SDKPERF_CMD");adapter=required("SWLB_SDKPERF_ADAPTER");endpoint=required("SWLB_SDKPERF_AMQP_ENDPOINT");smf=required("SWLB_SDKPERF_SMF_URL");user=required("SWLB_SDKPERF_USERNAME");password=required("SWLB_SDKPERF_PASSWORD");vpn=required("SWLB_SDKPERF_VPN");ingress=required("SWLB_SDKPERF_INGRESS_QUEUE");ingress_topic=required("SWLB_SDKPERF_INGRESS_TOPIC");result_queue=required("SWLB_SDKPERF_RESULT_QUEUE");result_topic=required("SWLB_SDKPERF_RESULT_TOPIC")
 version,env=version_of(sdk);started_at=time.time();run_id=f"{int(started_at)}-{os.getpid()}";base=[sdk,f"-cip={smf}",f"-cu={user}@{vpn}",f"-cp={password}","-mt=persistent","-soe"]
 consumer=base+[f"-sql={result_queue}","-md","-nsr"]
 producer=base+[f"-ptl={ingress_topic}",f"-mn={a.messages}",f"-mr={a.rate}",f"-msa={a.payload_bytes}","-ped=2",*a.producer_arg]
 adapter_cmd=[adapter,"-endpoint",endpoint,"-ingress-queue",ingress,"-result-topic",result_topic,"-group",a.group,"-run-id",run_id,"-entities",str(a.entities)]
 print(json.dumps({"kind":"status","state":"starting","version":version,"mode":"shim-bridge","run_id":run_id,"started_at":time.time(),"messages":a.messages,"rate":a.rate,"payload_bytes":a.payload_bytes,"entities":a.entities,"duration":a.seconds,"commands":{"producer":command_text(producer),"consumer":command_text(consumer)}}),flush=True)
 event_queue=queue.Queue(maxsize=2048);secret_values=(password,);evidence_dir=Path(os.getenv("SWLB_SDKPERF_EVIDENCE_DIR","/var/log/swlb/sdkperf-evidence"));evidence_dir.mkdir(parents=True,exist_ok=True)
 consumer_path=evidence_dir/f"{run_id}-consumer.log";producer_path=evidence_dir/f"{run_id}-producer.log"
 with consumer_path.open("w") as consumer_log,producer_path.open("w") as producer_log:
  consumer_evidence=ConsumerEvidence(run_id,a.messages)
  sdk_cons=spawn(consumer,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1,env=env);consumer_reader=threading.Thread(target=redacted_lines,args=(sdk_cons.stdout,consumer_log,secret_values,consumer_evidence.observe),daemon=True);consumer_reader.start();wait_consumer_ready(consumer_path,sdk_cons);ensure_running()
  bridge=spawn(adapter_cmd,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1,env=env)
  threading.Thread(target=redacted_lines,args=(bridge.stderr,producer_log,secret_values),daemon=True).start()
  bridge_write_lock=threading.Lock()
  def dashboard_reader():
   try:
    for line in sys.stdin:
     event=json.loads(line)
     if event.get("kind") not in ("receipt","delivery"):continue
     with bridge_write_lock:
      bridge.stdin.write(json.dumps(event,separators=(",",":"))+"\n");bridge.stdin.flush()
   except Exception as exc:event_queue.put({"kind":"bridge-error","error":f"dashboard input: {exc}"})
  threading.Thread(target=dashboard_reader,daemon=True).start()
  def bridge_reader():
   try:
    for line in bridge.stdout:event_queue.put(json.loads(line),timeout=5)
   except Exception as exc:event_queue.put({"kind":"bridge-error","error":str(exc)})
  threading.Thread(target=bridge_reader,daemon=True).start();ensure_running()
  sdk_pub=spawn(producer,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1,env=env);producer_reader=threading.Thread(target=redacted_lines,args=(sdk_pub.stdout,producer_log,secret_values),daemon=True);producer_reader.start()
  published=delivered=0;deadline=time.monotonic()+a.seconds
  while time.monotonic()<deadline and delivered<a.messages and not stopping.is_set():
   if sdk_cons.poll() is not None:raise RuntimeError(f"SDKPerf consumer disconnected: {sdk_cons.returncode}")
   if bridge.poll() is not None:raise RuntimeError(f"Go adapter exited: {bridge.returncode}")
   try:event=event_queue.get(timeout=.5)
   except queue.Empty:continue
   if event.get("kind")=="bridge-error":raise RuntimeError(event.get("error") or "adapter output failed")
   print(json.dumps(event),flush=True) # dashboard owns native shim I/O
   if event.get("kind")=="publish":published+=1
   elif event.get("kind")=="delivery-result" and not event.get("error"):delivered+=1
   print(json.dumps({"kind":"progress","published":published,"delivered":delivered,"expected":a.messages}),flush=True)
  if stopping.is_set():raise RuntimeError("SDKPerf test stopped")
  producer_rc=wait_process(sdk_pub,15);producer_reader.join(timeout=5)
  if producer_rc:raise RuntimeError(f"SDKPerf producer exited {producer_rc}")
  transmitted=producer_transmitted(producer_path)
  if transmitted!=a.messages:raise RuntimeError(f"SDKPerf producer reported {transmitted} transmitted, expected {a.messages}")
  if published!=a.messages or delivered!=a.messages:raise RuntimeError(f"native shim evidence {published} published, {delivered} delivered, expected {a.messages}")
  received=wait_consumer_evidence(consumer_evidence,sdk_cons,a.messages,max(20,min(120,a.seconds)))
  if received!=a.messages:raise RuntimeError(f"SDKPerf consumer observed {received}/{a.messages} unique run-correlated results")
  ensure_running();finished_at=time.time();evidence={"run_id":run_id,"version":version,"started_at":started_at,"finished_at":finished_at,"messages":a.messages,"rate":a.rate,"payload_bytes":a.payload_bytes,"entities":a.entities,"duration_limit":a.seconds,"producer_exit":producer_rc,"sdkperf_producer_transmitted":transmitted,"native_published":published,"native_delivered":delivered,"sdkperf_consumer_unique":received,"producer_command":command_text(producer),"consumer_command":command_text(consumer)}
  (evidence_dir/f"{run_id}.json").write_text(json.dumps(evidence,indent=2)+"\n")
  print(json.dumps({"kind":"status","state":"passed","version":version,"mode":"shim-bridge","run_id":run_id,"started_at":started_at,"finished_at":finished_at,"elapsed_seconds":round(finished_at-started_at,3),"messages":a.messages,"rate":a.rate,"payload_bytes":a.payload_bytes,"entities":a.entities,"duration":a.seconds,"producer_transmitted":transmitted,"published":published,"delivered":delivered,"consumer_received":received}),flush=True)

if __name__=="__main__":
 signal.signal(signal.SIGTERM,handle_signal);signal.signal(signal.SIGINT,handle_signal)
 try:main()
 except Exception as exc:
  error="SDKPerf producer did not exit within the evidence window" if isinstance(exc,subprocess.TimeoutExpired) else str(exc)
  print(json.dumps({"kind":"status","state":"failed","error":error}),flush=True);raise SystemExit(1)
 finally:stop_children()
