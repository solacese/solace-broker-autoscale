#!/usr/bin/env python3
"""Optional bounded SDKPerf test harness; never used by production binaries."""
from __future__ import annotations
import argparse,json,os,queue,re,shlex,signal,subprocess,sys,threading,time
from pathlib import Path

MAX_MESSAGES=1000;MAX_RATE=100;MAX_SECONDS=180;MAX_CAPTURE_BYTES=2*1024*1024
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
  if proc.poll() is None:
   try:os.killpg(proc.pid,signal.SIGTERM)
   except ProcessLookupError:pass
 deadline=time.monotonic()+8
 for proc in reversed(children):
  remaining=max(0,deadline-time.monotonic())
  try:proc.wait(timeout=remaining)
  except subprocess.TimeoutExpired:
   try:os.killpg(proc.pid,signal.SIGKILL)
   except ProcessLookupError:pass

def handle_signal(*_):stopping.set()

def version_of(sdk):
 env=dict(os.environ);env["JAVA_HOME"]=required("SWLB_SDKPERF_JAVA_HOME")
 result=subprocess.run([sdk,"-v"],capture_output=True,text=True,timeout=20,env=env)
 text=result.stdout+result.stderr
 match=re.search(r"(?:SDKPerf|sdkperf)[^\n]*?([0-9]+\.[0-9]+\.[0-9]+)",text,re.I)
 if result.returncode or not match:raise RuntimeError("SDKPerf version check failed")
 return match.group(1),env

def redacted_lines(stream,destination,secrets):
 total=0
 for line in stream:
  for secret in secrets:
   if secret:line=line.replace(secret,"<redacted>")
  encoded=line.encode(errors="replace");total+=len(encoded)
  if total<=MAX_CAPTURE_BYTES:destination.write(line);destination.flush()

def wait_consumer_ready(path,proc,timeout=20):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  if proc.poll() is not None:raise RuntimeError(f"SDKPerf consumer exited before readiness: {proc.returncode}")
  text=path.read_text(errors="replace") if path.exists() else ""
  if re.search(r"(connected|subscription|flow.*up|ready)",text,re.I):return
  time.sleep(.2)
 raise RuntimeError("SDKPerf consumer readiness was not observed")

def count_consumer_results(path,run_id):
 text=path.read_text(errors="replace") if path.exists() else ""
 ids=set(re.findall(rf"sdkperf-{re.escape(run_id)}-[0-9]{{6}}",text))
 return len(ids),text[-16000:]

def producer_transmitted(path):
 text=path.read_text(errors="replace") if path.exists() else ""
 matches=re.findall(r"Total Messages transmitted\s*=\s*([0-9]+)",text)
 return int(matches[-1]) if matches else None

def main():
 parser=argparse.ArgumentParser();parser.add_argument("--messages",type=int,default=100);parser.add_argument("--rate",type=int,default=25);parser.add_argument("--seconds",type=int,default=90);parser.add_argument("--group",choices=("events-a","events-b"),default="events-a");a=parser.parse_args()
 if not 1<=a.messages<=MAX_MESSAGES or not 1<=a.rate<=MAX_RATE or not 10<=a.seconds<=MAX_SECONDS:raise RuntimeError("bounded limits: messages 1..1000, rate 1..100, seconds 10..180")
 sdk=required("SWLB_SDKPERF_CMD");adapter=required("SWLB_SDKPERF_ADAPTER");endpoint=required("SWLB_SDKPERF_AMQP_ENDPOINT");smf=required("SWLB_SDKPERF_SMF_URL");user=required("SWLB_SDKPERF_USERNAME");password=required("SWLB_SDKPERF_PASSWORD");vpn=required("SWLB_SDKPERF_VPN");ingress=required("SWLB_SDKPERF_INGRESS_QUEUE");ingress_topic=required("SWLB_SDKPERF_INGRESS_TOPIC");result_queue=required("SWLB_SDKPERF_RESULT_QUEUE");result_topic=required("SWLB_SDKPERF_RESULT_TOPIC")
 version,env=version_of(sdk);run_id=f"{int(time.time())}-{os.getpid()}";base=[sdk,f"-cip={smf}",f"-cu={user}@{vpn}",f"-cp={password}","-mt=persistent","-soe"]
 consumer=base+[f"-sql={result_queue}","-md","-nsr"]
 producer=base+[f"-ptl={ingress_topic}",f"-mn={a.messages}",f"-mr={a.rate}","-msa=256","-ped=2"]
 adapter_cmd=[adapter,"-endpoint",endpoint,"-ingress-queue",ingress,"-result-topic",result_topic,"-group",a.group,"-run-id",run_id]
 print(json.dumps({"kind":"status","state":"starting","version":version,"mode":"shim-bridge","messages":a.messages,"rate":a.rate,"commands":{"producer":command_text(producer),"consumer":command_text(consumer)}}),flush=True)
 event_queue=queue.Queue(maxsize=2048);secret_values=(password,);evidence_dir=Path(os.getenv("SWLB_SDKPERF_EVIDENCE_DIR","/var/log/swlb/sdkperf-evidence"));evidence_dir.mkdir(parents=True,exist_ok=True)
 consumer_path=evidence_dir/f"{run_id}-consumer.log";producer_path=evidence_dir/f"{run_id}-producer.log"
 with consumer_path.open("w") as consumer_log,producer_path.open("w") as producer_log:
  sdk_cons=spawn(consumer,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1,env=env);threading.Thread(target=redacted_lines,args=(sdk_cons.stdout,consumer_log,secret_values),daemon=True).start();wait_consumer_ready(consumer_path,sdk_cons)
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
  threading.Thread(target=bridge_reader,daemon=True).start()
  sdk_pub=spawn(producer,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1,env=env);threading.Thread(target=redacted_lines,args=(sdk_pub.stdout,producer_log,secret_values),daemon=True).start()
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
  producer_rc=sdk_pub.wait(timeout=15)
  if producer_rc:raise RuntimeError(f"SDKPerf producer exited {producer_rc}")
  transmitted=producer_transmitted(producer_path)
  if transmitted!=a.messages:raise RuntimeError(f"SDKPerf producer reported {transmitted} transmitted, expected {a.messages}")
  if published!=a.messages or delivered!=a.messages:raise RuntimeError(f"native shim evidence {published} published, {delivered} delivered, expected {a.messages}")
  deadline=time.monotonic()+20;received=0
  while time.monotonic()<deadline:
   received,_=count_consumer_results(consumer_path,run_id)
   if received>=a.messages:break
   if sdk_cons.poll() is not None:raise RuntimeError(f"SDKPerf consumer disconnected before evidence: {sdk_cons.returncode}")
   time.sleep(.25)
  if received!=a.messages:raise RuntimeError(f"SDKPerf consumer observed {received}/{a.messages} unique run-correlated results")
  evidence={"run_id":run_id,"version":version,"messages":a.messages,"rate":a.rate,"producer_exit":producer_rc,"sdkperf_producer_transmitted":transmitted,"native_published":published,"native_delivered":delivered,"sdkperf_consumer_unique":received,"producer_command":command_text(producer),"consumer_command":command_text(consumer)}
  (evidence_dir/f"{run_id}.json").write_text(json.dumps(evidence,indent=2)+"\n")
  print(json.dumps({"kind":"status","state":"passed","version":version,"mode":"shim-bridge","messages":a.messages,"published":published,"delivered":delivered,"consumer_received":received}),flush=True)

if __name__=="__main__":
 signal.signal(signal.SIGTERM,handle_signal);signal.signal(signal.SIGINT,handle_signal)
 try:main()
 except Exception as exc:print(json.dumps({"kind":"status","state":"failed","error":str(exc)}),flush=True);raise SystemExit(1)
 finally:stop_children()
