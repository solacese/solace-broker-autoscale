"""Validation and command construction for optional SDKPerf test tooling."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import math
import shlex

MAX_MESSAGES=1000;MAX_RATE=100;MAX_PAYLOAD_BYTES=8192;MAX_ENTITIES=1000;MAX_DURATION=300;MIN_DURATION=10
TEMPLATES={
 "quick":{"name":"Quick check","messages":100,"rate":25,"payload_bytes":256,"entities":64,"duration":90,"group":"events-a"},
 "steady":{"name":"Steady","messages":1000,"rate":50,"payload_bytes":256,"entities":128,"duration":60,"group":"events-a"},
 "burst":{"name":"Burst","messages":1000,"rate":100,"payload_bytes":256,"entities":128,"duration":90,"group":"events-b"},
 "large":{"name":"Larger messages","messages":100,"rate":25,"payload_bytes":8192,"entities":64,"duration":120,"group":"events-b"},
}
_FORBIDDEN_CHARS="|;&><$`(){}\n\r"
_BLOCKED_PREFIXES=("-cip", "-cu", "-cp", "-cpf", "-ptl", "-pql", "-stl", "-sql", "-sdl", "-pfl", "-pal", "-mdd", "-epl", "-cpl", "-ss", "-jvm", "-D", "-jaas", "-ts", "-ks", "-cn", "-rc")

@dataclass(frozen=True)
class ManagedSpec:
 messages:int;rate:int;payload_bytes:int;entities:int;duration:int;group:str;extra_args:tuple[str,...]=()
 def public(self):
  value=asdict(self);value["extra_args"]=list(self.extra_args);return value

def _strict_int(value,name):
 if isinstance(value,bool) or isinstance(value,float) or not isinstance(value,(int,str)):raise ValueError(f"{name} must be an integer")
 if isinstance(value,str):
  if not value or value.strip()!=value or not value.isdigit():raise ValueError(f"{name} must be an integer")
  value=int(value)
 if not math.isfinite(value):raise ValueError(f"{name} must be finite")
 return value

def managed_spec(values):
 extras=values.get("extra_args",())
 if not isinstance(extras,(list,tuple)) or any(not isinstance(x,str) for x in extras):raise ValueError("extra_args must be strings")
 spec=ManagedSpec(_strict_int(values.get("messages",100),"messages"),_strict_int(values.get("rate",25),"rate"),_strict_int(values.get("payload_bytes",256),"payload_bytes"),_strict_int(values.get("entities",64),"entities"),_strict_int(values.get("duration",90),"duration"),str(values.get("group","events-a")),tuple(extras))
 for name,value,low,high in (("messages",spec.messages,1,MAX_MESSAGES),("rate",spec.rate,1,MAX_RATE),("payload bytes",spec.payload_bytes,1,MAX_PAYLOAD_BYTES),("entity count",spec.entities,1,MAX_ENTITIES),("duration",spec.duration,MIN_DURATION,MAX_DURATION)):
  if not low<=value<=high:raise ValueError(f"{name} must be {low}..{high}")
 if spec.group not in ("events-a","events-b"):raise ValueError("group must be events-a or events-b")
 minimum=math.ceil(spec.messages/spec.rate)+20
 if spec.duration<minimum:raise ValueError(f"duration must be at least {minimum}s for {spec.messages} messages at {spec.rate}/s plus completion time")
 for arg in spec.extra_args:
  if not _allowed_extra(arg):raise ValueError(f"unsupported managed SDKPerf option: {arg}")
 return spec

def template_spec(name,overrides=None):
 if name not in TEMPLATES:raise ValueError("unknown SDKPerf template")
 values=dict(TEMPLATES[name]);values.pop("name");values.update(overrides or {});return managed_spec(values)

def harness_argv(harness,spec):
 argv=[harness,"--messages",str(spec.messages),"--rate",str(spec.rate),"--payload-bytes",str(spec.payload_bytes),"--entities",str(spec.entities),"--seconds",str(spec.duration),"--group",spec.group]
 argv.extend("--producer-arg="+arg for arg in spec.extra_args)
 return argv

def managed_preview(spec):
 suffix=(" "+" ".join(spec.extra_args)) if spec.extra_args else ""
 return f"sdkperf_java.sh -mn={spec.messages} -mr={spec.rate} -msa={spec.payload_bytes}{suffix}  # managed: group={spec.group}, entities={spec.entities}, timeout={spec.duration}s"

def parse_console(command,ui_mode="managed",ui_options=None):
 if not isinstance(command,str) or not command.strip():raise ValueError("enter an SDKPerf command")
 if len(command)>4096:raise ValueError("command exceeds 4096 characters")
 if any(c in command for c in _FORBIDDEN_CHARS):raise ValueError("shell operators, substitution, and redirection are not supported")
 try:argv=shlex.split(command,posix=True)
 except ValueError as exc:raise ValueError(f"invalid quoting: {exc}") from exc
 if not argv or argv[0].split("/")[-1].lower() not in ("sdkperf","sdkperf_java.sh"):raise ValueError("command must start with sdkperf_java.sh")
 if len(argv)==2 and argv[1] in ("-h","-?","-hm","-he","-v"):return "info",(argv[1],)
 if ui_mode not in ("managed","direct"):raise ValueError("mode must be managed or direct")
 if ui_mode=="direct":raise ValueError("direct execution is limited to the fixed dedicated-topic examples; use managed mode here")
 values=dict(ui_options or {});values.setdefault("extra_args",[])
 mapping={"-mn":"messages","-mr":"rate","-msa":"payload_bytes"}
 for arg in argv[1:]:
  name,sep,value=arg.partition("=")
  if any(name==p or name.startswith(p) for p in _BLOCKED_PREFIXES):raise ValueError(f"blocked connection, credential, destination, file, JVM, profile, or retry option: {name}")
  if sep and name in mapping:values[mapping[name]]=value
  elif _allowed_extra(arg):values["extra_args"].append(arg)
  else:raise ValueError(f"unsupported SDKPerf option: {arg}; Help lists supported and blocked categories")
 return "managed",managed_spec(values)

def info_argv(sdkperf,parsed):return [sdkperf,*parsed]
def redact(text,secrets):
 for secret in secrets:
  if secret:text=text.replace(secret,"<redacted>")
 return text

def _allowed_extra(arg):
 if arg in ("-soe","-psm","-l","-mt=persistent"):return True
 for prefix,low,high in (("-ped=",0,30),("-lb=",1,4096),("-lg=",0,1000),("-psv=",1,50)):
  if arg.startswith(prefix):
   try:value=_strict_int(arg[len(prefix):],prefix[:-1])
   except ValueError:return False
   return low<=value<=high
 return False
