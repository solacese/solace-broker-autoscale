import unittest
from sdkperf_control import *

class SDKPerfControlTest(unittest.TestCase):
 def test_exact_four_templates_and_values(self):
  self.assertEqual(set(TEMPLATES),{"quick","steady","burst","large"});self.assertEqual((TEMPLATES["quick"]["messages"],TEMPLATES["quick"]["rate"],TEMPLATES["quick"]["payload_bytes"]),(100,25,256));self.assertEqual((TEMPLATES["steady"]["messages"],TEMPLATES["steady"]["rate"]),(1000,50));self.assertEqual((TEMPLATES["burst"]["messages"],TEMPLATES["burst"]["rate"]),(1000,100));self.assertEqual((TEMPLATES["large"]["messages"],TEMPLATES["large"]["payload_bytes"]),(100,8192))
 def test_bounds_and_strict_integer_validation(self):
  for field,value in (("messages",1001),("rate",101),("payload_bytes",8193),("entities",1001),("duration",301),("messages",True),("rate",2.5),("duration","NaN")):
   with self.subTest(field=field,value=value),self.assertRaises(ValueError):managed_spec({field:value})
 def test_duration_accounts_for_publish_time(self):
  with self.assertRaisesRegex(ValueError,"at least 120s"):managed_spec({"messages":1000,"rate":10,"duration":100})
 def test_values_reach_harness_argv_and_dash_args_remain_one_token(self):
  spec=managed_spec({"messages":321,"rate":44,"payload_bytes":2048,"entities":17,"duration":123,"group":"events-b","extra_args":["-l","-lb=64"]});argv=harness_argv("/fixed/harness",spec)
  for value in ("321","44","2048","17","123","events-b","--producer-arg=-l","--producer-arg=-lb=64"):self.assertIn(value,argv)
 def test_console_accepts_normal_sdkperf_syntax(self):
  mode,spec=parse_console("sdkperf_java.sh -mn=100 -mr=25 -msa=8192 -l -lb=64","managed",{"entities":12,"group":"events-b","duration":100})
  self.assertEqual(mode,"managed");self.assertEqual((spec.messages,spec.payload_bytes,spec.entities,spec.group),(100,8192,12,"events-b"));self.assertEqual(spec.extra_args,("-l","-lb=64"))
 def test_console_rejects_shell_connection_credentials_files_and_profiles(self):
  bad=("sdkperf_java.sh -mn=1 | id","sdkperf_java.sh $(id)","sdkperf_java.sh -cip=evil","sdkperf_java.sh -cp=secret","sdkperf_java.sh -pfl=/etc/passwd","sdkperf_java.sh -epl=hook","sdkperf_java.sh -Ddebug=true","bash -c id")
  for value in bad:
   with self.subTest(value=value),self.assertRaises(ValueError):parse_console(value,"managed",{})
 def test_direct_mode_rejected_with_clear_guidance(self):
  with self.assertRaisesRegex(ValueError,"fixed dedicated-topic examples"):parse_console("sdkperf_java.sh -mn=1","direct",{})
 def test_console_info_commands_only(self):
  for flag in ("-v","-h","-hm","-he"):self.assertEqual(parse_console("sdkperf_java.sh "+flag),("info",(flag,)))
 def test_redaction(self):self.assertEqual(redact("password=sensitive",("sensitive",)),"password=<redacted>")

if __name__=="__main__":unittest.main()
