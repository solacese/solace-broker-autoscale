package runtime

import (
	"strings"
	"testing"

	"github.com/solacese/solace-workload-balancer/config"
)

func credentialConfig() config.Config {
	return config.Config{
		Control: config.ControlBroker{
			Principal: "cu", UsernameEnv: "CONTROL_USER", PasswordEnv: "CONTROL_PASS",
			SEMPUsernameEnv: "CONTROL_SEMP_USER", SEMPPasswordEnv: "CONTROL_SEMP_PASS",
		},
		DataBrokers: []config.DataBroker{{
			ID: "broker-a", UsernameEnv: "DATA_USER", PasswordEnv: "DATA_PASS",
			SEMPUsernameEnv: "DATA_SEMP_USER", SEMPPasswordEnv: "DATA_SEMP_PASS",
		}},
		Cloud: config.CloudProvisioning{Enabled: true, CredentialEnv: "CLOUD_TOKEN"},
	}
}

func TestResolveCredentials(t *testing.T) {
	values := map[string]string{
		"CONTROL_USER": "cu", "CONTROL_PASS": "cp", "CONTROL_SEMP_USER": "csu", "CONTROL_SEMP_PASS": "csp",
		"DATA_USER": "du", "DATA_PASS": "dp", "DATA_SEMP_USER": "dsu", "DATA_SEMP_PASS": "dsp", "CLOUD_TOKEN": "token",
	}
	credentials, err := ResolveCredentials(credentialConfig(), false, func(name string) (string, bool) { value, ok := values[name]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Control.SMFUsername != "cu" || credentials.DataBrokers["broker-a"].SEMPPassword != "dsp" || credentials.DataBrokers["broker-a"].SMFUsername != "" || credentials.CloudToken != "token" {
		t.Fatalf("unexpected resolved credentials: %#v", credentials)
	}
}

func TestResolveCredentialsFailsOnMissingEnvironment(t *testing.T) {
	_, err := ResolveCredentials(credentialConfig(), false, func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "CONTROL_USER") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateOnlyDoesNotReadEnvironment(t *testing.T) {
	called := false
	_, err := ResolveCredentials(credentialConfig(), true, func(string) (string, bool) { called = true; return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("validate-only resolved credentials")
	}
}

func TestResolveParticipantCredentialsUsesOnlySMFEnvironment(t *testing.T) {
	cfg := credentialConfig()
	cfg.Cloud.Enabled = false
	cfg.Control.ParticipantCredentials = []config.ParticipantControlCredential{{Participant: "participant-1", Principal: "cu", UsernameEnv: "CONTROL_USER", PasswordEnv: "CONTROL_PASS"}}
	values := map[string]string{"CONTROL_USER": "cu", "CONTROL_PASS": "cp", "DATA_USER": "du", "DATA_PASS": "dp"}
	credentials, err := ResolveParticipantCredentials(cfg, "participant-1", false, func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Control.SMFUsername != "cu" || credentials.DataBrokers["broker-a"].SMFUsername != "du" {
		t.Fatalf("unexpected participant credentials: %#v", credentials)
	}
	if credentials.Control.SEMPUsername != "" || credentials.DataBrokers["broker-a"].SEMPUsername != "" || credentials.CloudToken != "" {
		t.Fatal("participant resolver populated controller-only credentials")
	}
}

func TestResolveParticipantCredentialsRejectsUnknownParticipant(t *testing.T) {
	cfg := credentialConfig()
	cfg.Cloud.Enabled = false
	if _, err := ResolveParticipantCredentials(cfg, "intruder", false, func(string) (string, bool) { return "value", true }); err == nil {
		t.Fatal("unknown participant credentials were resolved")
	}
}

func TestResolveParticipantCredentialsRejectsPrincipalMismatchWithoutLeakingSecret(t *testing.T) {
	cfg := credentialConfig()
	cfg.Cloud.Enabled = false
	cfg.Control.ParticipantCredentials = []config.ParticipantControlCredential{{Participant: "participant-1", Principal: "principal-1", UsernameEnv: "PARTICIPANT_USER", PasswordEnv: "PARTICIPANT_PASS"}}
	secret := "secret-value-that-must-not-leak"
	_, err := ResolveParticipantCredentials(cfg, "participant-1", false, func(name string) (string, bool) {
		if name == "PARTICIPANT_USER" {
			return "different-principal", true
		}
		return secret, true
	})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("error = %v", err)
	}
}

func TestResolveParticipantCredentialsRejectsCloudProvisioning(t *testing.T) {
	cfg := credentialConfig()
	cfg.Control.ParticipantCredentials = []config.ParticipantControlCredential{{Participant: "participant-1", Principal: "value", UsernameEnv: "CONTROL_USER", PasswordEnv: "CONTROL_PASS"}}
	_, err := ResolveParticipantCredentials(cfg, "participant-1", false, func(string) (string, bool) { return "value", true })
	if err == nil || !strings.Contains(err.Error(), "do not provision cloud") {
		t.Fatalf("error = %v", err)
	}
}
