package runtime

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/solacese/solace-workload-balancer/config"
)

// Credentials contains resolved process-local secrets. It must not be logged or
// persisted. The configuration stores only the names of environment variables.
type Credentials struct {
	Control     BrokerCredentials
	DataBrokers map[string]BrokerCredentials
	CloudToken  string
}

type BrokerCredentials struct {
	Principal    string
	SMFUsername  string
	SMFPassword  string
	SEMPUsername string
	SEMPPassword string
}

type LookupEnv func(string) (string, bool)

// ResolveControllerCredentials resolves only credentials used by the controller:
// Broker 0 SMF/SEMP, data-broker SEMP, and an enabled cloud provider. Data-plane
// SMF credentials remain scoped to participant processes.
// validateOnly deliberately performs no environment lookup.
func ResolveControllerCredentials(cfg config.Config, validateOnly bool, lookup LookupEnv) (Credentials, error) {
	if validateOnly {
		return Credentials{}, nil
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	control, err := resolveBrokerCredentials("control", cfg.Control.UsernameEnv, cfg.Control.PasswordEnv, cfg.Control.SEMPUsernameEnv, cfg.Control.SEMPPasswordEnv, lookup)
	if err != nil {
		return Credentials{}, err
	}
	control.Principal = cfg.Control.Principal
	if control.Principal != "" && control.SMFUsername != control.Principal {
		return Credentials{}, errors.New("runtime: resolved controller Broker 0 username does not match configured principal")
	}
	resolved := Credentials{Control: control, DataBrokers: make(map[string]BrokerCredentials, len(cfg.DataBrokers))}
	for _, broker := range cfg.DataBrokers {
		sempUsername, resolveErr := requiredEnvironment("data broker "+broker.ID+" SEMP username", broker.SEMPUsernameEnv, lookup)
		if resolveErr != nil {
			return Credentials{}, resolveErr
		}
		sempPassword, resolveErr := requiredEnvironment("data broker "+broker.ID+" SEMP password", broker.SEMPPasswordEnv, lookup)
		if resolveErr != nil {
			return Credentials{}, resolveErr
		}
		resolved.DataBrokers[broker.ID] = BrokerCredentials{SEMPUsername: sempUsername, SEMPPassword: sempPassword}
	}
	if cfg.Cloud.Enabled {
		resolved.CloudToken, err = requiredEnvironment("cloud credential", cfg.Cloud.CredentialEnv, lookup)
		if err != nil {
			return Credentials{}, err
		}
	}
	return resolved, nil
}

// ResolveCredentials is retained as a convenience alias for controller process
// assembly.
func ResolveCredentials(cfg config.Config, validateOnly bool, lookup LookupEnv) (Credentials, error) {
	return ResolveControllerCredentials(cfg, validateOnly, lookup)
}

// ResolveParticipantCredentials resolves only the SMF credentials used by a
// publisher or subscriber process. It deliberately ignores SEMP and cloud
// credentials because participant startup must never provision infrastructure.
func ResolveParticipantCredentials(cfg config.Config, participant string, validateOnly bool, lookup LookupEnv) (Credentials, error) {
	credential, configured := cfg.ParticipantControlCredential(participant)
	if !configured {
		return Credentials{}, fmt.Errorf("runtime: no Broker 0 credential is configured for participant %q", participant)
	}
	if validateOnly {
		return Credentials{}, nil
	}
	if cfg.Cloud.Enabled {
		return Credentials{}, errors.New("runtime: participant commands do not provision cloud resources")
	}
	if lookup == nil {
		lookup = os.LookupEnv
	}
	controlCredentials, err := resolveSMFCredentials("participant "+participant+" control", credential.UsernameEnv, credential.PasswordEnv, lookup)
	if err != nil {
		return Credentials{}, err
	}
	controlCredentials.Principal = credential.Principal
	if controlCredentials.SMFUsername != credential.Principal {
		return Credentials{}, fmt.Errorf("runtime: resolved Broker 0 username for participant %q does not match configured principal", participant)
	}
	resolved := Credentials{Control: controlCredentials, DataBrokers: make(map[string]BrokerCredentials, len(cfg.DataBrokers))}
	for _, broker := range cfg.DataBrokers {
		credentials, resolveErr := resolveSMFCredentials("data broker "+broker.ID, broker.UsernameEnv, broker.PasswordEnv, lookup)
		if resolveErr != nil {
			return Credentials{}, resolveErr
		}
		resolved.DataBrokers[broker.ID] = credentials
	}
	return resolved, nil
}

func resolveSMFCredentials(label, usernameEnv, passwordEnv string, lookup LookupEnv) (BrokerCredentials, error) {
	username, err := requiredEnvironment(label+" SMF username", usernameEnv, lookup)
	if err != nil {
		return BrokerCredentials{}, err
	}
	password, err := requiredEnvironment(label+" SMF password", passwordEnv, lookup)
	if err != nil {
		return BrokerCredentials{}, err
	}
	return BrokerCredentials{SMFUsername: username, SMFPassword: password}, nil
}

func resolveBrokerCredentials(label, usernameEnv, passwordEnv, sempUsernameEnv, sempPasswordEnv string, lookup LookupEnv) (BrokerCredentials, error) {
	username, err := requiredEnvironment(label+" SMF username", usernameEnv, lookup)
	if err != nil {
		return BrokerCredentials{}, err
	}
	password, err := requiredEnvironment(label+" SMF password", passwordEnv, lookup)
	if err != nil {
		return BrokerCredentials{}, err
	}
	sempUsername, err := requiredEnvironment(label+" SEMP username", sempUsernameEnv, lookup)
	if err != nil {
		return BrokerCredentials{}, err
	}
	sempPassword, err := requiredEnvironment(label+" SEMP password", sempPasswordEnv, lookup)
	if err != nil {
		return BrokerCredentials{}, err
	}
	return BrokerCredentials{SMFUsername: username, SMFPassword: password, SEMPUsername: sempUsername, SEMPPassword: sempPassword}, nil
}

func requiredEnvironment(label, name string, lookup LookupEnv) (string, error) {
	if name == "" {
		return "", fmt.Errorf("runtime: %s environment variable is not configured", label)
	}
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("runtime: %s environment variable %q is unset or empty", label, name)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", errors.New("runtime: credential environment value contains NUL")
	}
	return value, nil
}
