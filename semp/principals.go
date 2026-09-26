package semp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// ClientProfileSpec is the deliberately narrow client profile managed for
// workload-balancer publishers and consumers.
type ClientProfileSpec struct {
	MessageVPN                                    string
	Name                                          string
	AllowGuaranteedMsgSendEnabled                 bool
	AllowGuaranteedMsgReceiveEnabled              bool
	AllowGuaranteedEndpointCreateEnabled          bool
	RejectMsgToSenderOnNoSubscriptionMatchEnabled bool
}

// ACLProfileSpec is a deny-by-default ACL profile. Exceptions are deliberately
// not accepted here; managed clients should receive only the explicitly chosen
// defaults.
type ACLProfileSpec struct {
	MessageVPN                      string
	Name                            string
	ClientConnectDefaultAction      string
	PublishTopicDefaultAction       string
	SubscribeTopicDefaultAction     string
	SubscribeShareNameDefaultAction string
}

// ClientUsernameSpec binds one exact username to the managed profiles. Password
// provisioning is intentionally excluded so credentials never enter SEMP
// request bodies or errors through this package.
type ClientUsernameSpec struct {
	MessageVPN        string
	Name              string
	Enabled           bool
	ClientProfileName string
	ACLProfileName    string
}

type clientProfileConfig struct {
	MessageVPN                                    string `json:"msgVpnName,omitempty"`
	Name                                          string `json:"clientProfileName"`
	AllowGuaranteedMsgSendEnabled                 bool   `json:"allowGuaranteedMsgSendEnabled"`
	AllowGuaranteedMsgReceiveEnabled              bool   `json:"allowGuaranteedMsgReceiveEnabled"`
	AllowGuaranteedEndpointCreateEnabled          bool   `json:"allowGuaranteedEndpointCreateEnabled"`
	RejectMsgToSenderOnNoSubscriptionMatchEnabled bool   `json:"rejectMsgToSenderOnNoSubscriptionMatchEnabled"`
}

type aclProfileConfig struct {
	MessageVPN                      string `json:"msgVpnName,omitempty"`
	Name                            string `json:"aclProfileName"`
	ClientConnectDefaultAction      string `json:"clientConnectDefaultAction"`
	PublishTopicDefaultAction       string `json:"publishTopicDefaultAction"`
	SubscribeTopicDefaultAction     string `json:"subscribeTopicDefaultAction"`
	SubscribeShareNameDefaultAction string `json:"subscribeShareNameDefaultAction"`
}

type clientUsernameConfig struct {
	MessageVPN        string `json:"msgVpnName,omitempty"`
	Name              string `json:"clientUsername"`
	Enabled           bool   `json:"enabled"`
	ClientProfileName string `json:"clientProfileName"`
	ACLProfileName    string `json:"aclProfileName"`
}

func profilePath(vpn, collection, name string) string {
	return pathSegments("SEMP", "v2", "config", "msgVpns", vpn, collection, name)
}

func profileCollectionPath(vpn, collection string) string {
	return pathSegments("SEMP", "v2", "config", "msgVpns", vpn, collection)
}

// EnsureClientProfile creates or exactly adopts a narrow managed profile.
func (c *Client) EnsureClientProfile(ctx context.Context, spec ClientProfileSpec) error {
	if spec.MessageVPN == "" || spec.Name == "" {
		return errors.New("semp: client profile message VPN and name are required")
	}
	want := clientProfileConfig{
		MessageVPN:                           spec.MessageVPN,
		Name:                                 spec.Name,
		AllowGuaranteedMsgSendEnabled:        spec.AllowGuaranteedMsgSendEnabled,
		AllowGuaranteedMsgReceiveEnabled:     spec.AllowGuaranteedMsgReceiveEnabled,
		AllowGuaranteedEndpointCreateEnabled: spec.AllowGuaranteedEndpointCreateEnabled,
		RejectMsgToSenderOnNoSubscriptionMatchEnabled: spec.RejectMsgToSenderOnNoSubscriptionMatchEnabled,
	}
	return ensureExact(ctx, c, "client profile", spec.Name, profileCollectionPath(spec.MessageVPN, "clientProfiles"), profilePath(spec.MessageVPN, "clientProfiles", spec.Name), want)
}

// EnsureACLProfile creates or exactly adopts a managed ACL profile.
func (c *Client) EnsureACLProfile(ctx context.Context, spec ACLProfileSpec) error {
	if spec.MessageVPN == "" || spec.Name == "" {
		return errors.New("semp: ACL profile message VPN and name are required")
	}
	for _, action := range []string{spec.ClientConnectDefaultAction, spec.PublishTopicDefaultAction, spec.SubscribeTopicDefaultAction, spec.SubscribeShareNameDefaultAction} {
		if action != "allow" && action != "disallow" {
			return errors.New("semp: ACL profile actions must be allow or disallow")
		}
	}
	want := aclProfileConfig{
		MessageVPN:                      spec.MessageVPN,
		Name:                            spec.Name,
		ClientConnectDefaultAction:      spec.ClientConnectDefaultAction,
		PublishTopicDefaultAction:       spec.PublishTopicDefaultAction,
		SubscribeTopicDefaultAction:     spec.SubscribeTopicDefaultAction,
		SubscribeShareNameDefaultAction: spec.SubscribeShareNameDefaultAction,
	}
	return ensureExact(ctx, c, "ACL profile", spec.Name, profileCollectionPath(spec.MessageVPN, "aclProfiles"), profilePath(spec.MessageVPN, "aclProfiles", spec.Name), want)
}

// EnsureClientUsername creates or exactly adopts a managed client username.
func (c *Client) EnsureClientUsername(ctx context.Context, spec ClientUsernameSpec) error {
	if spec.MessageVPN == "" || spec.Name == "" || spec.ClientProfileName == "" || spec.ACLProfileName == "" {
		return errors.New("semp: client username, message VPN, client profile, and ACL profile are required")
	}
	want := clientUsernameConfig{
		MessageVPN:        spec.MessageVPN,
		Name:              spec.Name,
		Enabled:           spec.Enabled,
		ClientProfileName: spec.ClientProfileName,
		ACLProfileName:    spec.ACLProfileName,
	}
	return ensureExact(ctx, c, "client username", spec.Name, profileCollectionPath(spec.MessageVPN, "clientUsernames"), profilePath(spec.MessageVPN, "clientUsernames", spec.Name), want)
}

func ensureExact[T comparable](ctx context.Context, c *Client, kind, name, collectionPath, objectPath string, want T) error {
	get := func() (bool, error) {
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}
		if err := c.request(ctx, http.MethodGet, objectPath, nil, &envelope); err != nil {
			return false, err
		}
		return exactManagedConfig(envelope.Data, want)
	}
	match, err := get()
	if err == nil {
		if match {
			return nil
		}
		return fmt.Errorf("semp: managed %s %q exists with incompatible configuration", kind, name)
	}
	if !isNotFound(err) {
		return err
	}
	if err := c.request(ctx, http.MethodPost, collectionPath, want, nil); err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("semp: create managed %s %q: %w", kind, name, err)
	}
	match, err = get()
	if err != nil {
		return fmt.Errorf("semp: verify managed %s %q: %w", kind, name, err)
	}
	if !match {
		return fmt.Errorf("semp: managed %s %q exists with incompatible configuration", kind, name)
	}
	return nil
}

// ProvisionApplication ensures the ACL profile, client profile, then username.
func (c *Client) ProvisionApplication(ctx context.Context, acl ACLProfileSpec, profile ClientProfileSpec, username ClientUsernameSpec) error {
	if acl.MessageVPN != profile.MessageVPN || acl.MessageVPN != username.MessageVPN ||
		acl.Name != username.ACLProfileName || profile.Name != username.ClientProfileName {
		return errors.New("semp: application profile references are inconsistent")
	}
	if err := c.EnsureACLProfile(ctx, acl); err != nil {
		return err
	}
	if err := c.EnsureClientProfile(ctx, profile); err != nil {
		return err
	}
	return c.EnsureClientUsername(ctx, username)
}

// DeleteClientProfile deletes one exact profile name, idempotently.
func (c *Client) DeleteClientProfile(ctx context.Context, messageVPN, name string) error {
	return c.deleteExact(ctx, "client profile", messageVPN, name, "clientProfiles")
}

// DeleteACLProfile deletes one exact profile name, idempotently.
func (c *Client) DeleteACLProfile(ctx context.Context, messageVPN, name string) error {
	return c.deleteExact(ctx, "ACL profile", messageVPN, name, "aclProfiles")
}

// DeleteClientUsername deletes one exact username, idempotently.
func (c *Client) DeleteClientUsername(ctx context.Context, messageVPN, name string) error {
	return c.deleteExact(ctx, "client username", messageVPN, name, "clientUsernames")
}

func (c *Client) deleteExact(ctx context.Context, kind, vpn, name, collection string) error {
	if vpn == "" || name == "" {
		return fmt.Errorf("semp: message VPN and exact %s name are required", kind)
	}
	if err := c.request(ctx, http.MethodDelete, profilePath(vpn, collection, name), nil, nil); err != nil && !isNotFound(err) {
		return fmt.Errorf("semp: delete %s %q: %w", kind, name, err)
	}
	return nil
}
