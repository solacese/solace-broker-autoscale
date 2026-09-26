package cloud

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const (
	stateCreateIntent = "CREATE_INTENT"
	stateCreating     = "CREATING"
	stateReady        = "READY"
	stateDeleting     = "DELETING"
	stateDeleted      = "DELETED"
	stateAbsent       = "ABSENT"
)

// Lifecycle couples the Mission Control API with a durable ownership journal.
type Lifecycle struct {
	client       *Client
	journal      *Journal
	plan         QualificationPlan
	pollInterval time.Duration
}

// ServiceError scopes a lifecycle failure to one non-secret logical role.
type ServiceError struct {
	ServiceRole string
	Err         error
}

func (e *ServiceError) Error() string { return fmt.Sprintf("%s: %v", e.ServiceRole, e.Err) }
func (e *ServiceError) Unwrap() error { return e.Err }
func (e *ServiceError) Role() string  { return e.ServiceRole }

// OperationFailure reports a failed Mission Control operation without retaining
// service IDs, operation IDs, endpoints, or credentials.
type OperationFailure struct {
	OperationType string
	Message       string
}

func (e *OperationFailure) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("Mission Control %s operation failed", e.OperationType)
	}
	return fmt.Sprintf("Mission Control %s operation failed: %s", e.OperationType, e.Message)
}

func NewLifecycle(client *Client, journal *Journal, plan QualificationPlan, pollInterval time.Duration) (*Lifecycle, error) {
	if client == nil || journal == nil {
		return nil, errors.New("client and journal are required")
	}
	if client.TokenOwner() == "" {
		return nil, errors.New("client token owner is required for safe lifecycle reconciliation")
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	return &Lifecycle{client: client, journal: journal, plan: plan, pollInterval: pollInterval}, nil
}

// Ensure creates or recovers every service in the immutable qualification plan.
// The journal's create intent is fsync'd before the request is sent.
func (l *Lifecycle) Ensure(ctx context.Context) (map[string]Service, error) {
	type ensured struct {
		role    string
		service Service
		err     error
	}
	planned := l.plan.Services()
	results := make(chan ensured, len(planned))
	for _, service := range planned {
		service := service
		go func() {
			created, err := l.ensureOne(ctx, service)
			results <- ensured{role: service.Role, service: created, err: err}
		}()
	}
	services := make(map[string]Service, len(planned))
	var failures []error
	for range planned {
		result := <-results
		if result.err != nil {
			failures = append(failures, &ServiceError{ServiceRole: result.role, Err: result.err})
			continue
		}
		services[result.role] = result.service
	}
	if len(failures) != 0 {
		return services, errors.Join(failures...)
	}
	return services, nil
}

func (l *Lifecycle) ensureOne(ctx context.Context, planned PlannedService) (Service, error) {
	identity := planned.Request.Identity()
	record, exists := l.journal.Service(planned.Role)
	if exists {
		if !record.Identity.matchesPlan(identity) {
			return Service{}, errors.New("journal identity differs from immutable plan")
		}
		switch record.State {
		case stateReady:
			service, err := l.client.GetService(ctx, record.ServiceID)
			if err == nil {
				if err := verifyReadyService(service, planned); err != nil {
					return Service{}, err
				}
				return service, nil
			}
			if !IsHTTPStatus(err, http.StatusNotFound) {
				return Service{}, err
			}
			return Service{}, errors.New("journaled service ID no longer exists; refusing implicit replacement")
		case stateCreating:
			if record.ServiceID != "" && record.OperationID != "" {
				if _, err := l.client.WaitOperation(ctx, record.ServiceID, record.OperationID, l.pollInterval); err != nil {
					return Service{}, err
				}
				service, err := l.client.GetService(ctx, record.ServiceID)
				if err != nil {
					return Service{}, err
				}
				if mismatches := identity.mismatches(service); len(mismatches) != 0 || !ownedBy(service, l.client.TokenOwner()) {
					if len(mismatches) != 0 {
						return Service{}, fmt.Errorf("created service identity fields differ: %v", mismatches)
					}
					return Service{}, errors.New("created service owner differs from token owner")
				}
				if err := verifyReadyService(service, planned); err != nil {
					return Service{}, err
				}
				record.State = stateReady
				if err := l.journal.put(record); err != nil {
					return Service{}, err
				}
				return service, nil
			}
			return l.reconcileCreate(ctx, planned, record)
		case stateCreateIntent:
			return l.reconcileCreate(ctx, planned, record)
		case stateDeleting:
			return Service{}, errors.New("service cleanup is incomplete; finish cleanup before ensuring")
		case stateDeleted, stateAbsent:
			return Service{}, errors.New("service was deleted or proven absent; refusing implicit paid re-creation")
		default:
			return Service{}, fmt.Errorf("journal contains unknown state %q", record.State)
		}
	}

	key, err := randomKey()
	if err != nil {
		return Service{}, err
	}
	record = JournalService{Role: planned.Role, Identity: identity, State: stateCreateIntent, IdempotencyKey: key}
	if err := l.journal.put(record); err != nil {
		return Service{}, fmt.Errorf("persist create intent: %w", err)
	}

	op, err := l.client.CreateService(ctx, planned.Request, key)
	if err != nil {
		// The request may have reached Mission Control. Leave CREATE_INTENT in the
		// journal so the next run reconciles by exact identity and owner.
		return Service{}, fmt.Errorf("create request has uncertain outcome: %w", err)
	}
	if op.ID == "" || op.ResourceID == "" {
		return Service{}, errors.New("create response lacks operation ID or service resource ID")
	}
	record.State, record.OperationID, record.ServiceID = stateCreating, op.ID, op.ResourceID
	if err := l.journal.put(record); err != nil {
		return Service{}, err
	}
	if _, err := l.client.WaitOperation(ctx, record.ServiceID, record.OperationID, l.pollInterval); err != nil {
		return Service{}, err
	}
	service, err := l.client.GetService(ctx, record.ServiceID)
	if err != nil {
		return Service{}, err
	}
	if mismatches := planned.Request.Identity().mismatches(service); len(mismatches) != 0 || !ownedBy(service, l.client.TokenOwner()) {
		if len(mismatches) != 0 {
			return Service{}, fmt.Errorf("created service identity fields differ: %v", mismatches)
		}
		return Service{}, errors.New("created service owner differs from token owner")
	}
	if err := verifyReadyService(service, planned); err != nil {
		return Service{}, err
	}
	record.State = stateReady
	if err := l.journal.put(record); err != nil {
		return Service{}, err
	}
	return service, nil
}

func (l *Lifecycle) reconcileCreate(ctx context.Context, planned PlannedService, record JournalService) (Service, error) {
	exact, found, err := l.findServiceByIdentity(ctx, planned.Request.Identity())
	if err != nil {
		return Service{}, err
	}
	if !found {
		if l.client.idempotencyHeader == "" {
			return Service{}, errors.New("create remains unresolved after reconciliation; Mission Control v2 documents no idempotency header, so refusing an unsafe retry")
		}
		// Retry only when idempotency support was explicitly configured, using the
		// exact key persisted before the original request.
		op, err := l.client.CreateService(ctx, planned.Request, record.IdempotencyKey)
		if err != nil {
			return Service{}, fmt.Errorf("retry uncertain create: %w", err)
		}
		if op.ID == "" || op.ResourceID == "" {
			return Service{}, errors.New("create response lacks operation ID or service resource ID")
		}
		record.State, record.OperationID, record.ServiceID = stateCreating, op.ID, op.ResourceID
		if err := l.journal.put(record); err != nil {
			return Service{}, err
		}
		if _, err := l.client.WaitOperation(ctx, record.ServiceID, record.OperationID, l.pollInterval); err != nil {
			return Service{}, err
		}
	} else {
		record.ServiceID = exact.ID
		if record.ServiceID == "" {
			return Service{}, errors.New("matching service has no ID")
		}
		if len(exact.OngoingOperationIDs) > 1 {
			return Service{}, errors.New("matching service has multiple ongoing operations; refusing ambiguous adoption")
		}
		if len(exact.OngoingOperationIDs) == 1 {
			record.State, record.OperationID = stateCreating, exact.OngoingOperationIDs[0]
			if err := l.journal.put(record); err != nil {
				return Service{}, err
			}
			if _, err := l.client.WaitOperation(ctx, record.ServiceID, record.OperationID, l.pollInterval); err != nil {
				return Service{}, err
			}
		}
	}
	service, err := l.client.GetService(ctx, record.ServiceID)
	if err != nil {
		return Service{}, err
	}
	if mismatches := planned.Request.Identity().mismatches(service); len(mismatches) != 0 || !ownedBy(service, l.client.TokenOwner()) {
		if len(mismatches) != 0 {
			return Service{}, fmt.Errorf("reconciled service identity fields differ: %v", mismatches)
		}
		return Service{}, errors.New("reconciled service owner differs from token owner")
	}
	if err := verifyReadyService(service, planned); err != nil {
		return Service{}, err
	}
	record.State, record.OperationID = stateReady, ""
	if err := l.journal.put(record); err != nil {
		return Service{}, err
	}
	return service, nil
}

// findServiceByIdentity performs bounded eventual-consistency reconciliation.
// It returns found=false only after three clean observations with no same-name
// service. Any collision or ambiguous match fails closed.
func (l *Lifecycle) findServiceByIdentity(ctx context.Context, identity ServiceIdentity) (Service, bool, error) {
	for attempt := 0; attempt < 3; attempt++ {
		services, err := l.client.ListServices(ctx)
		if err != nil {
			return Service{}, false, fmt.Errorf("list services for uncertain-create reconciliation: %w", err)
		}
		var exact []Service
		var collisions []Service
		for _, summary := range services {
			if summary.Name != identity.Name {
				continue
			}
			if summary.ID == "" {
				collisions = append(collisions, summary)
				continue
			}
			// Broker settings and endpoint states are expansion-only; fetch the full
			// service before comparing the complete identity.
			service, getErr := l.client.GetService(ctx, summary.ID)
			if getErr != nil {
				return Service{}, false, fmt.Errorf("get same-name service %q for reconciliation: %w", summary.ID, getErr)
			}
			if identity.matches(service) && ownedBy(service, l.client.TokenOwner()) {
				exact = append(exact, service)
			} else {
				collisions = append(collisions, service)
			}
		}
		if len(exact) > 1 {
			return Service{}, false, fmt.Errorf("refusing adoption: %d exact identity and owner matches exist", len(exact))
		}
		if len(collisions) > 0 {
			return Service{}, false, errors.New("refusing adoption: service name collision has different identity or owner")
		}
		if len(exact) == 1 {
			return exact[0], true, nil
		}
		if attempt < 2 {
			if err := waitContext(ctx, l.pollInterval*time.Duration(attempt+1)); err != nil {
				return Service{}, false, err
			}
		}
	}
	return Service{}, false, nil
}

func ownedBy(service Service, owner string) bool {
	return owner != "" && (service.OwnedBy == owner || service.CreatedBy == owner)
}

func verifyReadyService(service Service, planned PlannedService) error {
	if service.CreationState != "COMPLETED" {
		return fmt.Errorf("service %q creation state is %q, want COMPLETED", service.ID, service.CreationState)
	}
	if service.AdminState != "START" {
		return fmt.Errorf("service %q admin state is %q, want START", service.ID, service.AdminState)
	}
	if len(service.ConnectionEndpoints) == 0 {
		return fmt.Errorf("service %q has no connection endpoints", service.ID)
	}
	for _, endpoint := range service.ConnectionEndpoints {
		if endpoint.ID == "" || endpoint.CreationState != "completed" {
			return fmt.Errorf("service %q endpoint %q creation state is %q, want completed", service.ID, endpoint.ID, endpoint.CreationState)
		}
	}
	if len(planned.Request.ServiceConnectionEndpoints) > 0 {
		for _, wanted := range planned.Request.ServiceConnectionEndpoints {
			found := false
			for _, endpoint := range service.ConnectionEndpoints {
				if endpoint.Name == wanted.Name && endpoint.AccessType == wanted.AccessType {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("service %q is missing requested %s endpoint %q", service.ID, wanted.AccessType, wanted.Name)
			}
		}
	}
	return nil
}

// Cleanup deletes only IDs persisted in this journal, treats 404 as success,
// and verifies every exact ID is absent. It never discovers deletion targets by
// name or prefix.
func (l *Lifecycle) Cleanup(ctx context.Context) error {
	services := l.journal.Services()
	// Resolve every intent before deleting anything. This avoids both stranding a
	// service accepted after a lost response and partially cleaning a plan before
	// discovering an ambiguous identity.
	for _, planned := range l.plan.Services() {
		record, ok := services[planned.Role]
		if !ok || record.State != stateCreateIntent {
			continue
		}
		if !record.Identity.matchesPlan(planned.Identity()) {
			return fmt.Errorf("cleanup %s: journal identity differs from plan", planned.Role)
		}
		service, found, err := l.findServiceByIdentity(ctx, record.Identity)
		if err != nil {
			return fmt.Errorf("cleanup %s: reconcile create intent: %w", planned.Role, err)
		}
		if !found {
			// Mission Control listings are eventually consistent. An unresolved create
			// intent can never be proven absent strongly enough to discard its cleanup
			// journal automatically; retain it for an explicit later retry.
			return fmt.Errorf("cleanup %s: create intent remains unresolved; retry cleanup later", planned.Role)
		}
		record.State, record.ServiceID = stateReady, service.ID
		if len(service.OngoingOperationIDs) == 1 {
			record.State, record.OperationID = stateCreating, service.OngoingOperationIDs[0]
		} else if len(service.OngoingOperationIDs) > 1 {
			return fmt.Errorf("cleanup %s: matching service has multiple ongoing operations", planned.Role)
		}
		if err := l.journal.put(record); err != nil {
			return fmt.Errorf("cleanup %s: persist reconciled service ID: %w", planned.Role, err)
		}
		services[planned.Role] = record
	}
	for index := len(l.plan.Services()) - 1; index >= 0; index-- {
		planned := l.plan.Services()[index]
		record, ok := services[planned.Role]
		if !ok || record.State == stateDeleted || record.State == stateAbsent {
			continue
		}
		if !record.Identity.matchesPlan(planned.Request.Identity()) {
			return fmt.Errorf("cleanup %s: journal identity differs from plan", planned.Role)
		}
		if record.ServiceID == "" {
			return fmt.Errorf("cleanup %s: unresolved %s has no exact service ID; reconcile create before cleanup", planned.Role, record.State)
		}
		if record.State == stateDeleting && record.OperationID != "" {
			if _, err := l.client.WaitOperation(ctx, record.ServiceID, record.OperationID, l.pollInterval); err != nil && !IsHTTPStatus(err, http.StatusNotFound) {
				return fmt.Errorf("cleanup %s: %w", planned.Role, err)
			}
			if err := l.verifyAbsent(ctx, record.ServiceID); err != nil {
				return fmt.Errorf("cleanup %s: %w", planned.Role, err)
			}
			record.State, record.OperationID = stateDeleted, ""
			if err := l.journal.put(record); err != nil {
				return err
			}
			continue
		}
		if record.State == stateCreating && record.OperationID != "" {
			if _, err := l.client.WaitOperation(ctx, record.ServiceID, record.OperationID, l.pollInterval); err != nil && !IsHTTPStatus(err, http.StatusNotFound) {
				return fmt.Errorf("cleanup %s: await create before delete: %w", planned.Role, err)
			}
			record.OperationID = ""
			if err := l.journal.put(record); err != nil {
				return err
			}
		}
		service, err := l.client.GetService(ctx, record.ServiceID)
		if IsHTTPStatus(err, http.StatusNotFound) {
			record.State, record.OperationID = stateDeleted, ""
			if err := l.journal.put(record); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("cleanup %s: verify journaled service: %w", planned.Role, err)
		}
		if mismatches := record.Identity.mismatches(service); len(mismatches) != 0 || !ownedBy(service, l.client.TokenOwner()) {
			if len(mismatches) != 0 {
				return fmt.Errorf("cleanup %s: journaled service identity fields differ: %v", planned.Role, mismatches)
			}
			return fmt.Errorf("cleanup %s: journaled service owner differs from token owner", planned.Role)
		}
		op, err := l.client.DeleteService(ctx, record.ServiceID, record.IdempotencyKey+"-delete")
		if IsHTTPStatus(err, http.StatusNotFound) {
			record.State, record.OperationID = stateDeleted, ""
			if err := l.journal.put(record); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("cleanup %s: %w", planned.Role, err)
		}
		record.State, record.OperationID = stateDeleting, op.ID
		if err := l.journal.put(record); err != nil {
			return err
		}
		if op.ID != "" {
			if _, err := l.client.WaitOperation(ctx, record.ServiceID, op.ID, l.pollInterval); err != nil && !IsHTTPStatus(err, http.StatusNotFound) {
				return fmt.Errorf("cleanup %s: %w", planned.Role, err)
			}
		}
		if err := l.verifyAbsent(ctx, record.ServiceID); err != nil {
			return fmt.Errorf("cleanup %s: %w", planned.Role, err)
		}
		record.State, record.OperationID = stateDeleted, ""
		if err := l.journal.put(record); err != nil {
			return err
		}
	}
	return nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (l *Lifecycle) verifyAbsent(ctx context.Context, serviceID string) error {
	for attempt := 0; attempt < 3; attempt++ {
		_, err := l.client.GetService(ctx, serviceID)
		if IsHTTPStatus(err, http.StatusNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if attempt < 2 {
			if err := waitContext(ctx, l.pollInterval); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("service ID %q still exists after deletion", serviceID)
}

func randomKey() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("generate idempotency key: %w", err)
	}
	return hex.EncodeToString(data[:]), nil
}

func IsHTTPStatus(err error, status int) bool {
	var httpError *HTTPError
	return errors.As(err, &httpError) && httpError.StatusCode == status
}
