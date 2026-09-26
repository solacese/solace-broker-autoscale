package qualification

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/solacese/solace-workload-balancer/cloud"
	"github.com/solacese/solace-workload-balancer/integration"
	shimPublisher "github.com/solacese/solace-workload-balancer/shim/publisher"
	shimSubscriber "github.com/solacese/solace-workload-balancer/shim/subscriber"
	"solace.dev/go/messaging/pkg/solace"
)

func trustStorePath() string {
	if configured := os.Getenv("SSL_CERT_DIR"); configured != "" {
		return configured
	}
	for _, candidate := range []string{"/etc/ssl/certs", "/etc/ssl"} {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
	}
	return ""
}

type sempFactory struct{}

func (sempFactory) New(bundle cloud.ConnectionBundle) (QueueAPI, error) {
	return newSEMPClient(bundle)
}

// nativeSession owns all native messaging services and publishers opened for a run.
type nativeSession struct {
	services        map[string]solace.MessagingService
	publishers      map[string]*integration.GuaranteedPublisher
	asyncPublishers map[string]*integration.AsyncPersistentPublisher
	factory         integration.ConsumerFactory
	once            sync.Once
	closeErr        error
}

func (s *nativeSession) BrokerPublisher() shimPublisher.BrokerPublisher {
	return integration.PublisherAdapter{Publishers: s.publishers, PartitionPolicy: qualificationPartitionPolicy()}
}
func (s *nativeSession) AsyncBrokerPublisher() shimPublisher.AsyncBrokerPublisher {
	return integration.AsyncPublisherAdapter{Publishers: s.asyncPublishers, PartitionPolicy: qualificationPartitionPolicy()}
}

func qualificationPartitionPolicy() integration.PartitionPolicy {
	return integration.PartitionPolicy{FlightGroup: true, BaggageGroup: false}
}
func (s *nativeSession) ConsumerFactory() shimSubscriber.ConsumerFactory         { return s.factory }
func (s *nativeSession) Publishers() map[string]*integration.GuaranteedPublisher { return s.publishers }
func (s *nativeSession) AsyncPublishers() map[string]*integration.AsyncPersistentPublisher {
	return s.asyncPublishers
}
func (s *nativeSession) NativeConsumerFactory() integration.ConsumerFactory { return s.factory }
func (s *nativeSession) Close() error {
	s.once.Do(func() {
		var mu sync.Mutex
		var errs []error
		record := func(err error) {
			if err == nil {
				return
			}
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
		var publishers sync.WaitGroup
		for role, publisher := range s.publishers {
			role, publisher := role, publisher
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				record(wrapCloseError("publisher", role, publisher.Close()))
			}()
		}
		for role, publisher := range s.asyncPublishers {
			role, publisher := role, publisher
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				record(wrapCloseError("asynchronous publisher", role, publisher.Close()))
			}()
		}
		publishers.Wait()

		var services sync.WaitGroup
		for role, service := range s.services {
			role, service := role, service
			services.Add(1)
			go func() {
				defer services.Done()
				record(wrapCloseError("service", role, service.Disconnect()))
			}()
		}
		services.Wait()
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

func wrapCloseError(kind, role string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close %s %s: %w", role, kind, err)
}

type nativeDataPlane struct {
	timeout              time.Duration
	maxInFlightPerBroker int
	partitionCount       uint32
}

func (n nativeDataPlane) Open(ctx context.Context, bundles Bundles, namespace string) (Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s := &nativeSession{services: make(map[string]solace.MessagingService, 3), publishers: make(map[string]*integration.GuaranteedPublisher, 3), asyncPublishers: make(map[string]*integration.AsyncPersistentPublisher, 3)}
	type opened struct {
		role      string
		service   solace.MessagingService
		publisher *integration.GuaranteedPublisher
		async     *integration.AsyncPersistentPublisher
		err       error
	}
	roles := [3]string{"broker-a", "broker-b", "broker-c"}
	results := make(chan opened, len(roles))
	for index, role := range roles {
		bundle := bundles.Data[index]
		go func() {
			result := opened{role: role}
			service, err := integration.ConnectContext(ctx, integration.Connection{Host: bundle.SMFHosts[0], MessageVPN: bundle.MessageVPN, Username: bundle.ServiceCredential.Username, Password: bundle.ServiceCredential.Password, ApplicationID: namespace + "-qualification-" + role, TrustStorePath: trustStorePath()})
			if err != nil {
				result.err = err
				results <- result
				return
			}
			result.service = service
			publisher, err := integration.NewGuaranteedPublisher(service, n.timeout)
			if err != nil {
				_ = service.Disconnect()
				result.err = err
				results <- result
				return
			}
			result.publisher = publisher
			asyncPublisher, err := integration.NewAsyncPersistentPublisher(service, uint(n.maxInFlightPerBroker))
			if err != nil {
				_ = publisher.Close()
				_ = service.Disconnect()
				result.err = err
				results <- result
				return
			}
			result.async = asyncPublisher
			results <- result
		}()
	}
	var openErrors []error
	for range roles {
		result := <-results
		if result.err != nil {
			openErrors = append(openErrors, fmt.Errorf("open %s: %w", result.role, result.err))
			continue
		}
		s.services[result.role] = result.service
		s.publishers[result.role] = result.publisher
		s.asyncPublishers[result.role] = result.async
	}
	if err := errors.Join(openErrors...); err != nil {
		_ = s.Close()
		return nil, err
	}
	s.factory = qualificationConsumerFactory(s.services, n.partitionCount)
	return s, nil
}

func qualificationConsumerFactory(services map[string]solace.MessagingService, partitionCount uint32) integration.ConsumerFactory {
	flows := int(partitionCount)
	if flows < 1 {
		flows = 1
	}
	return integration.ConsumerFactory{
		Services:        services,
		ExclusiveGroups: map[string]bool{BaggageGroup: true},
		ConsumersPerBinding: map[string]int{
			FlightGroup:  flows,
			BaggageGroup: 1,
		},
	}
}
