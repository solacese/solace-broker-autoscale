package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/solacese/solace-workload-balancer/broker0"
	"github.com/solacese/solace-workload-balancer/control"
	"github.com/solacese/solace-workload-balancer/controller"
)

// SnapshotFanoutPublisher stores each snapshot on the retained topic and then
// emits the same complete snapshot on the live-update topic. The operation IDs
// differ, so a crash between publications resumes only the missing copy.
type SnapshotFanoutPublisher struct {
	Publisher *broker0.PersistentControlPublisher
	Names     control.ManagedNames
}

func (p *SnapshotFanoutPublisher) Publish(ctx context.Context, update controller.ControlUpdate) error {
	if p == nil || p.Publisher == nil {
		return errors.New("runtime: snapshot fanout publisher is not initialized")
	}
	if err := p.Publisher.Publish(ctx, update); err != nil {
		return err
	}
	return p.publishUpdate(ctx, update.OperationID, update.Snapshot)
}

func (p *SnapshotFanoutPublisher) PublishSnapshot(ctx context.Context, snapshot control.MembershipSnapshot) error {
	if p == nil || p.Publisher == nil {
		return errors.New("runtime: snapshot fanout publisher is not initialized")
	}
	operationID := fmt.Sprintf("%s/snapshot/%d/%d/%s", snapshot.ScalingGroup, snapshot.Revision, snapshot.Epoch, snapshot.Phase)
	if err := p.Publisher.PublishSnapshotOperation(ctx, operationID, snapshot); err != nil {
		return err
	}
	return p.publishUpdate(ctx, operationID, snapshot)
}

func (p *SnapshotFanoutPublisher) publishUpdate(ctx context.Context, operationID string, snapshot control.MembershipSnapshot) error {
	topic, err := p.Names.UpdateTopic(snapshot.ScalingGroup)
	if err != nil {
		return err
	}
	return p.Publisher.PublishSnapshotTo(ctx, topic, operationID+"/update", snapshot)
}

func (p *SnapshotFanoutPublisher) PublishCommand(ctx context.Context, command control.CommandEnvelope) error {
	return p.Publisher.PublishCommand(ctx, command)
}
