# ADR 0001: Solace Cloud managed services only

## Status
Accepted.

## Context
Horizontal scaling of Solace brokers can be pursued in many environments: self-managed software
brokers, a Kubernetes operator with CRDs, or the managed Solace Cloud offering. Supporting all of
them multiplies the surface area of the actuator, the safety model, and the config schema.

The concrete problem this tool solves is: a customer on Solace Cloud has hit the ceiling of vertical
scaling (largest practical service class) and needs a horizontal path. That path is provisioning
additional managed services and steering traffic across them.

## Decision
Support **Solace Cloud managed services only**. No Kubernetes operator, no CRDs, no self-managed
broker lifecycle. The actuator speaks the Solace Cloud (Mission Control) REST API and nothing else.

## Consequences
- The actuator maps directly to Mission Control operations: create/delete service, change service
  class, update message spool, track async operations.
- Broker capacity is expressed in terms of Cloud **service classes**, which is exactly what the
  performance workbooks are indexed by.
- We do not carry Helm charts, operators, or SEMPv1 provisioning code.
- If self-managed support is ever needed it is a separate tool, not a mode of this one.
