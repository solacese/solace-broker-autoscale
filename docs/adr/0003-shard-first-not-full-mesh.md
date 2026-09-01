# ADR 0003: Shard first, not full mesh

## Status
Accepted.

## Context
Two topologies can spread load across brokers:

- **Mesh**: brokers are linked (DMR) and any client can connect anywhere; messages cross links to
  reach subscribers on other brokers.
- **Sharded**: the topic space is partitioned so that publishers and subscribers of a branch land on
  the same broker, and no payload crosses a broker boundary.

Mesh is operationally simpler for clients but pays an amplification cost: a message consumed on a
broker other than where it was published crosses an inter-broker link, consuming link bandwidth and,
for guaranteed delivery, spool at each hop. With large payloads this cost dominates and horizontal
scaling stops helping.

## Decision
**Shard first.** The default topology is `sharded`. Mesh is supported but its cost is modelled
explicitly (§5.3): the decision engine adds inter-broker link traffic to the bytes axis and, for
guaranteed delivery, to spool pressure. Mesh is *meant* to look bad for large payloads, because it
is.

## Consequences
- The Event Portal shard advisor (§8) exists to find good shard boundaries: weakly connected
  components of the application↔topic bipartite graph, so a branch's publishers and subscribers stay
  co-located.
- Applications that span multiple components are reported explicitly, because they are what force
  `hybrid` mode.
- Mesh mode is a deliberate, costed choice, not the path of least resistance.
