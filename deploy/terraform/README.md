# Terraform: identical broker configuration

Templates the **configuration** of a broker (Message VPN, queues, ACL profiles, client profiles, DMR
links) so a newly-provisioned broker is configured identically to its peers. The actuator triggers
`terraform apply` for configuration rather than hand-rolling SEMPv2 calls (§10).

- Service **provisioning** (create/delete the managed service) is done by the actuator via the
  Mission Control API - Terraform here is for the *config that must match across the shard*.
- `variables.tf` declares the per-shard config (VPN name, queues, subscriptions, client profiles,
  ACL profiles, DMR cluster links).
- `main.tf` applies it via the Solace SEMPv2 provider (`registry.terraform.io/SolaceProducts/solacebroker`).
- `example.auto.tfvars` shows a fictional shard configuration.

## Why Terraform for config, API for provisioning

Provisioning is a small, async, safety-gated action the actuator owns. Configuration is a large,
declarative surface that must be *identical* across every broker in a shard - exactly what Terraform
is good at, and exactly where hand-rolled SEMPv2 drift causes the subtle "one broker behaves
differently" incidents. Keeping them separate keeps the actuator's blast radius small.

## Usage

```bash
cd deploy/terraform
terraform init
terraform apply -var-file=example.auto.tfvars \
  -var 'broker_url=https://<new-broker-host>:943' \
  -var 'broker_username=<admin>' -var 'broker_password=<from-secret-manager>'
```

Credentials are never committed and never vended by this project - supply them from your secret
manager at apply time.
