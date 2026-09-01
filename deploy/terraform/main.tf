terraform {
  required_version = ">= 1.5"
  required_providers {
    solacebroker = {
      source  = "SolaceProducts/solacebroker"
      version = ">= 1.0"
    }
  }
}

provider "solacebroker" {
  url      = var.broker_url
  username = var.broker_username
  password = var.broker_password
}

# The Message VPN — identical name and spool sizing across the shard.
resource "solacebroker_msg_vpn" "vpn" {
  msg_vpn_name    = var.msg_vpn_name
  enabled         = true
  max_msg_spool_usage = var.max_spool_mb

  # Enable the messaging services the shard's protocols use. Ports come from broker config; the
  # assignment service reads them and never hardcodes them.
  service_smf_plain_text_enabled          = true
  service_amqp_plain_text_enabled         = true
  service_mqtt_plain_text_enabled         = true
  service_rest_incoming_plain_text_enabled = true
}

# Durable queues + their topic subscriptions — a queue lives on one broker, but its DEFINITION must
# be identical wherever the shard's brokers are provisioned so placement is deterministic.
resource "solacebroker_msg_vpn_queue" "queues" {
  for_each        = { for q in var.queues : q.name => q }
  msg_vpn_name    = solacebroker_msg_vpn.vpn.msg_vpn_name
  queue_name      = each.value.name
  access_type     = each.value.access_type
  ingress_enabled = true
  egress_enabled  = true
  permission      = "consume"
}

resource "solacebroker_msg_vpn_queue_subscription" "queue_subs" {
  for_each = merge([
    for q in var.queues : {
      for s in q.subscriptions : "${q.name}|${s}" => { queue = q.name, topic = s }
    }
  ]...)
  msg_vpn_name       = solacebroker_msg_vpn.vpn.msg_vpn_name
  queue_name         = each.value.queue
  subscription_topic = each.value.topic
  depends_on         = [solacebroker_msg_vpn_queue.queues]
}

resource "solacebroker_msg_vpn_client_profile" "profiles" {
  for_each                            = { for p in var.client_profiles : p.name => p }
  msg_vpn_name                        = solacebroker_msg_vpn.vpn.msg_vpn_name
  client_profile_name                 = each.value.name
  allow_guaranteed_msg_send_enabled   = each.value.allow_guaranteed_send
  allow_guaranteed_msg_receive_enabled = each.value.allow_guaranteed_receive
}

resource "solacebroker_msg_vpn_acl_profile" "acls" {
  for_each                      = { for a in var.acl_profiles : a.name => a }
  msg_vpn_name                  = solacebroker_msg_vpn.vpn.msg_vpn_name
  acl_profile_name              = each.value.name
  client_connect_default_action = each.value.client_connect_default
  publish_topic_default_action  = each.value.publish_topic_default
  subscribe_topic_default_action = each.value.subscribe_topic_default
}

# DMR links only exist for mesh/hybrid topologies; sharded topology provisions none, which is the
# whole point of ADR 0003 (no inter-broker payload crossing).
resource "solacebroker_dmr_cluster_link" "links" {
  for_each         = { for l in var.dmr_cluster_links : l.remote_node_name => l }
  dmr_cluster_name = "shard-cluster"
  remote_node_name = each.value.remote_node_name
  # remote_address wiring depends on your DMR cluster setup; see the provider docs.
}
