# Fictional example shard configuration (§12: no real customer data).
msg_vpn_name = "acme-prod"
max_spool_mb = 1500

queues = [
  {
    name          = "q.orders.billing"
    access_type   = "exclusive"
    subscriptions = ["acme/orders/invoice", "acme/orders/created"]
  },
]

client_profiles = [
  {
    name                     = "cp.guaranteed"
    allow_guaranteed_send    = true
    allow_guaranteed_receive = true
  },
]

acl_profiles = [
  {
    name                    = "acl.default"
    client_connect_default  = "allow"
    publish_topic_default   = "allow"
    subscribe_topic_default = "allow"
  },
]

# Pure sharded topology → no DMR links.
dmr_cluster_links = []
