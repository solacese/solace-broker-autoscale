variable "broker_url" {
  description = "SEMPv2 base URL of the broker to configure, e.g. https://host:943"
  type        = string
}

variable "broker_username" {
  description = "SEMP management username. Supply at apply time from a secret manager; never commit."
  type        = string
  sensitive   = true
}

variable "broker_password" {
  description = "SEMP management password. Supply at apply time from a secret manager; never commit."
  type        = string
  sensitive   = true
}

variable "msg_vpn_name" {
  description = "Message VPN name, identical across the shard's brokers."
  type        = string
}

variable "max_spool_mb" {
  description = "Message spool size in MB for the VPN."
  type        = number
  default     = 1500
}

variable "queues" {
  description = "Durable queues to provision identically on every broker in the shard."
  type = list(object({
    name          = string
    access_type   = string       # exclusive | non-exclusive
    subscriptions = list(string) # topic subscriptions attracted to this queue
  }))
  default = []
}

variable "client_profiles" {
  description = "Client profiles (flow control, guaranteed messaging enablement) shared across the shard."
  type = list(object({
    name                    = string
    allow_guaranteed_send   = bool
    allow_guaranteed_receive = bool
  }))
  default = []
}

variable "acl_profiles" {
  description = "ACL profiles applied identically across the shard."
  type = list(object({
    name                      = string
    client_connect_default    = string # allow | disallow
    publish_topic_default     = string
    subscribe_topic_default   = string
  }))
  default = []
}

variable "dmr_cluster_links" {
  description = "DMR links for mesh/hybrid topologies. Empty for pure sharded (no inter-broker links)."
  type = list(object({
    remote_node_name = string
    remote_address   = string
  }))
  default = []
}
