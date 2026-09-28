package control

func descriptors(ids Membership) []BrokerDescriptor {
	r := make([]BrokerDescriptor, len(ids))
	for i, id := range ids {
		r[i] = BrokerDescriptor{ID: id, Endpoint: "amqps://" + id + ".example:5671"}
	}
	return r
}
