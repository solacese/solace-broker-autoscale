package runtime

import "github.com/solacese/solace-workload-balancer/config"

func testBrokerInventory(ids []string) []config.DataBroker {
	r := make([]config.DataBroker, len(ids))
	for i, id := range ids {
		r[i] = config.DataBroker{ID: id, AMQPEndpoint: "amqps://" + id + ".example:5671", MessageVPN: "data"}
	}
	return r
}
