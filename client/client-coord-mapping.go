package client

// keep it simple
var staticCoordServers = []string{
	"http://node0:17000",
	"http://node1:17000",
	"http://node2:17000",
}

// TODO: If time permits (WIP): replace with hostname after RPC is fixed to return hostname
var staticIPLeaderMapping = map[string]string{
	"10.10.10.2:18000": "http://node0:17000",
	"10.10.10.3:18000": "http://node1:17000",
	"10.10.10.4:18000": "http://node2:17000",
}
