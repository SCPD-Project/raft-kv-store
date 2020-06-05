package client

// keep it simple
var staticCoordServers = []string {
	"http://127.0.0.0:17000",
	"http://127.0.0.1:17001",
	"http://127.0.0.1:17002",
}

// If time permits (WIP): replace with hostname after RPC is fixed to return hostname
var staticIPLeaderMapping = map[string]string {
	"10.10.10.2:18000": "http://127.0.0.1:17000",
	"10.10.10.3:18000": "http://127.0.0.1:17001",
	"10.10.10.4:18000": "http://127.0.0.1:17002",
}