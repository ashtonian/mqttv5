module github.com/ashtonian/mqttv5/benchmarks

go 1.27.2

require (
	github.com/ashtonian/mqttv5 v0.16.0
	github.com/ashtonian/mqttv5/queue/file v0.16.0
	github.com/eclipse/paho.golang v0.23.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	golang.org/x/perf v0.0.0-20260929162123-406019bb8b68
)

require (
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/ashtonian/mqttv5/internal/filedb v0.16.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace (
	github.com/ashtonian/mqttv5 => ..
	github.com/ashtonian/mqttv5/internal/filedb => ../internal/filedb
	github.com/ashtonian/mqttv5/queue/file => ../queue/file
	github.com/ashtonian/mqttv5/store/file => ../store/file
)
