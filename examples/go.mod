module github.com/ashtonian/mqttv5/examples

go 1.26.4

require (
	github.com/ashtonian/mqttv5 v0.0.0
	github.com/ashtonian/mqttv5/codec/json v0.0.0
	github.com/ashtonian/mqttv5/queue/file v0.0.0
	github.com/ashtonian/mqttv5/store/file v0.0.0
	github.com/ashtonian/mqttv5/transport/ws v0.0.0
)

require (
	github.com/ashtonian/mqttv5/internal/filedb v0.0.0 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.4.0 // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace (
	github.com/ashtonian/mqttv5 => ../
	github.com/ashtonian/mqttv5/codec/json => ../codec/json
	github.com/ashtonian/mqttv5/internal/filedb => ../internal/filedb
	github.com/ashtonian/mqttv5/queue/file => ../queue/file
	github.com/ashtonian/mqttv5/store/file => ../store/file
	github.com/ashtonian/mqttv5/transport/ws => ../transport/ws
)
