module github.com/ashtonian/mqttv5/queue/file

go 1.26.4

require (
	github.com/ashtonian/mqttv5 v0.0.0
	github.com/ashtonian/mqttv5/internal/filedb v0.0.0
	github.com/ashtonian/mqttv5/store/file v0.0.0
	go.etcd.io/bbolt v1.5.0
)

require golang.org/x/sys v0.48.0 // indirect

replace (
	github.com/ashtonian/mqttv5 => ../..
	github.com/ashtonian/mqttv5/internal/filedb => ../../internal/filedb
	github.com/ashtonian/mqttv5/store/file => ../../store/file
)
