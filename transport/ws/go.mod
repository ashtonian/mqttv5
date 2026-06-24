module github.com/ashtonian/mqttv5/transport/ws

go 1.26.4

require (
	github.com/ashtonian/mqttv5 v0.0.0
	github.com/gobwas/ws v1.4.0
)

require (
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	golang.org/x/sys v0.6.0 // indirect
)

replace github.com/ashtonian/mqttv5 => ../..
