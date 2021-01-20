module lukechampine.com/us

go 1.15

require (
	github.com/aead/chacha20 v0.0.0-20180709150244-8b13a72661da
	github.com/hashicorp/go-multierror v1.1.0
	github.com/pkg/errors v0.9.1
	gitlab.com/NebulousLabs/Sia v1.5.0
	gitlab.com/NebulousLabs/encoding v0.0.0-20200604091946-456c3dc907fe
	gitlab.com/NebulousLabs/log v0.0.0-20200604091839-0ba4a941cdc2
	gitlab.com/NebulousLabs/siamux v0.0.0-20200723083235-f2c35a421446 // for testing mux compatibility
	go.etcd.io/bbolt v1.3.5
	golang.org/x/crypto v0.0.0-20201012173705-84dcc777aaee
	golang.org/x/sys v0.0.0-20201015000850-e3ed0017c211
	lukechampine.com/frand v1.3.0
)
