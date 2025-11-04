package etcd

import (
	"context"
	"github.com/openimsdk/tools/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"testing"
	"time"
)

const testServerName = "test-user"

func getEtcd() discovery.SvcDiscoveryRegistry {
	var endpoints []string
	endpoints = []string{"localhost:12379"}
	endpoints = []string{
		"http://127.0.0.1:2379",  // etcd1
		"http://127.0.0.1:22379", // etcd2
		"http://127.0.0.1:32379", // etcd3
	}

	r, err := NewSvcDiscoveryRegistry("openim", endpoints, nil)
	if err != nil {
		panic(err)
	}
	r.AddOption(grpc.WithTransportCredentials(insecure.NewCredentials()))
	return r
}

func TestGetConn(t *testing.T) {
	r := getEtcd()
	for i := 1; ; i++ {
		cs, err := r.GetConn(context.Background(), testServerName)
		if err == nil {
			t.Log("get conns success:", i, cs)
		} else {
			t.Log("get conns failed:", i, err)
		}
		time.Sleep(time.Second)
	}
}

func TestRegister(t *testing.T) {
	r := getEtcd()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	t.Log("listening on port", port)
	if err := r.Register(context.Background(), testServerName, "192.168.10.105", port); err != nil {
		panic(err)
	}
	for i := 0; i < 1000; i++ {
		time.Sleep(time.Second)
		t.Log("registering...", i)
	}
	r.Close()
	t.Log("closed")
	time.Sleep(time.Second)
}

func TestWatch(t *testing.T) {
	r := getEtcd()
	t.Log("start watch")
	err := r.WatchKey(context.Background(), "openim/test-user", func(data *discovery.WatchKey) error {
		t.Log("watch data:", string(data.Value))
		return nil
	})
	t.Log(err)
}
