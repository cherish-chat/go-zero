package zrpc

import (
	"google.golang.org/grpc/resolver"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc/internal"
	"github.com/zeromicro/go-zero/zrpc/internal/auth"
	"github.com/zeromicro/go-zero/zrpc/internal/clientinterceptors"
	zrpcresolver "github.com/zeromicro/go-zero/zrpc/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	// WithDialOption is an alias of internal.WithDialOption.
	WithDialOption = internal.WithDialOption
	// WithNonBlock sets the dialing to be nonblock.
	WithNonBlock = internal.WithNonBlock
	// WithStreamClientInterceptor is an alias of internal.WithStreamClientInterceptor.
	WithStreamClientInterceptor = internal.WithStreamClientInterceptor
	// WithTimeout is an alias of internal.WithTimeout.
	WithTimeout = internal.WithTimeout
	// WithTransportCredentials return a func to make the gRPC calls secured with given credentials.
	WithTransportCredentials = internal.WithTransportCredentials
	// WithUnaryClientInterceptor is an alias of internal.WithUnaryClientInterceptor.
	WithUnaryClientInterceptor = internal.WithUnaryClientInterceptor
)

type (
	// Client is an alias of internal.Client.
	Client = internal.Client
	// ClientOption is an alias of internal.ClientOption.
	ClientOption = internal.ClientOption

	// A RpcClient is a rpc client.
	RpcClient struct {
		client Client
	}
)

// MustNewClient returns a Client, exits on any error.
func MustNewClient(c RpcClientConf, options ...ClientOption) Client {
	cli, err := NewClient(c, options...)
	logx.Must(err)
	return cli
}

// NewClient returns a Client.
func NewClient(c RpcClientConf, options ...ClientOption) (Client, error) {
	var opts []ClientOption
	if c.HasCredential() {
		opts = append(opts, WithDialOption(grpc.WithPerRPCCredentials(&auth.Credential{
			App:   c.App,
			Token: c.Token,
		})))
	}
	if c.NonBlock {
		opts = append(opts, WithNonBlock())
	}
	if c.Timeout > 0 {
		opts = append(opts, WithTimeout(time.Duration(c.Timeout)*time.Millisecond))
	}
	if c.KeepaliveTime > 0 {
		opts = append(opts, WithDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: c.KeepaliveTime,
		})))
	}

	opts = append(opts, options...)

	target, err := c.BuildTarget()
	if err != nil {
		return nil, err
	}

	client, err := internal.NewClient(target, c.Middlewares, opts...)
	if err != nil {
		return nil, err
	}

	return &RpcClient{
		client: client,
	}, nil
}

// NewClientWithTarget returns a Client with connecting to given target.
func NewClientWithTarget(target string, opts ...ClientOption) (Client, error) {
	var config RpcClientConf
	if err := conf.FillDefault(&config); err != nil {
		return nil, err
	}

	config.Target = target

	return NewClient(config, opts...)
}

// Conn returns the underlying grpc.ClientConn.
func (rc *RpcClient) Conn() *grpc.ClientConn {
	return rc.client.Conn()
}

// DontLogClientContentForMethod disable logging content for given method.
func DontLogClientContentForMethod(method string) {
	clientinterceptors.DontLogContentForMethod(method)
}

// SetClientSlowThreshold sets the slow threshold on client side.
func SetClientSlowThreshold(threshold time.Duration) {
	clientinterceptors.SetSlowThreshold(threshold)
}

type defaultUpdateHandler struct {
	targetAddressesMap map[string][]resolver.Address
	targetClientMap    map[string]map[string]Client
	lock               sync.RWMutex
}

var defaultHandler = &defaultUpdateHandler{
	targetAddressesMap: make(map[string][]resolver.Address),
	targetClientMap:    make(map[string]map[string]Client),
}

func (h *defaultUpdateHandler) OnUpdate(target resolver.Target, addresses []resolver.Address) {
	if target.URL.Scheme == "etcd" {
		serviceName := strings.TrimPrefix(target.URL.Path, "/")
		h.lock.Lock()
		h.targetAddressesMap[serviceName] = addresses
		clientMap, ok := h.targetClientMap[serviceName]
		if !ok {
			clientMap = make(map[string]Client)
		}
		for _, address := range addresses {
			if _, ok := clientMap[address.Addr]; !ok {
				clientConf := RpcClientConf{
					Endpoints: []string{address.Addr},
					NonBlock:  true,
					Timeout:   60000,
				}
				client, err := NewClient(clientConf)
				if err != nil {
					logx.Errorf("create client for %s:%s failed: %v", serviceName, address.Addr, err)
				} else {
					clientMap[address.Addr] = client
				}
			}
		}
		h.targetClientMap[serviceName] = clientMap
		h.lock.Unlock()
	}
}

func GetServiceAddresses(serviceName string) ([]resolver.Address, bool) {
	defaultHandler.lock.RLock()
	defer defaultHandler.lock.RUnlock()
	if addresses, ok := defaultHandler.targetAddressesMap[serviceName]; ok {
		return addresses, true
	}
	return nil, false
}

func GetServiceClientMap(serviceName string) (map[string]Client, bool) {
	defaultHandler.lock.RLock()
	defer defaultHandler.lock.RUnlock()
	if clientMap, ok := defaultHandler.targetClientMap[serviceName]; ok {
		return clientMap, true
	}
	return nil, false
}

func GetServiceClients(serviceName string) ([]Client, bool) {
	clientMap, ok := GetServiceClientMap(serviceName)
	if !ok {
		return nil, false
	}
	var clients []Client
	for _, client := range clientMap {
		clients = append(clients, client)
	}
	return clients, true
}

func init() {
	zrpcresolver.RegisterUpdateHandler(defaultHandler)
}
