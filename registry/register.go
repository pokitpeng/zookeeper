package registry

import (
	"context"
	"encoding/json"
	"path"
	"time"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	_ registry.Registrar = &Registry{}
	_ registry.Discovery = &Registry{}
)

// Option is etcd registry option.
type Option func(o *options)

type options struct {
	ctx      context.Context
	rootPath string
	timeout  time.Duration
}

// WithContext with registry context.
func WithContext(ctx context.Context) Option {
	return func(o *options) { o.ctx = ctx }
}

// WithRootPath with registry root path.
func WithRootPath(path string) Option {
	return func(o *options) { o.rootPath = path }
}

// WithTimeout with registry timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(o *options) { o.timeout = timeout }
}

// Registry is consul registry
type Registry struct {
	opts      *options
	zkServers []string
	conn      *zk.Conn
}

func New(zkServers []string, opts ...Option) (*Registry, error) {
	options := &options{
		ctx:      context.Background(),
		rootPath: "/microservices",
		timeout:  time.Second * 5,
	}
	for _, o := range opts {
		o(options)
	}
	conn, _, err := zk.Connect(zkServers, options.timeout)
	if err != nil {
		return nil, err
	}
	return &Registry{
		opts:      options,
		zkServers: zkServers,
		conn:      conn,
	}, err
}

func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	var data []byte
	var err error
	if err := r.ensureName(r.opts.rootPath, []byte("")); err != nil {
		return err
	}
	serviceNamePath := path.Join(r.opts.rootPath, service.Name)
	if err = r.ensureName(serviceNamePath, []byte("")); err != nil {
		return err
	}
	if data, err = json.Marshal(service); err != nil {
		return err
	}
	servicePath := path.Join(serviceNamePath, service.ID)
	if err = r.ensureName(servicePath, data); err != nil {
		return err
	}
	return nil
}

// Deregister the registration.
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	ch := make(chan error)
	servicePath := path.Join(r.opts.rootPath, service.Name, service.ID)
	go func() {
		err := r.conn.Delete(servicePath, -1)
		ch <- err
	}()
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-ch:
	}
	return err
}

func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	serviceNamePath := path.Join(r.opts.rootPath, serviceName)
	servicesID, _, err := r.conn.Children(serviceNamePath)
	if err != nil {
		return nil, err
	}
	var items []*registry.ServiceInstance
	for _, service := range servicesID {
		var item = &registry.ServiceInstance{}
		servicePath := path.Join(serviceNamePath, service)
		serviceInstanceByte, _, err := r.conn.Get(servicePath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(serviceInstanceByte, item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	return newWatcher(ctx, r.conn, serviceName, r.opts.rootPath)
}

// ensureName ensure node exists, if not exist, create and set data
func (r *Registry) ensureName(path string, data []byte) error {
	exists, _, err := r.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		_, err := r.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}
