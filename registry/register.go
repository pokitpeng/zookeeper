package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-zookeeper/zk"
)

var (
	_ registry.Registrar = &Registry{}
	_ registry.Discovery = &Registry{}
)

var gloableEvent = make(chan bool, 1)

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
	opts       *options
	zkServers  []string
	watchEvent <-chan zk.Event
	conn       *zk.Conn
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
	conn, event, err := zk.Connect(zkServers, options.timeout)
	if err != nil {
		return nil, err
	}
	return &Registry{
		opts:       options,
		zkServers:  zkServers,
		conn:       conn,
		watchEvent: event,
	}, err
}

func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	var data []byte
	var err error
	serviceNamePath := path.Join(r.opts.rootPath, service.Name)
	servicePath := path.Join(serviceNamePath, service.ID)
	go func() {
		for {
			select {
			case e := <-r.watchEvent:
				if e.Type == zk.EventNodeChildrenChanged && e.Path == servicePath {
					gloableEvent <- true
				}
			}
		}
	}()
	if err := r.ensureName(r.opts.rootPath, []byte("")); err != nil {
		return err
	}

	if err = r.ensureName(serviceNamePath, []byte("")); err != nil {
		return err
	}
	if data, err = json.Marshal(service); err != nil {
		return err
	}

	if err = r.ensureName(servicePath, data); err != nil {
		return err
	}
	return nil
}

// Deregister the registration.
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	ch := make(chan error, 1)
	servicePath := path.Join(r.opts.rootPath, service.Name, service.ID)
	go func() {
		err := r.conn.Delete(servicePath, -1)
		if err != nil {
			fmt.Printf("delete node [%s] err: %s\n", servicePath, err.Error())
		}
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
	serviceNamePath := path.Join(r.opts.rootPath, serviceName)
	return newWatcher(ctx, r.conn, serviceNamePath)
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
