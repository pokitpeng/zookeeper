package registry

import (
	"context"
	"encoding/json"
	"path"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	_ registry.Watcher = &watcher{}
)

type watcher struct {
	serviceName string
	ctx         context.Context
	cancel      context.CancelFunc
	rootPath    string
	watchEvent  <-chan zk.Event
	conn        *zk.Conn
}

func newWatcher(ctx context.Context, serviceName, rootPath string) (*watcher, error) {
	w := &watcher{
		serviceName: serviceName,
		rootPath:    rootPath,
	}
	w.ctx, w.cancel = context.WithCancel(ctx)
	_, _, events, err := w.conn.ChildrenW(path.Join(rootPath, serviceName))
	w.watchEvent = events
	return w, err
}

func (w watcher) Next() ([]*registry.ServiceInstance, error) {
	for {
		select {
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		case <-w.watchEvent:
		}
		serviceNamePath := path.Join(w.rootPath, w.serviceName)
		servicesID, _, err := w.conn.Children(serviceNamePath)
		if err != nil {
			return nil, err
		}
		var items []*registry.ServiceInstance
		for _, service := range servicesID {
			var item *registry.ServiceInstance
			servicePath := path.Join(serviceNamePath, service)
			serviceInstanceByte, _, err := w.conn.Get(servicePath)
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
}

func (w watcher) Stop() error {
	w.cancel()
	// close
	return nil
}
