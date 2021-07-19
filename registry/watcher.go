package registry

import (
	"context"
	"encoding/json"
	"path"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-zookeeper/zk"
)

var (
	_ registry.Watcher = &watcher{}
)

type watcher struct {
	serviceNamePath string
	ctx             context.Context
	cancel          context.CancelFunc
	conn            *zk.Conn
}

func newWatcher(ctx context.Context, conn *zk.Conn, serviceNamePath string) (w *watcher, err error) {
	w = &watcher{
		serviceNamePath: serviceNamePath,
		conn:            conn,
	}
	w.ctx, w.cancel = context.WithCancel(ctx)
	ss, _, err := conn.Children(serviceNamePath)
	if err != nil {
		return nil, err
	}
	// zk中如果有已注册的服务，需要传递nextEvent，防止获取ServiceInstance阻塞
	if len(ss) > 0 {
		nextEvent <- true
	}
	return w, err
}

func (w watcher) Next() ([]*registry.ServiceInstance, error) {
	for {
		select {
		case <-w.ctx.Done():
			return nil, w.ctx.Err()
		case <-nextEvent:
		}
		servicesID, _, err := w.conn.Children(w.serviceNamePath)
		if err != nil {
			return nil, err
		}
		var items []*registry.ServiceInstance
		for _, service := range servicesID {
			var item = &registry.ServiceInstance{}
			servicePath := path.Join(w.serviceNamePath, string(service))
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
