package registry

import (
	"context"

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
	event           chan struct{}
	set             *serviceSet
}

func (w watcher) Next() (services []*registry.ServiceInstance, err error) {
	select {
	case <-w.ctx.Done():
		err = w.ctx.Err()
	case <-w.event:
	}
	ss, ok := w.set.services.Load().([]*registry.ServiceInstance)
	if ok {
		for _, s := range ss {
			services = append(services, s)
		}
	}
	return
}

func (w *watcher) Stop() error {
	w.cancel()
	w.set.lock.Lock()
	defer w.set.lock.Unlock()
	delete(w.set.watcher, w)
	return nil
}
