package registry
//
// import (
// 	"path"
// 	"time"
//
// 	"github.com/samuel/go-zookeeper/zk"
// )
//
// type Client struct {
// 	zkServers []string // 多个节点地址
// 	zkRoot    string   // 服务根节点
// 	conn      *zk.Conn // zk的客户端连接
// }
//
// // NewClient creates consul client
// func NewClient(zkServers []string, zkRoot string, timeout time.Duration) (*Client, error) {
// 	client := new(Client)
// 	client.zkServers = zkServers
// 	client.zkRoot = zkRoot
//
// 	conn, _, err := zk.Connect(zkServers, timeout) // 连接服务器
// 	if err != nil {
// 		return nil, err
// 	}
// 	client.conn = conn
// 	if err := client.ensureRoot(); err != nil { // 创建服务根节点
// 		client.conn.Close()
// 		return nil, err
// 	}
// 	return client, nil
// }
//
//
