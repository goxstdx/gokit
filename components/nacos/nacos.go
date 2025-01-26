package nacos

import (
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type Conf struct {
	Ipaddr              string
	Port                uint64
	NamespaceId         string
	DataId              string
	Group               string
	UserName            string
	Password            string
	TimeoutMs           uint64
	NotLoadCacheAtStart bool
	LogDir              string
	CacheDir            string
	LogLevel            string
}

type Nacos struct {
	Conf Conf

	_client config_client.IConfigClient
}

func NewNacos(conf Conf) *Nacos {
	return &Nacos{
		Conf: conf,
	}
}
func (n *Nacos) CreateClient() (config_client.IConfigClient, error) {
	sc := []constant.ServerConfig{{
		Scheme:      "",
		ContextPath: "",
		IpAddr:      n.Conf.Ipaddr,
		Port:        n.Conf.Port,
		GrpcPort:    0,
	}}
	cc := constant.ClientConfig{
		NamespaceId:         n.Conf.NamespaceId,
		TimeoutMs:           n.Conf.TimeoutMs,
		NotLoadCacheAtStart: n.Conf.NotLoadCacheAtStart,
		LogDir:              n.Conf.LogDir,
		CacheDir:            n.Conf.CacheDir,
		LogLevel:            n.Conf.LogLevel,
		Username:            n.Conf.UserName,
		Password:            n.Conf.Password,
	}

	return clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
}

func (n *Nacos) GetConfig() string {
	param := vo.ConfigParam{
		DataId: n.Conf.DataId,
		Group:  n.Conf.Group,
	}

	configContext, err := n._client.GetConfig(param)
	if err != nil {
		panic(err)
	}

	return configContext
}

func (n *Nacos) ListenConfig(fn func(dataId, data string)) {
	err := n._client.ListenConfig(vo.ConfigParam{
		DataId: n.Conf.DataId,
		Group:  n.Conf.Group,
		OnChange: func(namespace, group, dataId, data string) {
			fmt.Println("listen config start")
			fn(dataId, data)
			fmt.Println("listen config end")
		},
	})

	if err != nil {
		fmt.Println("nacos listenConfig error", err.Error())
	}
}
