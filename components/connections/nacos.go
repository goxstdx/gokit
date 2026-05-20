package connections

import (
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/nacos"
)

var (
	nacosClient *nacos.NacosHTTP
)

// GenDefaultNacosClient 注意，这里每次执行都会覆盖默认的链接
func GenDefaultNacosClient(conf nacos.Conf) (err error) {
	nacosClient, err = nacos.NewNacosHTTP(conf)
	if err != nil {
		return err
	}

	return nil
}

// GetNacosClient 获取默认的 nacos 链接
func GetNacosClient() *nacos.NacosHTTP {
	return nacosClient
}
