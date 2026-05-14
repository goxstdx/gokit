package nacos

type Conf struct {
	Ipaddr              string
	Port                uint64
	NamespaceId         string
	DataId              string
	Group               string
	UserName            string
	Password            string
	TimeoutMs           uint64
	PollIntervalMs      uint64
	NotLoadCacheAtStart bool
	LogDir              string
	CacheDir            string
	LogLevel            string
}
