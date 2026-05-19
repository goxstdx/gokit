package consumer

// DelayRegisteredChecker 返回一个闭包函数，供 Producer 使用来检查 delay runner 是否已注册。
func (c *Consumer) DelayRegisteredChecker() func(string) bool {
	return func(runnerName string) bool {
		_, ok := c.registry.GetDelayRunners()[runnerName]
		return ok
	}
}
