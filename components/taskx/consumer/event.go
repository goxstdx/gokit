package consumer

import "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"

// groupEventEntries 按 QueueGroup 对 event runner 分组
func (c *Consumer) groupEventEntries(entries map[string]*EventEntry) map[string][]*EventEntry {
	groups := make(map[string][]*EventEntry)
	for _, entry := range entries {
		group := entry.Option.QueueGroup
		if group == "" {
			group = core.DefaultEventQueueGroup
		}
		groups[group] = append(groups[group], entry)
	}
	return groups
}

// resolveEventGroupName 根据 runner name 解析所属的事件队列组名
func (c *Consumer) resolveEventGroupName(runnerName string) string {
	name, _ := c.resolveEventGroupNameStrict(runnerName)
	return name
}

// resolveEventGroupNameStrict 根据 runner name 解析所属的事件队列组名，并返回是否已注册。
func (c *Consumer) resolveEventGroupNameStrict(runnerName string) (groupName string, registered bool) {
	entries := c.registry.GetEventRunners()
	if entry, ok := entries[runnerName]; ok {
		if entry.Option.QueueGroup != "" {
			return entry.Option.QueueGroup, true
		}
		return core.DefaultEventQueueGroup, true
	}
	return runnerName, false
}

// EventGroupResolver 返回一个闭包函数，供 Producer 使用来解析事件组名。
func (c *Consumer) EventGroupResolver() func(string) (string, bool) {
	return c.resolveEventGroupNameStrict
}
