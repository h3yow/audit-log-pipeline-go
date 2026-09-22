package router

import (
	"fmt"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
)

// Action 是策略命中后的动作。
type Action string

const (
	ActionAccept Action = "accept"
	ActionDeny   Action = "deny"
)

// Rule 是一条路由/拒绝策略。按顺序匹配，先命中先生效。
type Rule struct {
	Name    string
	Sources []string // 为空表示匹配所有来源
	Types   []string // 为空表示匹配所有类型
	Action  Action
	Route   string // accept 时的下游路由名
}

// Router 持有有序规则集，对事件做策略判定。
type Router struct {
	rules        []Rule
	defaultRoute string
}

// New 构造路由器。defaultRoute 为未命中任何规则时的兜底路由。
func New(rules []Rule, defaultRoute string) *Router {
	return &Router{rules: rules, defaultRoute: defaultRoute}
}

// Result 是策略判定结果。
type Result struct {
	Action Action
	Route  string
	Rule   string
	Reason string
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// Decide 对事件按规则顺序判定；无命中走兜底 accept 路由。
// accept 规则缺少 route 属于配置错误，显式返回 deny 而非静默放行。
func (r *Router) Decide(e *event.Event) Result {
	for _, rule := range r.rules {
		if len(rule.Sources) > 0 && !contains(rule.Sources, e.Source) {
			continue
		}
		if len(rule.Types) > 0 && !contains(rule.Types, e.Type) {
			continue
		}
		switch rule.Action {
		case ActionDeny:
			return Result{
				Action: ActionDeny,
				Rule:   rule.Name,
				Reason: fmt.Sprintf("matched deny rule %q", rule.Name),
			}
		case ActionAccept:
			if rule.Route == "" {
				return Result{
					Action: ActionDeny,
					Rule:   rule.Name,
					Reason: fmt.Sprintf("accept rule %q has no route", rule.Name),
				}
			}
			return Result{
				Action: ActionAccept,
				Route:  rule.Route,
				Rule:   rule.Name,
				Reason: fmt.Sprintf("matched accept rule %q", rule.Name),
			}
		}
	}
	return Result{
		Action: ActionAccept,
		Route:  r.defaultRoute,
		Rule:   "default",
		Reason: "no rule matched; using default route",
	}
}
