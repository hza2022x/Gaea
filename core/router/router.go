package router

import (
	"fmt"
	"strings"

	"github.com/XiaoMi/Gaea/models"
)

type Router struct {
	rules       map[string]map[string]Rule // dbname-tablename
	defaultRule Rule
}

// NewRouter build router according to the models of namespace
func NewRouter(namespace *models.Namespace) (*Router, error) {
	// copy names of slice list
	var sliceNames []string
	for _, slice := range namespace.Slices {
		sliceNames = append(sliceNames, slice.Name)
	}

	// check default slice
	if !IncludeSlice(sliceNames, namespace.DefaultSlice) {
		return nil, fmt.Errorf("default slice[%s] not in the slice list",
			namespace.DefaultSlice)
	}

	// create router of special namespace
	rt := new(Router)
	rt.rules = make(map[string]map[string]Rule)
	rt.defaultRule = NewDefaultRule(namespace.DefaultSlice)

	linkedRuleIndexes := make([]int, 0)

	for i, shard := range namespace.ShardRules {
		for _, slice := range shard.Slices {
			if !IncludeSlice(sliceNames, slice) {
				return nil, fmt.Errorf("shard table[%s] slice[%s] not in the namespace.slices list:[%s]",
					shard.Table, slice, strings.Join(shard.Slices, ","))
			}
		}

		// get index of linked table config and handle it later
		if shard.Type == LinkedTableRuleType {
			linkedRuleIndexes = append(linkedRuleIndexes, i)
			continue
		}

		rule, err := ParseRule(shard)
		if err != nil {
			return nil, err
		}

		// if global table rule, use the namespace slice names
		// TODO: refactor
		if rule.ruleType == GlobalTableRuleType {
			rule.slices = sliceNames
		}

		if rule.ruleType == DefaultRuleType {
			return nil, fmt.Errorf("[default-rule] duplicate, must only one")
		}
		//if the database exist in rules
		if _, ok := rt.rules[rule.db]; ok {
			if _, ok := rt.rules[rule.db][rule.table]; ok {
				return nil, fmt.Errorf("table %s rule in %s duplicate", rule.table, rule.db)
			} else {
				rt.rules[rule.db][rule.table] = rule
			}
		} else {
			m := make(map[string]Rule)
			rt.rules[rule.db] = m
			rt.rules[rule.db][rule.table] = rule
		}
	}

	// create linked rule
	for _, i := range linkedRuleIndexes {
		rule, err := createLinkedRule(rt.rules, namespace.ShardRules[i])
		if err != nil {
			return nil, fmt.Errorf("create linked rule error: %v", err)
		}
		rt.rules[rule.db][rule.table] = rule
	}

	return rt, nil
}

// GetShardRule 获取规则 可否将规则放到全局Context中？TODO
func (r *Router) GetShardRule(db, table string) (Rule, bool) {
	dbTables := strings.Split(table, ".")
	if len(dbTables) == 2 {
		table = strings.Trim(dbTables[1], "`")
		db = strings.Trim(dbTables[0], "`")
	}
	rule, ok := r.rules[db][table]
	return rule, ok
}

func (r *Router) GetRule(db, table string) Rule {
	arry := strings.Split(table, ".")
	if len(arry) == 2 {
		table = strings.Trim(arry[1], "`")
		db = strings.Trim(arry[0], "`")
	}
	rule := r.rules[db][table]
	if rule == nil {
		//set the database of default rule
		r.defaultRule.(*BaseRule).db = db
		return r.defaultRule
	} else {
		return rule
	}
}

func (r *Router) GetDefaultRule() Rule {
	return r.defaultRule
}
