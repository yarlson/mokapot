package sns

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// MessageAttribute represents a single SNS message attribute with type and value.
type MessageAttribute struct {
	DataType    string // "String", "Number", "Binary"
	StringValue string
}

// filterPolicy is a parsed SNS filter policy.
// Keys are attribute names, values are arrays of conditions (OR'd).
// All keys must match for the policy to pass (AND'd across keys).
type filterPolicy map[string][]condition

type conditionKind int

const (
	conditionExactString conditionKind = iota
	conditionExactNumeric
	conditionExists
	conditionAnythingBut
	conditionNumeric
	conditionPrefix
)

// condition represents one filter condition in a policy.
type condition struct {
	kind conditionKind

	strVal string
	numVal float64

	existsVal bool

	anythingButStrings []string
	anythingButNumbers []float64

	numericOps []numericOp

	prefix string
}

type numericOp struct {
	op    string // "=", ">", ">=", "<", "<="
	value float64
}

// parseFilterPolicy parses a FilterPolicy JSON string and validates it.
func parseFilterPolicy(policyJSON string) (filterPolicy, error) {
	if policyJSON == "" {
		return filterPolicy{}, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policyJSON), &raw); err != nil {
		return nil, fmt.Errorf("%w: Invalid filter policy: %s", ErrInvalidParameter, err.Error())
	}

	if len(raw) == 0 {
		return filterPolicy{}, nil
	}

	policy := make(filterPolicy, len(raw))

	for key, rawConditions := range raw {
		var conditionsArray []json.RawMessage
		if err := json.Unmarshal(rawConditions, &conditionsArray); err != nil {
			return nil, fmt.Errorf("%w: Filter policy condition for '%s' must be an array", ErrInvalidParameter, key)
		}

		conditions := make([]condition, 0, len(conditionsArray))
		for _, rawCond := range conditionsArray {
			cond, err := parseCondition(rawCond)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		}
		policy[key] = conditions
	}

	return policy, nil
}

func parseCondition(raw json.RawMessage) (condition, error) {
	// Try string literal
	var strVal string
	if err := json.Unmarshal(raw, &strVal); err == nil {
		return condition{kind: conditionExactString, strVal: strVal}, nil
	}

	// Try number literal
	var numVal float64
	if err := json.Unmarshal(raw, &numVal); err == nil {
		return condition{kind: conditionExactNumeric, numVal: numVal}, nil
	}

	// Try object with operator
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return condition{}, fmt.Errorf("%w: Invalid filter policy condition", ErrInvalidParameter)
	}

	if val, ok := obj["exists"]; ok {
		var exists bool
		if err := json.Unmarshal(val, &exists); err != nil {
			return condition{}, fmt.Errorf("%w: 'exists' value must be a boolean", ErrInvalidParameter)
		}
		return condition{kind: conditionExists, existsVal: exists}, nil
	}

	if val, ok := obj["anything-but"]; ok {
		return parseAnythingBut(val)
	}

	if val, ok := obj["numeric"]; ok {
		return parseNumericCondition(val)
	}

	if val, ok := obj["prefix"]; ok {
		var prefix string
		if err := json.Unmarshal(val, &prefix); err != nil {
			return condition{}, fmt.Errorf("%w: 'prefix' value must be a string", ErrInvalidParameter)
		}
		return condition{kind: conditionPrefix, prefix: prefix}, nil
	}

	return condition{}, fmt.Errorf("%w: Unsupported filter policy operator", ErrInvalidParameter)
}

func parseAnythingBut(raw json.RawMessage) (condition, error) {
	// Try single string
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return condition{kind: conditionAnythingBut, anythingButStrings: []string{single}}, nil
	}

	// Try single number
	var singleNum float64
	if err := json.Unmarshal(raw, &singleNum); err == nil {
		return condition{kind: conditionAnythingBut, anythingButNumbers: []float64{singleNum}}, nil
	}

	// Try array
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return condition{}, fmt.Errorf("%w: 'anything-but' value must be a string, number, or array", ErrInvalidParameter)
	}

	var strs []string
	var nums []float64
	for _, item := range arr {
		var s string
		if err := json.Unmarshal(item, &s); err == nil {
			strs = append(strs, s)
			continue
		}
		var n float64
		if err := json.Unmarshal(item, &n); err == nil {
			nums = append(nums, n)
			continue
		}
		return condition{}, fmt.Errorf("%w: 'anything-but' array items must be strings or numbers", ErrInvalidParameter)
	}

	return condition{kind: conditionAnythingBut, anythingButStrings: strs, anythingButNumbers: nums}, nil
}

func parseNumericCondition(raw json.RawMessage) (condition, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return condition{}, fmt.Errorf("%w: 'numeric' value must be an array", ErrInvalidParameter)
	}

	if len(arr) == 0 || len(arr)%2 != 0 {
		return condition{}, fmt.Errorf("%w: 'numeric' array must have 2 or 4 elements", ErrInvalidParameter)
	}

	var ops []numericOp
	for i := 0; i < len(arr); i += 2 {
		var op string
		if err := json.Unmarshal(arr[i], &op); err != nil {
			return condition{}, fmt.Errorf("%w: numeric operator must be a string", ErrInvalidParameter)
		}
		switch op {
		case "=", ">", ">=", "<", "<=":
		default:
			return condition{}, fmt.Errorf("%w: unsupported numeric operator: %s", ErrInvalidParameter, op)
		}

		var val float64
		if err := json.Unmarshal(arr[i+1], &val); err != nil {
			return condition{}, fmt.Errorf("%w: numeric value must be a number", ErrInvalidParameter)
		}

		ops = append(ops, numericOp{op: op, value: val})
	}

	return condition{kind: conditionNumeric, numericOps: ops}, nil
}

// matchesFilterPolicy checks if message attributes match a filter policy.
// If policy is nil/empty, all messages pass.
func matchesFilterPolicy(policy filterPolicy, attrs map[string]MessageAttribute) bool {
	if len(policy) == 0 {
		return true
	}

	for key, conditions := range policy {
		attr, exists := attrs[key]
		if !matchesConditions(conditions, attr, exists) {
			return false
		}
	}

	return true
}

// matchesConditions checks if an attribute matches any of the conditions (OR).
func matchesConditions(conditions []condition, attr MessageAttribute, attrExists bool) bool {
	for _, cond := range conditions {
		if matchesCondition(cond, attr, attrExists) {
			return true
		}
	}
	return false
}

func matchesCondition(cond condition, attr MessageAttribute, attrExists bool) bool {
	switch cond.kind {
	case conditionExists:
		return attrExists == cond.existsVal

	case conditionExactString:
		return attrExists && attr.StringValue == cond.strVal

	case conditionExactNumeric:
		if !attrExists || !strings.HasPrefix(attr.DataType, "Number") {
			return false
		}
		num, err := strconv.ParseFloat(attr.StringValue, 64)
		if err != nil {
			return false
		}
		return num == cond.numVal

	case conditionAnythingBut:
		if !attrExists {
			return false
		}
		for _, s := range cond.anythingButStrings {
			if attr.StringValue == s {
				return false
			}
		}
		if strings.HasPrefix(attr.DataType, "Number") {
			if num, err := strconv.ParseFloat(attr.StringValue, 64); err == nil {
				for _, n := range cond.anythingButNumbers {
					if num == n {
						return false
					}
				}
			}
		}
		return true

	case conditionNumeric:
		if !attrExists || !strings.HasPrefix(attr.DataType, "Number") {
			return false
		}
		num, err := strconv.ParseFloat(attr.StringValue, 64)
		if err != nil {
			return false
		}
		for _, op := range cond.numericOps {
			if !evaluateNumericOp(op, num) {
				return false
			}
		}
		return true

	case conditionPrefix:
		return attrExists && strings.HasPrefix(attr.StringValue, cond.prefix)
	}
	return false
}

func evaluateNumericOp(op numericOp, val float64) bool {
	switch op.op {
	case "=":
		return val == op.value
	case ">":
		return val > op.value
	case ">=":
		return val >= op.value
	case "<":
		return val < op.value
	case "<=":
		return val <= op.value
	}
	return false
}
