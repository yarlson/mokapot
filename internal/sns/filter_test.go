package sns

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustParseFilterPolicy parses a filter policy and fails the test on error.
func mustParseFilterPolicy(t *testing.T, policyJSON string) filterPolicy {
	t.Helper()
	policy, err := parseFilterPolicy(policyJSON)
	require.NoError(t, err)
	return policy
}

// --- parseFilterPolicy tests ---

func TestParseFilterPolicy_Empty(t *testing.T) {
	policy, err := parseFilterPolicy("")
	require.NoError(t, err)
	assert.Empty(t, policy)
}

func TestParseFilterPolicy_EmptyObject(t *testing.T) {
	policy, err := parseFilterPolicy("{}")
	require.NoError(t, err)
	assert.Empty(t, policy)
}

func TestParseFilterPolicy_InvalidJSON(t *testing.T) {
	_, err := parseFilterPolicy("not json")
	assert.ErrorIs(t, err, ErrInvalidParameter)
}

func TestParseFilterPolicy_NonArrayCondition(t *testing.T) {
	_, err := parseFilterPolicy(`{"key": "not-an-array"}`)
	assert.ErrorIs(t, err, ErrInvalidParameter)
}

func TestParseFilterPolicy_ExactStringMatch(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"event_type": ["order_created"]}`)
	require.Len(t, policy["event_type"], 1)
	assert.Equal(t, conditionExactString, policy["event_type"][0].kind)
	assert.Equal(t, "order_created", policy["event_type"][0].strVal)
}

func TestParseFilterPolicy_ExactNumericMatch(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [100]}`)
	require.Len(t, policy["price"], 1)
	assert.Equal(t, conditionExactNumeric, policy["price"][0].kind)
	assert.Equal(t, float64(100), policy["price"][0].numVal)
}

func TestParseFilterPolicy_AllowlistArray(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"event_type": ["created", "updated", "deleted"]}`)
	require.Len(t, policy["event_type"], 3)
	for _, c := range policy["event_type"] {
		assert.Equal(t, conditionExactString, c.kind)
	}
}

func TestParseFilterPolicy_Exists(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"customer_id": [{"exists": true}]}`)
	require.Len(t, policy["customer_id"], 1)
	assert.Equal(t, conditionExists, policy["customer_id"][0].kind)
	assert.True(t, policy["customer_id"][0].existsVal)
}

func TestParseFilterPolicy_ExistsNonBoolean(t *testing.T) {
	_, err := parseFilterPolicy(`{"key": [{"exists": "yes"}]}`)
	assert.ErrorIs(t, err, ErrInvalidParameter)
}

func TestParseFilterPolicy_AnythingButString(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"status": [{"anything-but": "cancelled"}]}`)
	require.Len(t, policy["status"], 1)
	assert.Equal(t, conditionAnythingBut, policy["status"][0].kind)
	assert.Equal(t, []string{"cancelled"}, policy["status"][0].anythingButStrings)
}

func TestParseFilterPolicy_AnythingButArray(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"status": [{"anything-but": ["cancelled", "failed"]}]}`)
	require.Len(t, policy["status"], 1)
	assert.Equal(t, conditionAnythingBut, policy["status"][0].kind)
	assert.Equal(t, []string{"cancelled", "failed"}, policy["status"][0].anythingButStrings)
}

func TestParseFilterPolicy_AnythingButNumber(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"anything-but": 0}]}`)
	require.Len(t, policy["price"], 1)
	assert.Equal(t, conditionAnythingBut, policy["price"][0].kind)
	assert.Equal(t, []float64{0}, policy["price"][0].anythingButNumbers)
}

func TestParseFilterPolicy_NumericBetween(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"numeric": [">=", 100, "<=", 200]}]}`)
	require.Len(t, policy["price"], 1)
	assert.Equal(t, conditionNumeric, policy["price"][0].kind)
	require.Len(t, policy["price"][0].numericOps, 2)
	assert.Equal(t, ">=", policy["price"][0].numericOps[0].op)
	assert.Equal(t, float64(100), policy["price"][0].numericOps[0].value)
	assert.Equal(t, "<=", policy["price"][0].numericOps[1].op)
	assert.Equal(t, float64(200), policy["price"][0].numericOps[1].value)
}

func TestParseFilterPolicy_NumericGreaterThan(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"numeric": [">", 0]}]}`)
	require.Len(t, policy["price"], 1)
	require.Len(t, policy["price"][0].numericOps, 1)
	assert.Equal(t, ">", policy["price"][0].numericOps[0].op)
}

func TestParseFilterPolicy_NumericInvalidOp(t *testing.T) {
	_, err := parseFilterPolicy(`{"price": [{"numeric": ["!=", 0]}]}`)
	assert.ErrorIs(t, err, ErrInvalidParameter)
}

func TestParseFilterPolicy_NumericOddElements(t *testing.T) {
	_, err := parseFilterPolicy(`{"price": [{"numeric": [">", 0, "<="]}]}`)
	assert.ErrorIs(t, err, ErrInvalidParameter)
}

func TestParseFilterPolicy_Prefix(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"event_type": [{"prefix": "order_"}]}`)
	require.Len(t, policy["event_type"], 1)
	assert.Equal(t, conditionPrefix, policy["event_type"][0].kind)
	assert.Equal(t, "order_", policy["event_type"][0].prefix)
}

func TestParseFilterPolicy_UnsupportedOperator(t *testing.T) {
	_, err := parseFilterPolicy(`{"key": [{"suffix": "abc"}]}`)
	assert.ErrorIs(t, err, ErrInvalidParameter)
}

func TestParseFilterPolicy_MultipleKeys(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{
		"event_type": ["order_created"],
		"store": ["us-east", "us-west"]
	}`)
	assert.Len(t, policy, 2)
	assert.Len(t, policy["event_type"], 1)
	assert.Len(t, policy["store"], 2)
}

// --- matchesFilterPolicy tests ---

func TestMatchesFilterPolicy_NilPolicy(t *testing.T) {
	assert.True(t, matchesFilterPolicy(nil, nil))
	assert.True(t, matchesFilterPolicy(nil, map[string]MessageAttribute{
		"key": {DataType: "String", StringValue: "val"},
	}))
}

func TestMatchesFilterPolicy_EmptyPolicy(t *testing.T) {
	assert.True(t, matchesFilterPolicy(filterPolicy{}, nil))
}

func TestMatchesFilterPolicy_ExactStringMatch(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"event_type": ["order_created"]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_created"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_updated"},
	}))

	// Missing attribute
	assert.False(t, matchesFilterPolicy(policy, nil))
	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{}))
}

func TestMatchesFilterPolicy_AllowlistMatch(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"event_type": ["created", "updated"]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "created"},
	}))

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "updated"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "deleted"},
	}))
}

func TestMatchesFilterPolicy_ExactNumericMatch(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [100]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "100"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "200"},
	}))

	// String type doesn't match numeric condition
	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "String", StringValue: "100"},
	}))
}

func TestMatchesFilterPolicy_ExistsTrue(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"customer_id": [{"exists": true}]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"customer_id": {DataType: "String", StringValue: "abc"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{}))
	assert.False(t, matchesFilterPolicy(policy, nil))
}

func TestMatchesFilterPolicy_ExistsFalse(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"customer_id": [{"exists": false}]}`)

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"customer_id": {DataType: "String", StringValue: "abc"},
	}))

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{}))
	assert.True(t, matchesFilterPolicy(policy, nil))
}

func TestMatchesFilterPolicy_AnythingButString(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"status": [{"anything-but": ["cancelled", "failed"]}]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"status": {DataType: "String", StringValue: "active"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"status": {DataType: "String", StringValue: "cancelled"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"status": {DataType: "String", StringValue: "failed"},
	}))

	// Missing attribute doesn't match anything-but
	assert.False(t, matchesFilterPolicy(policy, nil))
}

func TestMatchesFilterPolicy_AnythingButNumber(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"anything-but": [0, -1]}]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "100"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "0"},
	}))
}

func TestMatchesFilterPolicy_NumericGreaterThan(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"numeric": [">", 0]}]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "100"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "0"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "-1"},
	}))
}

func TestMatchesFilterPolicy_NumericBetween(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"numeric": [">=", 100, "<=", 200]}]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "100"},
	}))

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "150"},
	}))

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "200"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "99"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "201"},
	}))
}

func TestMatchesFilterPolicy_NumericEquals(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"numeric": ["=", 42]}]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "42"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "Number", StringValue: "43"},
	}))
}

func TestMatchesFilterPolicy_NumericNonNumberType(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"price": [{"numeric": [">", 0]}]}`)

	// String-type attribute doesn't match numeric condition
	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"price": {DataType: "String", StringValue: "100"},
	}))
}

func TestMatchesFilterPolicy_Prefix(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{"event_type": [{"prefix": "order_"}]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_created"},
	}))

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_updated"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "user_created"},
	}))
}

func TestMatchesFilterPolicy_MultipleKeysAND(t *testing.T) {
	policy := mustParseFilterPolicy(t, `{
		"event_type": ["order_created"],
		"store": ["us-east"]
	}`)

	// Both match
	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_created"},
		"store":      {DataType: "String", StringValue: "us-east"},
	}))

	// Only one matches
	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_created"},
		"store":      {DataType: "String", StringValue: "eu-west"},
	}))

	// Neither matches
	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "user_created"},
		"store":      {DataType: "String", StringValue: "eu-west"},
	}))

	// Missing key
	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_created"},
	}))
}

func TestMatchesFilterPolicy_MixedConditionTypes(t *testing.T) {
	// String or numeric in same key: both are in OR
	policy := mustParseFilterPolicy(t, `{"value": ["special", 42]}`)

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"value": {DataType: "String", StringValue: "special"},
	}))

	assert.True(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"value": {DataType: "Number", StringValue: "42"},
	}))

	assert.False(t, matchesFilterPolicy(policy, map[string]MessageAttribute{
		"value": {DataType: "String", StringValue: "other"},
	}))
}
