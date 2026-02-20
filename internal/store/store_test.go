package store_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/store"
)

func TestOpenAndClose(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestSaveAndLoadSQSState(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	defer s.Close()

	data := []byte(`[{"name":"q1"}]`)
	require.NoError(t, s.SaveSQSState(data))

	loaded, err := s.LoadSQSState()
	require.NoError(t, err)
	assert.Equal(t, data, loaded)
}

func TestSaveAndLoadSNSState(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	defer s.Close()

	data := []byte(`[{"name":"t1","arn":"arn:aws:sns:eu-central-1:000000000000:t1"}]`)
	require.NoError(t, s.SaveSNSState(data))

	loaded, err := s.LoadSNSState()
	require.NoError(t, err)
	assert.Equal(t, data, loaded)
}

func TestLoadReturnsNilWhenEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	defer s.Close()

	sqsData, err := s.LoadSQSState()
	require.NoError(t, err)
	assert.Nil(t, sqsData)

	snsData, err := s.LoadSNSState()
	require.NoError(t, err)
	assert.Nil(t, snsData)
}

func TestStateSurvivesReopenCycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")

	// Write state.
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.SaveSQSState([]byte("sqs-data")))
	require.NoError(t, s.SaveSNSState([]byte("sns-data")))
	require.NoError(t, s.Close())

	// Reopen and verify.
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	sqsData, err := s2.LoadSQSState()
	require.NoError(t, err)
	assert.Equal(t, []byte("sqs-data"), sqsData)

	snsData, err := s2.LoadSNSState()
	require.NoError(t, err)
	assert.Equal(t, []byte("sns-data"), snsData)
}
