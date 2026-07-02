package storage

import (
	"encoding/json"
	"os"
	"sort"
	"sync"

	"github.com/adi/d9ds3/internal/types"
)

// iamStore holds replicated IAM accounts, persisted as a single JSON map keyed by
// access-key id. Mutated only via FSM.Apply (OpPutAccount/OpDeleteAccount), so it
// stays identical on every replica.
type iamStore struct {
	path     string
	mu       sync.RWMutex
	accounts map[string]types.Account
}

func newIAMStore(path string) *iamStore {
	return &iamStore{path: path, accounts: map[string]types.Account{}}
}

func (s *iamStore) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(b, &s.accounts)
}

func (s *iamStore) persist() error {
	b, err := json.Marshal(s.accounts)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *iamStore) put(config []byte) error {
	var a types.Account
	if err := json.Unmarshal(config, &a); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[a.AccessKeyID] = a
	return s.persist()
}

func (s *iamStore) delete(accessKeyID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.accounts, accessKeyID)
	return s.persist()
}

func (s *iamStore) lookup(accessKeyID string) (types.Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.accounts[accessKeyID]
	return a, ok
}

func (s *iamStore) list() []types.Account {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]types.Account, 0, len(s.accounts))
	for _, a := range s.accounts {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccessKeyID < out[j].AccessKeyID })
	return out
}
