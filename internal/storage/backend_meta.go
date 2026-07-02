package storage

import (
	"encoding/json"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// objMetaMut mutates a single object version's metadata in place.
type objMetaMut func(v *types.ObjectMeta, c *command.Command) error

// applyObjectMetaMutation locates the target version (specified or latest) and
// applies mut, then persists the key's history.
func (b *posixBackend) applyObjectMetaMutation(c *command.Command, mut objMetaMut) error {
	km, err := b.loadKeyMeta(c.Bucket, c.Key)
	if err != nil {
		return err
	}
	km.Synthesized = false // a replicated metadata change makes the key first-class
	idx := 0
	if c.VersionID != "" {
		found := false
		for i := range km.Versions {
			if km.Versions[i].VersionID == c.VersionID {
				idx, found = i, true
				break
			}
		}
		if !found {
			return s3err.ErrNoSuchVersion
		}
	} else if km.Latest() == nil || km.Versions[0].DeleteMarker {
		return s3err.ErrNoSuchKey
	}
	if err := mut(&km.Versions[idx], c); err != nil {
		return err
	}
	return b.writeKeyMeta(km)
}

func mutObjectACL(v *types.ObjectMeta, c *command.Command) error {
	var acl types.ACL
	if err := json.Unmarshal(c.Config, &acl); err != nil {
		return err
	}
	v.ACL = &acl
	return nil
}

func mutObjectTags(v *types.ObjectMeta, c *command.Command) error {
	var tags map[string]string
	if err := json.Unmarshal(c.Config, &tags); err != nil {
		return err
	}
	v.Tags = tags
	return nil
}

func mutObjectDelTags(v *types.ObjectMeta, _ *command.Command) error {
	v.Tags = nil
	return nil
}

func mutObjectRetention(v *types.ObjectMeta, c *command.Command) error {
	var r types.Retention
	if err := json.Unmarshal(c.Config, &r); err != nil {
		return err
	}
	v.Retention = &r
	return nil
}

func mutObjectLegalHold(v *types.ObjectMeta, c *command.Command) error {
	var lh types.LegalHold
	if err := json.Unmarshal(c.Config, &lh); err != nil {
		return err
	}
	v.LegalHold = &lh
	return nil
}
