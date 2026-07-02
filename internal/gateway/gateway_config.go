package gateway

import (
	"encoding/json"

	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/types"
)

// ---- bucket configuration ----

func (g *Gateway) PutBucketACL(ctx Ctx, bucket string, acl *types.ACL) (uint64, error) {
	return g.submitConfig(command.OpPutBucketAcl, bucket, "", "", ctx, jsonBytes(acl))
}

func (g *Gateway) PutBucketPolicy(ctx Ctx, bucket string, policy []byte) (uint64, error) {
	return g.submitConfig(command.OpPutBucketPolicy, bucket, "", "", ctx, policy)
}

func (g *Gateway) DeleteBucketPolicy(ctx Ctx, bucket string) (uint64, error) {
	return g.submitConfig(command.OpDeleteBucketPolicy, bucket, "", "", ctx, nil)
}

func (g *Gateway) PutBucketCors(ctx Ctx, bucket string, rules []types.CORSRule) (uint64, error) {
	return g.submitConfig(command.OpPutBucketCors, bucket, "", "", ctx, jsonBytes(rules))
}

func (g *Gateway) DeleteBucketCors(ctx Ctx, bucket string) (uint64, error) {
	return g.submitConfig(command.OpDeleteBucketCors, bucket, "", "", ctx, nil)
}

func (g *Gateway) PutBucketTagging(ctx Ctx, bucket string, tags map[string]string) (uint64, error) {
	return g.submitConfig(command.OpPutBucketTagging, bucket, "", "", ctx, jsonBytes(tags))
}

func (g *Gateway) DeleteBucketTagging(ctx Ctx, bucket string) (uint64, error) {
	return g.submitConfig(command.OpDeleteBucketTag, bucket, "", "", ctx, nil)
}

func (g *Gateway) PutBucketVersioning(ctx Ctx, bucket, status string) (uint64, error) {
	return g.submitConfig(command.OpPutBucketVersioning, bucket, "", "", ctx, []byte(status))
}

func (g *Gateway) PutBucketOwnership(ctx Ctx, bucket, ownership string) (uint64, error) {
	return g.submitConfig(command.OpPutBucketOwnership, bucket, "", "", ctx, []byte(ownership))
}

func (g *Gateway) DeleteBucketOwnership(ctx Ctx, bucket string) (uint64, error) {
	return g.submitConfig(command.OpDeleteBucketOwner, bucket, "", "", ctx, nil)
}

func (g *Gateway) PutObjectLockConfig(ctx Ctx, bucket string, cfg *types.ObjectLockConfig) (uint64, error) {
	return g.submitConfig(command.OpPutObjectLockConfig, bucket, "", "", ctx, jsonBytes(cfg))
}

// ---- object metadata ----

func (g *Gateway) PutObjectACL(ctx Ctx, bucket, key, version string, acl *types.ACL) (uint64, error) {
	return g.submitConfig(command.OpPutObjectAcl, bucket, key, version, ctx, jsonBytes(acl))
}

func (g *Gateway) PutObjectTagging(ctx Ctx, bucket, key, version string, tags map[string]string) (uint64, error) {
	return g.submitConfig(command.OpPutObjectTagging, bucket, key, version, ctx, jsonBytes(tags))
}

func (g *Gateway) DeleteObjectTagging(ctx Ctx, bucket, key, version string) (uint64, error) {
	return g.submitConfig(command.OpDeleteObjectTag, bucket, key, version, ctx, nil)
}

func (g *Gateway) PutObjectRetention(ctx Ctx, bucket, key, version string, r *types.Retention) (uint64, error) {
	return g.submitConfig(command.OpPutObjectRetention, bucket, key, version, ctx, jsonBytes(r))
}

func (g *Gateway) PutObjectLegalHold(ctx Ctx, bucket, key, version, status string) (uint64, error) {
	return g.submitConfig(command.OpPutObjectLegalHold, bucket, key, version, ctx, jsonBytes(&types.LegalHold{Status: status}))
}

// submitConfig builds and submits a config-setting command.
func (g *Gateway) submitConfig(op command.Op, bucket, key, version string, ctx Ctx, config []byte) (uint64, error) {
	return g.submit(&command.Command{
		Op: op, Bucket: bucket, Key: key, VersionID: version,
		Config: config, IssuedBy: ctx.Account,
	})
}

func jsonBytes(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
