package gateway

import (
	"github.com/adi/d9ds3/internal/command"
	"github.com/adi/d9ds3/internal/types"
)

// PutAccount creates or updates an IAM account (replicated via the log).
func (g *Gateway) PutAccount(ctx Ctx, a types.Account) (uint64, error) {
	return g.submit(&command.Command{Op: command.OpPutAccount, Config: jsonBytes(a), IssuedBy: ctx.Account})
}

// DeleteAccount removes an IAM account by access-key id.
func (g *Gateway) DeleteAccount(ctx Ctx, accessKeyID string) (uint64, error) {
	return g.submit(&command.Command{Op: command.OpDeleteAccount, Key: accessKeyID, IssuedBy: ctx.Account})
}

// GetAccount / ListAccounts delegate to the cluster (used for auth + admin).
func (g *Gateway) GetAccount(accessKeyID string) (*types.Account, error) {
	return g.cl.GetAccount(accessKeyID)
}
func (g *Gateway) ListAccounts() ([]types.Account, error) { return g.cl.ListAccounts() }
