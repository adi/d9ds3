package s3api

import (
	"encoding/json"
	"net/http"

	"github.com/adi/d9ds3/internal/s3err"
	"github.com/adi/d9ds3/internal/types"
)

// handleAdmin is a minimal IAM admin API, reachable at "/?admin&action=...".
// It requires the admin role. Accounts are replicated via the log, so a change is
// visible to every gateway/replica.
func (s *Server) handleAdmin(rc *reqCtx) {
	if rc.role != "admin" {
		writeErr(rc.w, rc.r, s3err.ErrAccessDenied)
		return
	}
	switch rc.q.get("action") {
	case "create-account":
		var a types.Account
		body, _ := readBody(rc)
		if err := json.Unmarshal(body, &a); err != nil || a.AccessKeyID == "" || a.SecretKey == "" {
			writeErr(rc.w, rc.r, s3err.ErrInvalidArgument)
			return
		}
		if a.Role == "" {
			a.Role = "user"
		}
		if _, err := s.gw.PutAccount(rc.gctx, a); err != nil {
			writeErr(rc.w, rc.r, err)
			return
		}
		writeAdminJSON(rc.w, map[string]string{"status": "created", "access_key_id": a.AccessKeyID})
	case "delete-account":
		ak := rc.q.get("access-key")
		if ak == "" {
			writeErr(rc.w, rc.r, s3err.ErrInvalidArgument)
			return
		}
		if _, err := s.gw.DeleteAccount(rc.gctx, ak); err != nil {
			writeErr(rc.w, rc.r, err)
			return
		}
		writeAdminJSON(rc.w, map[string]string{"status": "deleted", "access_key_id": ak})
	case "list-accounts":
		accts, err := s.gw.ListAccounts()
		if err != nil {
			writeErr(rc.w, rc.r, err)
			return
		}
		writeAdminJSON(rc.w, accts)
	default:
		writeErr(rc.w, rc.r, s3err.ErrNotImplemented)
	}
}

func writeAdminJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(v)
}
