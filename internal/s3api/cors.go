package s3api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/adi/d9ds3/internal/types"
)

// handleCORSPreflight answers an OPTIONS preflight against the bucket's CORS config.
func (s *Server) handleCORSPreflight(w http.ResponseWriter, r *http.Request) {
	bucket, _ := splitPath(r.URL.Path)
	origin := r.Header.Get("Origin")
	reqMethod := r.Header.Get("Access-Control-Request-Method")
	if bucket == "" || origin == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	bm, err := s.gw.GetBucketMeta(bucket)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	rule := matchCORS(bm.CORS, origin, reqMethod)
	if rule == nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	h := w.Header()
	if containsStr(rule.AllowedOrigins, "*") {
		h.Set("Access-Control-Allow-Origin", "*")
	} else {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Add("Vary", "Origin")
	}
	h.Set("Access-Control-Allow-Methods", strings.Join(rule.AllowedMethods, ", "))
	if reqHeaders := r.Header.Get("Access-Control-Request-Headers"); reqHeaders != "" {
		h.Set("Access-Control-Allow-Headers", reqHeaders)
	} else if len(rule.AllowedHeaders) > 0 {
		h.Set("Access-Control-Allow-Headers", strings.Join(rule.AllowedHeaders, ", "))
	}
	if len(rule.ExposeHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(rule.ExposeHeaders, ", "))
	}
	if rule.MaxAgeSeconds > 0 {
		h.Set("Access-Control-Max-Age", strconv.Itoa(rule.MaxAgeSeconds))
	}
	w.WriteHeader(http.StatusOK)
}

// matchCORS returns the first rule matching origin and method, or nil.
func matchCORS(rules []types.CORSRule, origin, method string) *types.CORSRule {
	for i := range rules {
		r := &rules[i]
		if !originMatches(r.AllowedOrigins, origin) {
			continue
		}
		if method != "" && !containsFold(r.AllowedMethods, method) {
			continue
		}
		return r
	}
	return nil
}

func originMatches(allowed []string, origin string) bool {
	for _, a := range allowed {
		if a == "*" || a == origin {
			return true
		}
		if i := strings.IndexByte(a, '*'); i >= 0 {
			prefix, suffix := a[:i], a[i+1:]
			if strings.HasPrefix(origin, prefix) && strings.HasSuffix(origin, suffix) {
				return true
			}
		}
	}
	return false
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
func containsFold(ss []string, want string) bool {
	for _, s := range ss {
		if strings.EqualFold(s, want) {
			return true
		}
	}
	return false
}
