// Package authz implements access-control evaluation for the S3 gateway:
// AWS-style bucket-policy evaluation, ACL permission checks, and canned-ACL
// construction. It is intentionally dependency-free apart from internal/types.
package authz

import (
	"encoding/json"
	"net"
	"regexp"
	"strings"

	"github.com/adi/d9ds3/internal/types"
)

// Decision is the outcome of a bucket-policy evaluation.
type Decision int

const (
	// DecisionNone means no statement matched (neither Allow nor Deny).
	DecisionNone Decision = iota
	// DecisionAllow means an Allow statement matched and no Deny did.
	DecisionAllow
	// DecisionDeny means a Deny statement matched (Deny always wins).
	DecisionDeny
)

// Request describes an access attempt for bucket-policy evaluation.
type Request struct {
	Principal       string // authenticated account id, or "" if anonymous
	IsAuthenticated bool
	Action          string // e.g. "s3:GetObject", "s3:PutObject", "s3:ListBucket"
	Bucket          string
	Key             string // object key ("" for bucket-level actions)
	SourceIP        string
}

const arnPrefix = "arn:aws:s3:::"

// ---- bucket policy document model ----

// policyDoc is the top-level bucket-policy JSON structure.
type policyDoc struct {
	Version   string      `json:"Version"`
	ID        string      `json:"Id"`
	Statement []statement `json:"Statement"`
}

// statement is one policy statement. Principal, Action, Resource and the
// condition-value leaves may each be a scalar or an array in the JSON, so they
// are captured as json.RawMessage and normalized lazily.
type statement struct {
	Sid       string          `json:"Sid"`
	Effect    string          `json:"Effect"`
	Principal json.RawMessage `json:"Principal"`
	Action    json.RawMessage `json:"Action"`
	Resource  json.RawMessage `json:"Resource"`
	Condition condition       `json:"Condition"`
}

// condition maps an operator name (e.g. "IpAddress", "StringEquals") to a set
// of condition keys, each of which maps to one or more expected values.
type condition map[string]map[string]stringOrSlice

// stringOrSlice unmarshals either a JSON string or a JSON array of strings.
type stringOrSlice []string

func (s *stringOrSlice) UnmarshalJSON(b []byte) error {
	b = []byte(strings.TrimSpace(string(b)))
	if len(b) == 0 || string(b) == "null" {
		*s = nil
		return nil
	}
	if b[0] == '[' {
		var arr []string
		if err := json.Unmarshal(b, &arr); err != nil {
			return err
		}
		*s = arr
		return nil
	}
	var single string
	if err := json.Unmarshal(b, &single); err != nil {
		return err
	}
	*s = []string{single}
	return nil
}

// EvaluatePolicy parses an AWS S3 bucket policy (JSON) and evaluates req against
// it. Deny beats Allow; no matching statement => DecisionNone. It never panics
// on malformed input (returns DecisionNone instead).
func EvaluatePolicy(policyJSON []byte, req Request) Decision {
	if len(policyJSON) == 0 {
		return DecisionNone
	}
	var doc policyDoc
	if err := json.Unmarshal(policyJSON, &doc); err != nil {
		return DecisionNone
	}

	allow := false
	for i := range doc.Statement {
		st := &doc.Statement[i]
		if !statementMatches(st, req) {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(st.Effect)) {
		case "deny":
			return DecisionDeny
		case "allow":
			allow = true
		}
	}
	if allow {
		return DecisionAllow
	}
	return DecisionNone
}

// statementMatches reports whether every dimension of st (principal, action,
// resource, condition) matches req. Any parse problem or unknown/unsupported
// condition fails the statement closed.
func statementMatches(st *statement, req Request) bool {
	return matchPrincipal(st.Principal, req) &&
		matchAction(st.Action, req.Action) &&
		matchResource(st.Resource, req) &&
		matchConditions(st.Condition, req)
}

// ---- principal ----

func matchPrincipal(raw json.RawMessage, req Request) bool {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	// "Principal":"*"
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return false
		}
		return principalIDMatches(s, req)
	}
	// "Principal":{"AWS":"..." | ["...",...]} (also tolerate other service keys)
	var obj map[string]stringOrSlice
	if err := json.Unmarshal(raw, &obj); err != nil {
		return false
	}
	for _, vals := range obj {
		for _, v := range vals {
			if principalIDMatches(v, req) {
				return true
			}
		}
	}
	return false
}

// principalIDMatches tests a single principal entry against the request.
// Anonymous requests only match the wildcard "*".
func principalIDMatches(entry string, req Request) bool {
	entry = strings.TrimSpace(entry)
	if entry == "*" {
		return true
	}
	if !req.IsAuthenticated || req.Principal == "" {
		return false
	}
	if entry == req.Principal {
		return true
	}
	// ARN forms such as arn:aws:iam::<acct>:root or .../user/<name>.
	if strings.HasSuffix(entry, ":root") {
		return arnAccountID(entry) == req.Principal
	}
	if id := arnAccountID(entry); id != "" && id == req.Principal {
		return true
	}
	return strings.HasSuffix(entry, ":"+req.Principal)
}

// arnAccountID extracts the account-id field from an IAM ARN
// (arn:aws:iam::<account>:...), returning "" if not present.
func arnAccountID(arn string) string {
	if !strings.HasPrefix(arn, "arn:") {
		return ""
	}
	parts := strings.Split(arn, ":")
	if len(parts) >= 5 {
		return parts[4]
	}
	return ""
}

// ---- action ----

func matchAction(raw json.RawMessage, action string) bool {
	actions, ok := decodeStringOrSlice(raw)
	if !ok {
		return false
	}
	for _, pat := range actions {
		if wildcardMatchFold(pat, action) {
			return true
		}
	}
	return false
}

// ---- resource ----

func matchResource(raw json.RawMessage, req Request) bool {
	resources, ok := decodeStringOrSlice(raw)
	if !ok {
		return false
	}
	target := req.Bucket
	if req.Key != "" {
		target = req.Bucket + "/" + req.Key
	}
	for _, r := range resources {
		r = strings.TrimSpace(r)
		if r == "*" {
			return true
		}
		r = strings.TrimPrefix(r, arnPrefix)
		if wildcardMatch(r, target) {
			return true
		}
	}
	return false
}

// ---- conditions ----

func matchConditions(cond condition, req Request) bool {
	for op, keys := range cond {
		if !matchConditionOp(op, keys, req) {
			return false
		}
	}
	return true
}

// matchConditionOp evaluates a single condition operator over all its keys.
// Unknown operators or keys fail closed.
func matchConditionOp(op string, keys map[string]stringOrSlice, req Request) bool {
	switch strings.ToLower(op) {
	case "ipaddress":
		return evalIP(keys, req, false)
	case "notipaddress":
		return evalIP(keys, req, true)
	case "stringequals":
		return evalString(keys, req, matchStringEquals)
	case "stringnotequals":
		return evalStringNegated(keys, req, matchStringEquals)
	case "stringlike":
		return evalString(keys, req, matchStringLike)
	case "stringnotlike":
		return evalStringNegated(keys, req, matchStringLike)
	default:
		// Unknown operator: fail closed.
		return false
	}
}

// evalIP handles IpAddress / NotIpAddress on aws:SourceIp. When negate is true
// the request IP must be outside every listed CIDR/address.
func evalIP(keys map[string]stringOrSlice, req Request, negate bool) bool {
	for key, cidrs := range keys {
		if !strings.EqualFold(key, "aws:SourceIp") {
			return false // unknown key => fail closed
		}
		ip := net.ParseIP(req.SourceIP)
		if ip == nil {
			return false
		}
		in := ipInAny(ip, cidrs)
		if negate {
			if in {
				return false
			}
		} else {
			if !in {
				return false
			}
		}
	}
	return true
}

func ipInAny(ip net.IP, cidrs []string) bool {
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if !strings.Contains(c, "/") {
			if p := net.ParseIP(c); p != nil && p.Equal(ip) {
				return true
			}
			continue
		}
		_, network, err := net.ParseCIDR(c)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// evalString applies a positive string matcher: for each key, at least one of
// the listed values must satisfy the matcher against the request value.
func evalString(keys map[string]stringOrSlice, req Request, m func(pat, val string) bool) bool {
	for key, vals := range keys {
		reqVal, ok := conditionKeyValue(key, req)
		if !ok {
			return false // unknown key => fail closed
		}
		matched := false
		for _, v := range vals {
			if m(v, reqVal) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// evalStringNegated applies a negated string matcher: the request value must
// match none of the listed values.
func evalStringNegated(keys map[string]stringOrSlice, req Request, m func(pat, val string) bool) bool {
	for key, vals := range keys {
		reqVal, ok := conditionKeyValue(key, req)
		if !ok {
			return false
		}
		for _, v := range vals {
			if m(v, reqVal) {
				return false
			}
		}
	}
	return true
}

func matchStringEquals(pat, val string) bool { return pat == val }
func matchStringLike(pat, val string) bool   { return wildcardMatch(pat, val) }

// conditionKeyValue resolves a supported condition key to its value on the
// request. The bool result is false for unsupported keys (fail closed).
func conditionKeyValue(key string, req Request) (string, bool) {
	switch strings.ToLower(key) {
	case "s3:prefix":
		return req.Key, true
	case "aws:username", "aws:userid", "aws:principalaccount":
		return req.Principal, true
	case "s3:x-amz-acl":
		return "", true
	default:
		return "", false
	}
}

// ---- helpers ----

// decodeStringOrSlice decodes a raw JSON value that may be a string or a string
// array. It returns ok=false on absent or malformed input.
func decodeStringOrSlice(raw json.RawMessage) ([]string, bool) {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 || string(raw) == "null" {
		return nil, false
	}
	var s stringOrSlice
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, false
	}
	return []string(s), true
}

// wildcardMatch reports whether the glob pattern (with "*" and "?") matches s.
func wildcardMatch(pattern, s string) bool {
	return compileGlob(pattern, false).MatchString(s)
}

// wildcardMatchFold is like wildcardMatch but case-insensitive, used for action
// names to stay lenient about "s3:getobject" vs "s3:GetObject".
func wildcardMatchFold(pattern, s string) bool {
	return compileGlob(pattern, true).MatchString(s)
}

// compileGlob turns a glob (* and ?) into an anchored regexp.
func compileGlob(pattern string, fold bool) *regexp.Regexp {
	var b strings.Builder
	if fold {
		b.WriteString("(?i)")
	}
	b.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	// Pattern is always a valid regexp because every literal run is quoted.
	return regexp.MustCompile(b.String())
}

// ---- ACL evaluation ----

// permImplies reports whether holding permission `have` satisfies a requirement
// for `want`. FULL_CONTROL implies everything; otherwise permissions are exact.
func permImplies(have, want string) bool {
	have = strings.ToUpper(strings.TrimSpace(have))
	want = strings.ToUpper(strings.TrimSpace(want))
	if have == "FULL_CONTROL" {
		return true
	}
	return have == want
}

// CheckACL reports whether accountID (with isAuthenticated) holds the required
// permission on acl. required is one of READ|WRITE|READ_ACP|WRITE_ACP|
// FULL_CONTROL. The ACL owner always has FULL_CONTROL. CanonicalUser grants
// match by ID; Group grants honor AllUsers (everyone) and AuthenticatedUsers
// (any authenticated principal). FULL_CONTROL implies all other permissions.
func CheckACL(acl *types.ACL, accountID string, isAuthenticated bool, required string) bool {
	if acl == nil {
		return false
	}
	// Owner always has FULL_CONTROL.
	if accountID != "" && accountID == acl.Owner.ID {
		return true
	}
	for _, g := range acl.Grants {
		if !permImplies(g.Permission, required) {
			continue
		}
		if granteeMatches(g.Grantee, accountID, isAuthenticated) {
			return true
		}
	}
	return false
}

// granteeMatches reports whether the caller identity satisfies a grantee.
func granteeMatches(gr types.Grantee, accountID string, isAuthenticated bool) bool {
	switch gr.Type {
	case "CanonicalUser":
		return accountID != "" && gr.ID == accountID
	case "Group":
		switch gr.URI {
		case types.GroupAllUsers:
			return true
		case types.GroupAuthenticatedUsers:
			return isAuthenticated
		}
	}
	return false
}

// ---- canned ACLs ----

func fullControlGrant(o types.Owner) types.Grant {
	return types.Grant{
		Grantee:    types.Grantee{Type: "CanonicalUser", ID: o.ID, DisplayName: o.DisplayName},
		Permission: "FULL_CONTROL",
	}
}

func groupGrant(uri, perm string) types.Grant {
	return types.Grant{
		Grantee:    types.Grantee{Type: "Group", URI: uri},
		Permission: perm,
	}
}

func userGrant(o types.Owner, perm string) types.Grant {
	return types.Grant{
		Grantee:    types.Grantee{Type: "CanonicalUser", ID: o.ID, DisplayName: o.DisplayName},
		Permission: perm,
	}
}

// CannedACL builds an ACL for a canned-ACL name with the given owner. Supported
// names: "private" (default), "public-read", "public-read-write",
// "authenticated-read", "bucket-owner-read", "bucket-owner-full-control". Any
// unknown name yields a private ACL. bucketOwner is used for the
// bucket-owner-* canned ACLs and may equal owner.
func CannedACL(name string, owner types.Owner, bucketOwner types.Owner) *types.ACL {
	acl := &types.ACL{
		Owner:  owner,
		Grants: []types.Grant{fullControlGrant(owner)},
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "public-read":
		acl.Grants = append(acl.Grants, groupGrant(types.GroupAllUsers, "READ"))
	case "public-read-write":
		acl.Grants = append(acl.Grants,
			groupGrant(types.GroupAllUsers, "READ"),
			groupGrant(types.GroupAllUsers, "WRITE"))
	case "authenticated-read":
		acl.Grants = append(acl.Grants, groupGrant(types.GroupAuthenticatedUsers, "READ"))
	case "bucket-owner-read":
		if bucketOwner.ID != "" && bucketOwner.ID != owner.ID {
			acl.Grants = append(acl.Grants, userGrant(bucketOwner, "READ"))
		}
	case "bucket-owner-full-control":
		if bucketOwner.ID != "" && bucketOwner.ID != owner.ID {
			acl.Grants = append(acl.Grants, userGrant(bucketOwner, "FULL_CONTROL"))
		}
	default:
		// "private" and any unknown name: owner-only FULL_CONTROL (already set).
	}
	return acl
}
