package authz

import (
	"testing"

	"github.com/adi/d9ds3/internal/types"
)

// ---- EvaluatePolicy ----

func TestEvaluatePolicy_AllowMatch(t *testing.T) {
	pol := []byte(`{
		"Version":"2012-10-17",
		"Statement":[{
			"Sid":"AllowGet",
			"Effect":"Allow",
			"Principal":"*",
			"Action":"s3:GetObject",
			"Resource":"arn:aws:s3:::mybucket/*"
		}]
	}`)
	req := Request{Action: "s3:GetObject", Bucket: "mybucket", Key: "a/b.txt"}
	if got := EvaluatePolicy(pol, req); got != DecisionAllow {
		t.Fatalf("want DecisionAllow, got %v", got)
	}
}

func TestEvaluatePolicy_NoMatch(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"s3:GetObject",
		"Resource":"arn:aws:s3:::other/*"}]}`)
	req := Request{Action: "s3:GetObject", Bucket: "mybucket", Key: "x"}
	if got := EvaluatePolicy(pol, req); got != DecisionNone {
		t.Fatalf("want DecisionNone, got %v", got)
	}
}

func TestEvaluatePolicy_DenyBeatsAllow(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::b/*"},
		{"Effect":"Deny","Principal":"*","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::b/*"}
	]}`)
	del := Request{Action: "s3:DeleteObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, del); got != DecisionDeny {
		t.Fatalf("delete: want DecisionDeny, got %v", got)
	}
	get := Request{Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, get); got != DecisionAllow {
		t.Fatalf("get: want DecisionAllow, got %v", got)
	}
}

func TestEvaluatePolicy_WildcardActionAndResource(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"s3:Get*",
		"Resource":["arn:aws:s3:::b","arn:aws:s3:::b/*"]}]}`)
	cases := []struct {
		name string
		req  Request
		want Decision
	}{
		{"GetObject on object", Request{Action: "s3:GetObject", Bucket: "b", Key: "k"}, DecisionAllow},
		{"GetBucketLocation bucket-level", Request{Action: "s3:GetBucketLocation", Bucket: "b"}, DecisionAllow},
		{"PutObject not allowed", Request{Action: "s3:PutObject", Bucket: "b", Key: "k"}, DecisionNone},
		{"wrong bucket", Request{Action: "s3:GetObject", Bucket: "c", Key: "k"}, DecisionNone},
	}
	for _, tc := range cases {
		if got := EvaluatePolicy(pol, tc.req); got != tc.want {
			t.Errorf("%s: want %v, got %v", tc.name, tc.want, got)
		}
	}
}

func TestEvaluatePolicy_StarAction(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`)
	req := Request{Action: "s3:AnythingGoes", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, req); got != DecisionAllow {
		t.Fatalf("want DecisionAllow, got %v", got)
	}
}

func TestEvaluatePolicy_AnonymousVsAuthenticatedPrincipal(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":{"AWS":"acct-123"},
		"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`)

	anon := Request{Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, anon); got != DecisionNone {
		t.Errorf("anonymous: want DecisionNone, got %v", got)
	}
	auth := Request{Principal: "acct-123", IsAuthenticated: true, Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, auth); got != DecisionAllow {
		t.Errorf("authenticated: want DecisionAllow, got %v", got)
	}
	other := Request{Principal: "acct-999", IsAuthenticated: true, Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, other); got != DecisionNone {
		t.Errorf("other principal: want DecisionNone, got %v", got)
	}
}

func TestEvaluatePolicy_PrincipalRootArn(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::acct-123:root"]},
		"Action":"s3:*","Resource":"arn:aws:s3:::b/*"}]}`)
	req := Request{Principal: "acct-123", IsAuthenticated: true, Action: "s3:PutObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, req); got != DecisionAllow {
		t.Fatalf("root arn: want DecisionAllow, got %v", got)
	}
}

func TestEvaluatePolicy_PrincipalArrayAndStarEntry(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":{"AWS":["a","*","c"]},
		"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`)
	// "*" entry should match anonymous too.
	req := Request{Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, req); got != DecisionAllow {
		t.Fatalf("star entry: want DecisionAllow, got %v", got)
	}
}

func TestEvaluatePolicy_IpConditionAllowAndDeny(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"s3:GetObject",
		"Resource":"arn:aws:s3:::b/*",
		"Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/24"}}}]}`)

	in := Request{Action: "s3:GetObject", Bucket: "b", Key: "k", SourceIP: "10.0.0.55"}
	if got := EvaluatePolicy(pol, in); got != DecisionAllow {
		t.Errorf("in-range: want DecisionAllow, got %v", got)
	}
	out := Request{Action: "s3:GetObject", Bucket: "b", Key: "k", SourceIP: "192.168.1.1"}
	if got := EvaluatePolicy(pol, out); got != DecisionNone {
		t.Errorf("out-of-range: want DecisionNone, got %v", got)
	}
	bad := Request{Action: "s3:GetObject", Bucket: "b", Key: "k", SourceIP: "not-an-ip"}
	if got := EvaluatePolicy(pol, bad); got != DecisionNone {
		t.Errorf("bad-ip: want DecisionNone, got %v", got)
	}
}

func TestEvaluatePolicy_NotIpAddressDeny(t *testing.T) {
	// Deny anything NOT from the office range.
	pol := []byte(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::b/*"},
		{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::b/*",
		 "Condition":{"NotIpAddress":{"aws:SourceIp":"10.0.0.0/24"}}}
	]}`)
	office := Request{Action: "s3:GetObject", Bucket: "b", Key: "k", SourceIP: "10.0.0.9"}
	if got := EvaluatePolicy(pol, office); got != DecisionAllow {
		t.Errorf("office ip: want DecisionAllow, got %v", got)
	}
	outside := Request{Action: "s3:GetObject", Bucket: "b", Key: "k", SourceIP: "8.8.8.8"}
	if got := EvaluatePolicy(pol, outside); got != DecisionDeny {
		t.Errorf("outside ip: want DecisionDeny, got %v", got)
	}
}

func TestEvaluatePolicy_StringLikePrefix(t *testing.T) {
	// s3:prefix maps to req.Key; use a wildcard Resource so resource matching
	// does not interfere with the condition under test.
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"s3:ListBucket",
		"Resource":"*",
		"Condition":{"StringLike":{"s3:prefix":"home/*"}}}]}`)
	ok := Request{Action: "s3:ListBucket", Bucket: "b", Key: "home/photos"}
	if got := EvaluatePolicy(pol, ok); got != DecisionAllow {
		t.Errorf("matching prefix: want DecisionAllow, got %v", got)
	}
	no := Request{Action: "s3:ListBucket", Bucket: "b", Key: "other/x"}
	if got := EvaluatePolicy(pol, no); got != DecisionNone {
		t.Errorf("non-matching prefix: want DecisionNone, got %v", got)
	}
}

func TestEvaluatePolicy_StringEqualsPrincipal(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"s3:GetObject",
		"Resource":"arn:aws:s3:::b/*",
		"Condition":{"StringEquals":{"aws:username":"alice"}}}]}`)
	ok := Request{Principal: "alice", IsAuthenticated: true, Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, ok); got != DecisionAllow {
		t.Errorf("matching username: want DecisionAllow, got %v", got)
	}
	no := Request{Principal: "bob", IsAuthenticated: true, Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, no); got != DecisionNone {
		t.Errorf("non-matching username: want DecisionNone, got %v", got)
	}
}

func TestEvaluatePolicy_UnknownConditionFailsClosed(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"s3:GetObject",
		"Resource":"arn:aws:s3:::b/*",
		"Condition":{"DateGreaterThan":{"aws:CurrentTime":"2020-01-01T00:00:00Z"}}}]}`)
	req := Request{Action: "s3:GetObject", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, req); got != DecisionNone {
		t.Fatalf("unknown condition op: want DecisionNone, got %v", got)
	}
}

func TestEvaluatePolicy_MalformedDoesNotPanic(t *testing.T) {
	cases := [][]byte{
		nil,
		[]byte(``),
		[]byte(`not json`),
		[]byte(`{"Statement": "oops"}`),
		[]byte(`{"Statement":[{"Effect":"Allow","Principal":123,"Action":{},"Resource":true}]}`),
		[]byte(`{"Statement":[{}]}`),
	}
	req := Request{Action: "s3:GetObject", Bucket: "b", Key: "k"}
	for i, c := range cases {
		if got := EvaluatePolicy(c, req); got != DecisionNone {
			t.Errorf("case %d: want DecisionNone, got %v", i, got)
		}
	}
}

func TestEvaluatePolicy_BucketLevelAction(t *testing.T) {
	pol := []byte(`{"Version":"2012-10-17","Statement":[{
		"Effect":"Allow","Principal":"*","Action":"s3:ListBucket",
		"Resource":"arn:aws:s3:::b"}]}`)
	// bucket-level: no Key
	ok := Request{Action: "s3:ListBucket", Bucket: "b"}
	if got := EvaluatePolicy(pol, ok); got != DecisionAllow {
		t.Errorf("bucket-level: want DecisionAllow, got %v", got)
	}
	// object-level resource should not match a bucket-only resource
	objReq := Request{Action: "s3:ListBucket", Bucket: "b", Key: "k"}
	if got := EvaluatePolicy(pol, objReq); got != DecisionNone {
		t.Errorf("object target against bucket resource: want DecisionNone, got %v", got)
	}
}

// ---- CheckACL ----

func TestCheckACL_OwnerAlwaysFullControl(t *testing.T) {
	acl := &types.ACL{Owner: types.Owner{ID: "owner-1"}}
	for _, perm := range []string{"READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"} {
		if !CheckACL(acl, "owner-1", true, perm) {
			t.Errorf("owner should have %s", perm)
		}
	}
}

func TestCheckACL_CanonicalUserGrant(t *testing.T) {
	acl := &types.ACL{
		Owner: types.Owner{ID: "owner-1"},
		Grants: []types.Grant{
			{Grantee: types.Grantee{Type: "CanonicalUser", ID: "user-2"}, Permission: "READ"},
		},
	}
	if !CheckACL(acl, "user-2", true, "READ") {
		t.Error("user-2 should have READ")
	}
	if CheckACL(acl, "user-2", true, "WRITE") {
		t.Error("user-2 should NOT have WRITE")
	}
	if CheckACL(acl, "user-3", true, "READ") {
		t.Error("user-3 should NOT have READ")
	}
}

func TestCheckACL_FullControlImpliesAll(t *testing.T) {
	acl := &types.ACL{
		Owner: types.Owner{ID: "owner-1"},
		Grants: []types.Grant{
			{Grantee: types.Grantee{Type: "CanonicalUser", ID: "user-2"}, Permission: "FULL_CONTROL"},
		},
	}
	for _, perm := range []string{"READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"} {
		if !CheckACL(acl, "user-2", true, perm) {
			t.Errorf("FULL_CONTROL grant should imply %s", perm)
		}
	}
}

func TestCheckACL_AllUsersGroup(t *testing.T) {
	acl := &types.ACL{
		Owner: types.Owner{ID: "owner-1"},
		Grants: []types.Grant{
			{Grantee: types.Grantee{Type: "Group", URI: types.GroupAllUsers}, Permission: "READ"},
		},
	}
	// anonymous
	if !CheckACL(acl, "", false, "READ") {
		t.Error("AllUsers should grant READ to anonymous")
	}
	// authenticated stranger
	if !CheckACL(acl, "stranger", true, "READ") {
		t.Error("AllUsers should grant READ to any authenticated user")
	}
	if CheckACL(acl, "", false, "WRITE") {
		t.Error("AllUsers READ should not grant WRITE")
	}
}

func TestCheckACL_AuthenticatedUsersGroup(t *testing.T) {
	acl := &types.ACL{
		Owner: types.Owner{ID: "owner-1"},
		Grants: []types.Grant{
			{Grantee: types.Grantee{Type: "Group", URI: types.GroupAuthenticatedUsers}, Permission: "READ"},
		},
	}
	if CheckACL(acl, "", false, "READ") {
		t.Error("AuthenticatedUsers should NOT grant READ to anonymous")
	}
	if !CheckACL(acl, "someone", true, "READ") {
		t.Error("AuthenticatedUsers should grant READ to authenticated caller")
	}
}

func TestCheckACL_NilACL(t *testing.T) {
	if CheckACL(nil, "x", true, "READ") {
		t.Error("nil ACL should deny")
	}
}

// ---- CannedACL ----

func hasGroupGrant(acl *types.ACL, uri, perm string) bool {
	for _, g := range acl.Grants {
		if g.Grantee.Type == "Group" && g.Grantee.URI == uri && g.Permission == perm {
			return true
		}
	}
	return false
}

func hasUserGrant(acl *types.ACL, id, perm string) bool {
	for _, g := range acl.Grants {
		if g.Grantee.Type == "CanonicalUser" && g.Grantee.ID == id && g.Permission == perm {
			return true
		}
	}
	return false
}

func TestCannedACL_Private(t *testing.T) {
	owner := types.Owner{ID: "owner-1", DisplayName: "Owner One"}
	acl := CannedACL("private", owner, owner)
	if acl.Owner.ID != "owner-1" {
		t.Fatalf("owner not set: %+v", acl.Owner)
	}
	if len(acl.Grants) != 1 || !hasUserGrant(acl, "owner-1", "FULL_CONTROL") {
		t.Fatalf("private should be owner-only FULL_CONTROL, got %+v", acl.Grants)
	}
	// owner still passes CheckACL for everything
	if !CheckACL(acl, "owner-1", true, "READ") {
		t.Error("private owner should read")
	}
	if CheckACL(acl, "", false, "READ") {
		t.Error("private should deny anonymous read")
	}
}

func TestCannedACL_UnknownDefaultsPrivate(t *testing.T) {
	owner := types.Owner{ID: "owner-1"}
	acl := CannedACL("bogus-name", owner, owner)
	if len(acl.Grants) != 1 || !hasUserGrant(acl, "owner-1", "FULL_CONTROL") {
		t.Fatalf("unknown canned name should default to private, got %+v", acl.Grants)
	}
}

func TestCannedACL_PublicReadGrantsAllUsersRead(t *testing.T) {
	owner := types.Owner{ID: "owner-1"}
	acl := CannedACL("public-read", owner, owner)
	if !hasGroupGrant(acl, types.GroupAllUsers, "READ") {
		t.Fatalf("public-read must grant READ to AllUsers, got %+v", acl.Grants)
	}
	if hasGroupGrant(acl, types.GroupAllUsers, "WRITE") {
		t.Error("public-read must NOT grant WRITE")
	}
	if !CheckACL(acl, "", false, "READ") {
		t.Error("public-read should let anonymous READ")
	}
	if CheckACL(acl, "", false, "WRITE") {
		t.Error("public-read should NOT let anonymous WRITE")
	}
}

func TestCannedACL_PublicReadWrite(t *testing.T) {
	owner := types.Owner{ID: "owner-1"}
	acl := CannedACL("public-read-write", owner, owner)
	if !hasGroupGrant(acl, types.GroupAllUsers, "READ") || !hasGroupGrant(acl, types.GroupAllUsers, "WRITE") {
		t.Fatalf("public-read-write must grant READ and WRITE to AllUsers, got %+v", acl.Grants)
	}
	if !CheckACL(acl, "", false, "WRITE") {
		t.Error("public-read-write should let anonymous WRITE")
	}
}

func TestCannedACL_AuthenticatedRead(t *testing.T) {
	owner := types.Owner{ID: "owner-1"}
	acl := CannedACL("authenticated-read", owner, owner)
	if !hasGroupGrant(acl, types.GroupAuthenticatedUsers, "READ") {
		t.Fatalf("authenticated-read must grant READ to AuthenticatedUsers, got %+v", acl.Grants)
	}
	if CheckACL(acl, "", false, "READ") {
		t.Error("authenticated-read should deny anonymous")
	}
	if !CheckACL(acl, "someone", true, "READ") {
		t.Error("authenticated-read should allow authenticated")
	}
}

func TestCannedACL_BucketOwnerRead(t *testing.T) {
	owner := types.Owner{ID: "object-writer"}
	bucketOwner := types.Owner{ID: "bucket-owner"}
	acl := CannedACL("bucket-owner-read", owner, bucketOwner)
	if !hasUserGrant(acl, "bucket-owner", "READ") {
		t.Fatalf("bucket-owner-read must grant READ to bucket owner, got %+v", acl.Grants)
	}
	if !hasUserGrant(acl, "object-writer", "FULL_CONTROL") {
		t.Error("object writer keeps FULL_CONTROL")
	}
}

func TestCannedACL_BucketOwnerFullControl(t *testing.T) {
	owner := types.Owner{ID: "object-writer"}
	bucketOwner := types.Owner{ID: "bucket-owner"}
	acl := CannedACL("bucket-owner-full-control", owner, bucketOwner)
	if !hasUserGrant(acl, "bucket-owner", "FULL_CONTROL") {
		t.Fatalf("bucket-owner-full-control must grant FULL_CONTROL to bucket owner, got %+v", acl.Grants)
	}
	if !CheckACL(acl, "bucket-owner", true, "WRITE") {
		t.Error("bucket owner FULL_CONTROL implies WRITE")
	}
}

func TestCannedACL_BucketOwnerSameAsOwner(t *testing.T) {
	owner := types.Owner{ID: "owner-1"}
	acl := CannedACL("bucket-owner-full-control", owner, owner)
	// No duplicate grant when owner == bucketOwner.
	if len(acl.Grants) != 1 {
		t.Fatalf("expected single grant when owner==bucketOwner, got %+v", acl.Grants)
	}
}
