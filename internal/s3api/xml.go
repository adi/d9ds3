package s3api

import (
	"encoding/xml"
	"time"

	"github.com/adi/d9ds3/internal/types"
)

const s3ns = "http://s3.amazonaws.com/doc/2006-03-01/"
const xsiNS = "http://www.w3.org/2001/XMLSchema-instance"

// ---- ListBuckets ----

type xListAllMyBuckets struct {
	XMLName xml.Name  `xml:"ListAllMyBucketsResult"`
	XMLNS   string    `xml:"xmlns,attr"`
	Owner   xOwner    `xml:"Owner"`
	Buckets xBucketsW `xml:"Buckets"`
}
type xBucketsW struct {
	Bucket []xBucket `xml:"Bucket"`
}
type xBucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}
type xOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}

func renderListBuckets(owner string, bs []types.BucketMeta) *xListAllMyBuckets {
	out := &xListAllMyBuckets{XMLNS: s3ns, Owner: xOwner{ID: owner, DisplayName: owner}}
	for _, b := range bs {
		out.Buckets.Bucket = append(out.Buckets.Bucket, xBucket{Name: b.Name, CreationDate: fmtTime(b.CreatedAt)})
	}
	return out
}

// ---- ListObjects (v1 + v2) ----

type xListBucket struct {
	XMLName               xml.Name  `xml:"ListBucketResult"`
	XMLNS                 string    `xml:"xmlns,attr"`
	Name                  string    `xml:"Name"`
	Prefix                string    `xml:"Prefix"`
	Delimiter             string    `xml:"Delimiter,omitempty"`
	MaxKeys               int       `xml:"MaxKeys"`
	KeyCount              int       `xml:"KeyCount,omitempty"`
	IsTruncated           bool      `xml:"IsTruncated"`
	Marker                string    `xml:"Marker,omitempty"`
	NextMarker            string    `xml:"NextMarker,omitempty"`
	ContinuationToken     string    `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
	StartAfter            string    `xml:"StartAfter,omitempty"`
	Contents              []xObject `xml:"Contents"`
	CommonPrefixes        []xPrefix `xml:"CommonPrefixes"`
}
type xObject struct {
	Key          string  `xml:"Key"`
	LastModified string  `xml:"LastModified"`
	ETag         string  `xml:"ETag"`
	Size         int64   `xml:"Size"`
	StorageClass string  `xml:"StorageClass"`
	Owner        *xOwner `xml:"Owner,omitempty"`
}
type xPrefix struct {
	Prefix string `xml:"Prefix"`
}

func renderListObjects(in types.ListInput, res *types.ListResult, v2 bool) *xListBucket {
	out := &xListBucket{
		XMLNS: s3ns, Name: in.Bucket, Prefix: in.Prefix, Delimiter: in.Delimiter,
		MaxKeys: maxKeysOr(in.MaxKeys), IsTruncated: res.IsTruncated,
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, xObject{
			Key: o.Key, LastModified: fmtTime(o.LastModified), ETag: o.ETag,
			Size: o.Size, StorageClass: storageClassOr(o.StorageClass),
		})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, xPrefix{Prefix: p})
	}
	if v2 {
		out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
		out.ContinuationToken = in.ContinuationToken
		out.StartAfter = in.StartAfter
		if res.IsTruncated {
			out.NextContinuationToken = res.NextToken
		}
	} else {
		out.Marker = in.Marker
		if res.IsTruncated {
			out.NextMarker = res.NextMarker
		}
	}
	return out
}

// ---- ListObjectVersions ----

type xListVersions struct {
	XMLName             xml.Name         `xml:"ListVersionsResult"`
	XMLNS               string           `xml:"xmlns,attr"`
	Name                string           `xml:"Name"`
	Prefix              string           `xml:"Prefix"`
	Delimiter           string           `xml:"Delimiter,omitempty"`
	MaxKeys             int              `xml:"MaxKeys"`
	IsTruncated         bool             `xml:"IsTruncated"`
	KeyMarker           string           `xml:"KeyMarker"`
	VersionIdMarker     string           `xml:"VersionIdMarker"`
	NextKeyMarker       string           `xml:"NextKeyMarker,omitempty"`
	NextVersionIdMarker string           `xml:"NextVersionIdMarker,omitempty"`
	Versions            []xVersion       `xml:"Version"`
	DeleteMarkers       []xDeleteMarkerV `xml:"DeleteMarker"`
	CommonPrefixes      []xPrefix        `xml:"CommonPrefixes"`
}
type xVersion struct {
	Key          string `xml:"Key"`
	VersionId    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}
type xDeleteMarkerV struct {
	Key          string `xml:"Key"`
	VersionId    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
}

func renderListVersions(in types.ListVersionsInput, res *types.ListVersionsResult) *xListVersions {
	out := &xListVersions{
		XMLNS: s3ns, Name: in.Bucket, Prefix: in.Prefix, Delimiter: in.Delimiter,
		MaxKeys: maxKeysOr(in.MaxKeys), IsTruncated: res.IsTruncated,
		KeyMarker: in.KeyMarker, VersionIdMarker: in.VersionIDMarker,
		NextKeyMarker: res.NextKeyMarker, NextVersionIdMarker: res.NextVersionIDMarker,
	}
	for _, v := range res.Versions {
		out.Versions = append(out.Versions, xVersion{
			Key: v.Key, VersionId: versionOrNull(v.VersionID), IsLatest: v.IsLatest,
			LastModified: fmtTime(v.LastModified), ETag: v.ETag, Size: v.Size,
			StorageClass: storageClassOr(v.StorageClass),
		})
	}
	for _, v := range res.DeleteMarkers {
		out.DeleteMarkers = append(out.DeleteMarkers, xDeleteMarkerV{
			Key: v.Key, VersionId: versionOrNull(v.VersionID), IsLatest: v.IsLatest,
			LastModified: fmtTime(v.LastModified),
		})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, xPrefix{Prefix: p})
	}
	return out
}

// ---- Copy / Multipart ----

type xCopyResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	XMLNS        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type xInitiateMPU struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type xCompleteMPU struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// CompleteMultipartUpload request body.
type xCompleteReq struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []xCompletePart `xml:"Part"`
}
type xCompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type xListParts struct {
	XMLName      xml.Name `xml:"ListPartsResult"`
	XMLNS        string   `xml:"xmlns,attr"`
	Bucket       string   `xml:"Bucket"`
	Key          string   `xml:"Key"`
	UploadId     string   `xml:"UploadId"`
	StorageClass string   `xml:"StorageClass"`
	IsTruncated  bool     `xml:"IsTruncated"`
	Parts        []xPart  `xml:"Part"`
}
type xPart struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type xListMPU struct {
	XMLName xml.Name    `xml:"ListMultipartUploadsResult"`
	XMLNS   string      `xml:"xmlns,attr"`
	Bucket  string      `xml:"Bucket"`
	Uploads []xMPUEntry `xml:"Upload"`
}
type xMPUEntry struct {
	Key       string `xml:"Key"`
	UploadId  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

// ---- DeleteObjects (batch) ----

type xDeleteReq struct {
	XMLName xml.Name    `xml:"Delete"`
	Quiet   bool        `xml:"Quiet"`
	Objects []xToDelete `xml:"Object"`
}
type xToDelete struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId"`
}
type xDeleteResult struct {
	XMLName xml.Name    `xml:"DeleteResult"`
	XMLNS   string      `xml:"xmlns,attr"`
	Deleted []xDeleted  `xml:"Deleted"`
	Errors  []xDelError `xml:"Error"`
}
type xDeleted struct {
	Key                   string `xml:"Key"`
	VersionId             string `xml:"VersionId,omitempty"`
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionId string `xml:"DeleteMarkerVersionId,omitempty"`
}
type xDelError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// ---- ACL ----

type xACL struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	XMLNS   string   `xml:"xmlns,attr"`
	Owner   xOwner   `xml:"Owner"`
	Grants  []xGrant `xml:"AccessControlList>Grant"`
}
type xGrant struct {
	Grantee xGrantee `xml:"Grantee"`
	Perm    string   `xml:"Permission"`
}
type xGrantee struct {
	XMLNSXsi    string `xml:"xmlns:xsi,attr,omitempty"`
	Type        string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
}

func renderACL(acl *types.ACL) *xACL {
	out := &xACL{XMLNS: s3ns, Owner: xOwner{ID: acl.Owner.ID, DisplayName: acl.Owner.DisplayName}}
	for _, g := range acl.Grants {
		out.Grants = append(out.Grants, xGrant{
			Grantee: xGrantee{
				XMLNSXsi: xsiNS, Type: g.Grantee.Type, ID: g.Grantee.ID,
				DisplayName: g.Grantee.DisplayName, URI: g.Grantee.URI,
			},
			Perm: g.Permission,
		})
	}
	return out
}

func parseACL(b []byte) (*types.ACL, error) {
	var x xACL
	if err := xml.Unmarshal(b, &x); err != nil {
		return nil, err
	}
	acl := &types.ACL{Owner: types.Owner{ID: x.Owner.ID, DisplayName: x.Owner.DisplayName}}
	for _, g := range x.Grants {
		acl.Grants = append(acl.Grants, types.Grant{
			Grantee:    types.Grantee{Type: g.Grantee.Type, ID: g.Grantee.ID, DisplayName: g.Grantee.DisplayName, URI: g.Grantee.URI},
			Permission: g.Perm,
		})
	}
	return acl, nil
}

// ---- Tagging ----

type xTagging struct {
	XMLName xml.Name `xml:"Tagging"`
	XMLNS   string   `xml:"xmlns,attr"`
	Tags    []xTag   `xml:"TagSet>Tag"`
}
type xTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

func renderTagging(tags map[string]string) *xTagging {
	out := &xTagging{XMLNS: s3ns}
	for k, v := range tags {
		out.Tags = append(out.Tags, xTag{Key: k, Value: v})
	}
	return out
}
func parseTagging(b []byte) (map[string]string, error) {
	var x xTagging
	if err := xml.Unmarshal(b, &x); err != nil {
		return nil, err
	}
	m := map[string]string{}
	for _, t := range x.Tags {
		m[t.Key] = t.Value
	}
	return m, nil
}

// ---- Versioning ----

type xVersioning struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	XMLNS   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status,omitempty"`
}

// ---- CORS ----

type xCORS struct {
	XMLName xml.Name    `xml:"CORSConfiguration"`
	XMLNS   string      `xml:"xmlns,attr"`
	Rules   []xCORSRule `xml:"CORSRule"`
}
type xCORSRule struct {
	ID             string   `xml:"ID,omitempty"`
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedHeaders []string `xml:"AllowedHeader,omitempty"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty"`
	MaxAgeSeconds  int      `xml:"MaxAgeSeconds,omitempty"`
}

func renderCORS(rules []types.CORSRule) *xCORS {
	out := &xCORS{XMLNS: s3ns}
	for _, r := range rules {
		out.Rules = append(out.Rules, xCORSRule{
			ID: r.ID, AllowedOrigins: r.AllowedOrigins, AllowedMethods: r.AllowedMethods,
			AllowedHeaders: r.AllowedHeaders, ExposeHeaders: r.ExposeHeaders, MaxAgeSeconds: r.MaxAgeSeconds,
		})
	}
	return out
}
func parseCORS(b []byte) ([]types.CORSRule, error) {
	var x xCORS
	if err := xml.Unmarshal(b, &x); err != nil {
		return nil, err
	}
	var out []types.CORSRule
	for _, r := range x.Rules {
		out = append(out, types.CORSRule{
			ID: r.ID, AllowedOrigins: r.AllowedOrigins, AllowedMethods: r.AllowedMethods,
			AllowedHeaders: r.AllowedHeaders, ExposeHeaders: r.ExposeHeaders, MaxAgeSeconds: r.MaxAgeSeconds,
		})
	}
	return out, nil
}

// ---- Object lock / retention / legal hold ----

type xObjectLockCfg struct {
	XMLName           xml.Name   `xml:"ObjectLockConfiguration"`
	XMLNS             string     `xml:"xmlns,attr"`
	ObjectLockEnabled string     `xml:"ObjectLockEnabled,omitempty"`
	Rule              *xLockRule `xml:"Rule,omitempty"`
}
type xLockRule struct {
	DefaultRetention xDefaultRetention `xml:"DefaultRetention"`
}
type xDefaultRetention struct {
	Mode  string `xml:"Mode"`
	Days  int    `xml:"Days,omitempty"`
	Years int    `xml:"Years,omitempty"`
}
type xRetention struct {
	XMLName         xml.Name `xml:"Retention"`
	XMLNS           string   `xml:"xmlns,attr"`
	Mode            string   `xml:"Mode"`
	RetainUntilDate string   `xml:"RetainUntilDate"`
}
type xLegalHold struct {
	XMLName xml.Name `xml:"LegalHold"`
	XMLNS   string   `xml:"xmlns,attr"`
	Status  string   `xml:"Status"`
}

// ---- Ownership controls ----

type xOwnership struct {
	XMLName xml.Name         `xml:"OwnershipControls"`
	XMLNS   string           `xml:"xmlns,attr"`
	Rules   []xOwnershipRule `xml:"Rule"`
}
type xOwnershipRule struct {
	ObjectOwnership string `xml:"ObjectOwnership"`
}

// ---- Location ----

type xLocation struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	XMLNS    string   `xml:"xmlns,attr"`
	Location string   `xml:",chardata"`
}

// ---- Object attributes ----

type xObjectAttributes struct {
	XMLName      xml.Name `xml:"GetObjectAttributesResponse"`
	XMLNS        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag,omitempty"`
	ObjectSize   int64    `xml:"ObjectSize,omitempty"`
	StorageClass string   `xml:"StorageClass,omitempty"`
}

// ---- CreateBucketConfiguration (parsed, location ignored) ----

type xCreateBucketConfig struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

// ---- helpers ----

func maxKeysOr(n int) int {
	if n <= 0 || n > 1000 {
		return 1000
	}
	return n
}
func storageClassOr(s string) string {
	if s == "" {
		return "STANDARD"
	}
	return s
}
func versionOrNull(v string) string {
	if v == "" {
		return "null"
	}
	return v
}
func parseAmzTime(s string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, iso8601} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
