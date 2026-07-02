// Package s3err renders S3-style error codes as the XML bodies AWS SDKs expect.
package s3err

import (
	"encoding/xml"
	"net/http"
)

// APIError is an S3 error with its HTTP status and wire code.
type APIError struct {
	Code       string // S3 error code, e.g. "NoSuchKey"
	Message    string
	HTTPStatus int
}

func (e APIError) Error() string { return e.Code + ": " + e.Message }

// errorResponse is the S3 XML error envelope.
type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

// XML marshals the error into an S3 error document.
func (e APIError) XML(resource, requestID string) []byte {
	body, _ := xml.Marshal(errorResponse{
		Code:      e.Code,
		Message:   e.Message,
		Resource:  resource,
		RequestID: requestID,
	})
	return append([]byte(xml.Header), body...)
}

// Common errors.
var (
	ErrNoSuchBucket        = APIError{"NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound}
	ErrNoSuchKey           = APIError{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}
	ErrNoSuchVersion       = APIError{"NoSuchVersion", "The specified version does not exist.", http.StatusNotFound}
	ErrNoSuchUpload        = APIError{"NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound}
	ErrNoSuchBucketPolicy  = APIError{"NoSuchBucketPolicy", "The bucket policy does not exist.", http.StatusNotFound}
	ErrNoSuchCORS          = APIError{"NoSuchCORSConfiguration", "The CORS configuration does not exist.", http.StatusNotFound}
	ErrNoSuchTagSet        = APIError{"NoSuchTagSet", "There is no tag set associated with this resource.", http.StatusNotFound}
	ErrNoSuchObjectLock    = APIError{"ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist for this bucket.", http.StatusNotFound}
	ErrBucketNotEmpty      = APIError{"BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict}
	ErrBucketAlreadyOwn    = APIError{"BucketAlreadyOwnedByYou", "The bucket already exists.", http.StatusConflict}
	ErrBucketAlreadyExists = APIError{"BucketAlreadyExists", "The requested bucket name is not available.", http.StatusConflict}
	ErrInvalidBucketName   = APIError{"InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest}
	ErrInvalidKey          = APIError{"InvalidArgument", "The specified key is not valid.", http.StatusBadRequest}
	ErrInvalidArgument     = APIError{"InvalidArgument", "Invalid Argument.", http.StatusBadRequest}
	ErrInvalidRange        = APIError{"InvalidRange", "The requested range is not satisfiable.", http.StatusRequestedRangeNotSatisfiable}
	ErrInvalidPart         = APIError{"InvalidPart", "One or more of the specified parts could not be found.", http.StatusBadRequest}
	ErrInvalidPartOrder    = APIError{"InvalidPartOrder", "The list of parts was not in ascending order.", http.StatusBadRequest}
	ErrEntityTooSmall      = APIError{"EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.", http.StatusBadRequest}
	ErrBadDigest           = APIError{"BadDigest", "The Content-MD5 you specified did not match what we received.", http.StatusBadRequest}
	ErrPreconditionFailed  = APIError{"PreconditionFailed", "At least one of the preconditions you specified did not hold.", http.StatusPreconditionFailed}
	ErrNotModified         = APIError{"NotModified", "Not Modified.", http.StatusNotModified}
	ErrMethodNotAllowed    = APIError{"MethodNotAllowed", "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed}
	ErrMalformedXML        = APIError{"MalformedXML", "The XML you provided was not well-formed or did not validate.", http.StatusBadRequest}
	ErrAccessDenied        = APIError{"AccessDenied", "Access Denied.", http.StatusForbidden}
	ErrSignatureMismatch   = APIError{"SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", http.StatusForbidden}
	ErrInvalidAccessKey    = APIError{"InvalidAccessKeyId", "The AWS access key Id you provided does not exist in our records.", http.StatusForbidden}
	ErrNotImplemented      = APIError{"NotImplemented", "This operation is not implemented.", http.StatusNotImplemented}
	ErrInternal            = APIError{"InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError}
	ErrNoLeader            = APIError{"ServiceUnavailable", "No storage log leader available.", http.StatusServiceUnavailable}
	ErrSlowDown            = APIError{"SlowDown", "Please reduce your request rate.", http.StatusServiceUnavailable}
)

// From maps an arbitrary error to an APIError (defaults to InternalError).
func From(err error) APIError {
	if err == nil {
		return APIError{}
	}
	if ae, ok := err.(APIError); ok {
		return ae
	}
	e := ErrInternal
	e.Message = err.Error()
	return e
}
