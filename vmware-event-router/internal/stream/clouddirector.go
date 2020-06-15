package stream

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/color"
	"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/connection"
	"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/events"

	"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/metrics"
	"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/processor"
)

const (
	//ProviderCloudDirector is the name used to identify this provider in the
	// VMware Event Router configuration file
	ProviderCloudDirector   = "vmware_clouddirector"
	authMethodCloudDirector = "user_password"
)

// cloudDirectorStream handles the connection to cloudDirectorStream to retrieve and event stream
type cloudDirectorStream struct {
	bearer           string
	cloudDirectorURL string
	*log.Logger
	verbose bool
}

type auditTrail struct {
	ResultTotal  int         `json:"resultTotal"`
	PageCount    int         `json:"pageCount"`
	Page         int         `json:"page"`
	PageSize     int         `json:"pageSize"`
	Associations interface{} `json:"associations"`
	Values       []events.CloudDirectorEvent
}

// NewCloudDirectorStream returns a Cloud Director Connection for a given configuration
// It connects to Cloud Director to authenticate, and stores the Bearer token for subsequent calls
func NewCloudDirectorStream(ctx context.Context, cfg connection.Config, opts ...CloudDirectorOption) (Streamer, error) {
	var cloudDirector cloudDirectorStream
	logger := log.New(os.Stdout, color.Magenta("[VMware Cloud Director] "), log.LstdFlags)
	cloudDirector.Logger = logger
	cloudDirector.cloudDirectorURL = cfg.Address
	// apply options
	for _, opt := range opts {
		opt(&cloudDirector)
	}

	var username, password string
	switch cfg.Auth.Method {
	case authMethodCloudDirector:
		username = cfg.Auth.Secret["username"]
		password = cfg.Auth.Secret["password"]
	default:
		return nil, errors.Errorf("unsupported authentication method for stream CloudDirector: %s", cfg.Auth.Method)
	}

	if cfg.Options["insecure"] == "true" {
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Authenticating to Cloud Director
	authRequest, err := http.NewRequest("POST", cloudDirector.cloudDirectorURL+"/api/sessions", nil)
	authRequest.Header.Set("Accept", "application/*+xml;version=34.0")
	authRequest.SetBasicAuth(username, password)
	cloudDirector.Logger.Println("Authenticating to Cloud Director", cloudDirector.cloudDirectorURL)
	authResponse, err := http.DefaultClient.Do(authRequest)
	if err != nil {
		errors.Wrap(err, "Error authenticating ")
	}
	if authResponse.StatusCode != http.StatusOK {
		errors.Errorf("Error authenticating: %s", authResponse.Status)
	}
	cloudDirector.Logger.Println("Authentication Response: " + authResponse.Status)
	authToken := authResponse.Header.Get("X-VMWARE-VCLOUD-ACCESS-TOKEN")
	cloudDirector.bearer = "Bearer " + authToken
	cloudDirector.Logger.Println("Authentication to Cloud Director " + cloudDirector.cloudDirectorURL + " successful")

	return &cloudDirector, nil
}

func (cloudDirector *cloudDirectorStream) Stream(ctx context.Context, p processor.Processor) error {
	cloudDirector.Logger.Println("Get Cloud Director events...")
	request, err := http.NewRequest("GET", cloudDirector.cloudDirectorURL+"/cloudapi/1.0.0/auditTrail", nil)
	request.Header.Set("Accept", "application/json;version=34.0")
	request.Header.Set("Authorization", cloudDirector.bearer)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		errors.Wrap(err, "Error getting auditTrail")
	}
	cloudDirector.Logger.Println("Response status: ", response.Status)

	var auditTrail auditTrail
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		errors.Wrap(err, "Error reading response body")
	}
	err = json.Unmarshal(b, &auditTrail)
	if err != nil {
		errors.Wrap(err, "Error parsing response body")
	}
	response.Body.Close()
	totalEvents := auditTrail.ResultTotal
	cloudDirector.Logger.Println("Total Events: ", totalEvents)
	var event = auditTrail.Values[0]
	cloudDirector.Logger.Println(event)
	ce, err := events.NewCloudEventFromCloudDirector(event, cloudDirector.cloudDirectorURL)
	if err != nil {
		cloudDirector.Logger.Printf("skipping event %v because it coud not be converted to CloudEvent: %v", event, err)
	}

	err = p.Process(*ce)
	if err != nil {
		cloudDirector.Logger.Printf("could not proccess event %v: %v", ce, err)
	}
	return nil
}

func (cloudDirector *cloudDirectorStream) Shutdown(ctx context.Context) error {
	return nil
}

func (cloudDirector *cloudDirectorStream) PushMetrics(ctx context.Context, ms metrics.Receiver) {
}
