package stream

import (
	"context"
	"log"
	"net/http"
	"os"
	"encoding/json"
	"crypto/tls"

	"github.com/pkg/errors"
	"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/color"
	"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/connection"
	//"github.com/vmware-samples/vcenter-event-broker-appliance/vmware-event-router/internal/events"
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
	*log.Logger
	verbose bool
}


// NewCloudDirectorStream returns a Cloud Director event manager for a given configuration
func NewCloudDirectorStream(ctx context.Context, cfg connection.Config, opts ...CloudDirectorOption) (Streamer, error) {
	var cloudDirector cloudDirectorStream
	logger := log.New(os.Stdout, color.Magenta("[VMware Cloud Director] "), log.LstdFlags)
	cloudDirector.Logger = logger
	cloudDirectorUrl := cfg.Address
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
	authRequest, err := http.NewRequest("POST",cloudDirectorUrl + "/api/sessions", nil)
	authRequest.Header.Set("Accept", "application/*+xml;version=34.0")
	authRequest.SetBasicAuth(username, password)
	cloudDirector.Logger.Println("Authenticating to Cloud Director", cloudDirectorUrl)
	authResponse, err := http.DefaultClient.Do(authRequest)
	if err != nil {
		errors.Wrap(err, "Error authenticating ")
	}	
	if authResponse.StatusCode != http.StatusOK {
		errors.Errorf("Error authenticating: %s", authResponse.Status)
	}
	cloudDirector.Logger.Println("Authentication Response: " + authResponse.Status)
	authToken := authResponse.Header.Get("X-VMWARE-VCLOUD-ACCESS-TOKEN")
	var bearer = "Bearer " + authToken

	// TODO create a loop to poll events (time based, with filter?)
	// TODO check bearer token expiration
	cloudDirector.Logger.Println("Get Cloud Director events...")
	request, err := http.NewRequest("GET", cloudDirectorUrl + "/cloudapi/1.0.0/auditTrail", nil)
	request.Header.Set("Accept", "application/json;version=34.0")
	request.Header.Set("Authorization", bearer)
	audit, err := http.DefaultClient.Do(request)
	if err != nil {
		errors.Wrap(err, "Error getting auditTrail")
	}
	defer audit.Body.Close()
	cloudDirector.Logger.Println("Response status: ", audit.Status)

	var result map[string]interface{}
	json.NewDecoder(audit.Body).Decode(&result)
	//cloudDirector.Logger.Println(result)
	totalEvents := result["resultTotal"]
	cloudDirector.Logger.Println("Total Events: ", totalEvents)


	return &cloudDirector, nil
}

func (cloudDirector *cloudDirectorStream) Stream(ctx context.Context, p processor.Processor) error {
	return nil
}

func (cloudDirector *cloudDirectorStream) Shutdown(ctx context.Context) error {
	return nil
}

func (cloudDirector *cloudDirectorStream) PushMetrics(ctx context.Context, ms metrics.Receiver) {
}
