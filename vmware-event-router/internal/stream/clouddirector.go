package stream

import (
	"context"
	"log"
	//"net/url"
	"net/http"
	"os"
	"encoding/json"

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

type CloudDirectorConnectionConfig struct {
	User     string
	Password string
	Org      string
	Href     string
	VDC      string
	Insecure bool
}

// NewCloudDirectorStream returns a Cloud Director event manager for a given configuration
func NewCloudDirectorStream(ctx context.Context, cfg connection.Config, opts ...CloudDirectorOption) (Streamer, error) {
	var cloudDirector cloudDirectorStream
	logger := log.New(os.Stdout, color.Magenta("[CloudDirector] "), log.LstdFlags)
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

	var insecure bool
	if cfg.Options["insecure"] == "true" {
		insecure = true
	}
	
	// TODO implement VCD api calls to get audit events
	cloudDirectorConnectionConfig := CloudDirectorConnectionConfig{
		User: username,
		Password: password,
		Org : "T3",
		Href: cloudDirectorUrl,
		Insecure: insecure,
	}

	request, err := http.NewRequest("GET", cloudDirectorConnectionConfig.Href + "/cloudapi/1.0.0/auditTrail", nil)
	request.Header.Set("Accept", "application/json;version=34.0")
	request.Header.Set("Bearer", "blah")
	audit, err := http.DefaultClient.Do(request)
	if err != nil {
		errors.Wrap(err, "Error getting auditTrail")
	}

	var result map[string]interface{}
	json.NewDecoder(audit.Body).Decode(&result)
	totalEvents := result["resultTotal"]
	cloudDirector.Logger.Println("Total Events: %d", totalEvents)


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
