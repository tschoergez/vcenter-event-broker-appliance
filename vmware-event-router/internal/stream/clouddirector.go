package stream

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

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
	verbose  bool
	pageSize int
	interval int
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
	cloudDirector.pageSize = 10
	cloudDirector.interval = 30
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
	authRequest.Header.Set("Accept", "application/*+xml;version=33.0")
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

// Stream polls the CloudDirector auditTrail API endlessly
func (cloudDirector *cloudDirectorStream) Stream(ctx context.Context, p processor.Processor) error {
	loc, _ := time.LoadLocation("UTC")
	end := time.Now().In(loc)
	start := end.Add(time.Second * time.Duration(-cloudDirector.interval))
	for {
		page := 1
		pageCount, err := cloudDirector.getAuditTrail(start, end, page, p)
		if err != nil {
			cloudDirector.Logger.Printf(err.Error())
		}
		page++
		for ; page <= pageCount; page++ {
			_, err := cloudDirector.getAuditTrail(start, end, page, p)
			if err != nil {
				cloudDirector.Logger.Printf(err.Error())
			}
		}
		// set boundaries for next poll
		start = end
		time.Sleep(time.Second * time.Duration(cloudDirector.interval))
		end = time.Now().In(loc)
	}
}

// fetches auditTrail events from Cloud Director filtered by start and end time and a given page number
// processes them for the given processor
// returns the pageCount of response
func (cloudDirector *cloudDirectorStream) getAuditTrail(start time.Time, end time.Time, page int, p processor.Processor) (int, error) {
	timestampFormat := "2006-01-02T15:04:05.000Z"
	startString := start.Format(timestampFormat)
	endString := end.Format(timestampFormat)

	timeFilter := "(timestamp=gt=" + startString + ";timestamp=le=" + endString + ")"
	cloudDirector.Logger.Printf("Get Cloud Director events from %v to %v", startString, endString)

	request, err := http.NewRequest("GET", cloudDirector.cloudDirectorURL+"/cloudapi/1.0.0/auditTrail", nil)
	request.Header.Set("Accept", "application/json;version=33.0")
	request.Header.Set("Authorization", cloudDirector.bearer)
	q := url.Values{}
	q.Add("page", strconv.Itoa(page))
	q.Add("pageSize", strconv.Itoa(cloudDirector.pageSize))
	q.Add("sortASC", "timestamp")
	q.Add("filter", timeFilter)
	request.URL.RawQuery = q.Encode()
	cloudDirector.Logger.Println(request.URL.String())

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		errors.Wrap(err, "Error getting auditTrail")
	}
	cloudDirector.Logger.Println("Response status: ", response.Status)
	statusOK := response.StatusCode >= 200 && response.StatusCode < 300
	if !statusOK {
		err = errors.Errorf("Non-OK HTTP status: %v", response.StatusCode)
		return 0, err
	}

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
	pageCount := auditTrail.PageCount
	cloudDirector.Logger.Println("Total Events: ", totalEvents)
	cloudDirector.Logger.Println("PageCount: ", pageCount)

	if totalEvents <= 0 {
		return 0, nil
	}

	// loop through events
	for idx := range auditTrail.Values {
		var event = auditTrail.Values[idx]
		cloudDirector.Logger.Println(event)
		ce, err := events.NewCloudEventFromCloudDirector(event, cloudDirector.cloudDirectorURL)
		if err != nil {
			cloudDirector.Logger.Printf("skipping event %v because it coud not be converted to CloudEvent: %v", event, err)
		}

		err = p.Process(*ce)
		if err != nil {
			cloudDirector.Logger.Printf("could not proccess event %v: %v", ce, err)
		}
	}
	return pageCount, nil
}

func (cloudDirector *cloudDirectorStream) Shutdown(ctx context.Context) error {
	return nil
}

func (cloudDirector *cloudDirectorStream) PushMetrics(ctx context.Context, ms metrics.Receiver) {
}
