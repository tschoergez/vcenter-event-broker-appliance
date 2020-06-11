package stream


// CloudDirectorOption configures the CloudDirector stream provider
type CloudDirectorOption func(*cloudDirectorStream)

// WithCloudDirectorVerbose enables verbose logging for the AWS processor
func WithCloudDirectorVerbose(verbose bool) CloudDirectorOption {
	return func(vc *cloudDirectorStream) {
		vc.verbose = verbose
	}
}
