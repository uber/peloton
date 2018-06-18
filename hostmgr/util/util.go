package util

import "strings"

// LabelKeyToEnvVarName converts a task label key to an env var name
// Example: label key 'peloton.job_id' converted to env var name 'PELOTON_JOB_ID'
func LabelKeyToEnvVarName(labelKey string) string {
	return strings.ToUpper(strings.Replace(labelKey, ".", "_", 1))
}
