package strings

// ValidateString returns true if length of the string is greater than 0
func ValidateString(str string) bool {
	if len(str) > 0 {
		return true
	}
	return false
}
