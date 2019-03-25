package auth

// Type is the auth type used
type Type string

const (
	// UNDEFINED is the undefined type which would behave the same as NOOP
	UNDEFINED = Type("")
	// NOOP would effectively disable security feature
	NOOP = Type("NOOP")
	// BASIC would use username and password for auth
	BASIC = Type("BASIC")
)

// Token is used by SecurityManager to authenticate a user
type Token interface {
	Get(k string) (string, bool)
}

// SecurityManager includes authentication related methods
type SecurityManager interface {
	// Authenticate with the token provided,
	// return User if authentication succeeds
	Authenticate(token Token) (User, error)
}

// User includes authorization related methods
type User interface {
	// IsPermitted returns whether user can
	// access the specified procedure
	IsPermitted(procedure string) bool
}
