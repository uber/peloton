package querybuilder

// StatementAccessor provides an interface to access statement internals
type StatementAccessor interface {
	// GetWhereParts gets the where clause of this statement
	GetWhereParts() []Sqlizer
	// GetResource gets the resource type (that is, the table) on which this statement operates
	GetResource() string
	// GetColumns returns the selected columns and is applicable for select stmt only. Other types
	// of statements will return nil
	GetColumns() []Sqlizer
}
