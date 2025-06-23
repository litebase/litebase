package components

type FormField struct {
	CharLimit   int
	Conditions  []Condition
	Name        string
	Label       string
	Placeholder string
	Required    bool
	Type        InputType
	Options     map[string]string
}

type Condition struct {
	FieldName string
	Operator  string
	Value     any
}
