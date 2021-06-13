package frontend

type FieldType uint64

const (
	FieldTypeBoolean FieldType = iota
	FieldTypeInteger
	FieldTypeString
	FieldTypeFloat
	FieldTypeNull
)

type Value struct{}
