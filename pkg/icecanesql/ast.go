package icecanesql

// Statement denotes a parsed SQL statement
type Statement interface {
}

var _ Statement = (*CreateTableStatement)(nil)
var _ Statement = (*DropTableStatement)(nil)
var _ Statement = (*BeginTxnStatement)(nil)
var _ Statement = (*FinishTxnStatement)(nil)

// DDLStatement is a data definition query.
// It could be Create/Drop of tables
type CreateTableStatement struct {
	Spec *TableSpec
}

type DropTableStatement struct {
	TableName string
}

type BeginTxnStatement struct{}

// FinishTxnStatement denotes a COMMIT/ROLLBACK statement
type FinishTxnStatement struct {
	IsCommit bool
}

// TableSpec defines the specification of a table
type TableSpec struct {
	TableName string
	Columns   []*ColumnSpec
}

// ColumnSpec defines a single column of a table
// TODO: consider supporting defaults after supporting values
type ColumnSpec struct {
	Name       string
	Type       ColumnType
	Nullable   bool
	PrimaryKey bool
	Unique     bool
	Index      bool
	References string // the foreign key reference
}

type ColumnType int

const (
	ColumnTypeBoolean ColumnType = iota
	ColumnTypeInteger
	ColumnTypeString
	ColumnTypeFloat
)
