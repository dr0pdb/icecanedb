package frontend

// DDLStatement is a data definition query.
// It could be Create/Drop of tables
type CreateTableStatement struct {
	Spec *TableSpec
}

func (cts *CreateTableStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (cts *CreateTableStatement) statement() {}

type DropTableStatement struct {
	TableName string
}

func (dts *DropTableStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (dts *DropTableStatement) statement() {}

type TruncateTableStatement struct {
	TableName string
}

func (tts *TruncateTableStatement) Accept(v Visitor) (node Node, ok bool) {
	panic("")
}

func (tts *TruncateTableStatement) statement() {}

// TableSpec defines the specification of a table
// TODO: add methods for getting column, primary key etc.
type TableSpec struct {
	TableId   uint64 // internal id of the table. unique
	TableName string
	Columns   []*ColumnSpec
}

// ColumnSpec defines a single column of a table
type ColumnSpec struct {
	Name       string
	Type       FieldType
	Nullable   bool
	PrimaryKey bool
	Unique     bool
	Index      bool
	References string // the foreign key reference
	Default    Expression
}
