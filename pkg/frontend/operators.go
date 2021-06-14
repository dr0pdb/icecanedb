package frontend

type Operator uint64

const (
	OperatorEqual              Operator = iota // '='
	OperatorGreaterThan                        // '>'
	OperatorLessThan                           // '<'
	OperatorPlus                               // '+'
	OperatorMinus                              // '-'
	OperatorAsterisk                           // '*'
	OperatorSlash                              // '/'
	OperatorCaret                              // '^'
	OperatorPercent                            // '%'
	OperatorExclamation                        // '!'
	OperatorQuestionMark                       // '?'
	OperatorNotEqual                           // "!="
	OperatorLessThanEqualTo                    // "<="
	OperatorGreaterThanEqualTo                 // ">="
	OperatorAndAnd                             // "&&"
	OperatorOrOr                               // "||"
)
