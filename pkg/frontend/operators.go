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

var (
	itemTypeToOperator map[itemType]Operator = map[itemType]Operator{
		itemEqual:              OperatorEqual,
		itemGreaterThan:        OperatorGreaterThan,
		itemLessThan:           OperatorLessThan,
		itemPlus:               OperatorPlus,
		itemMinus:              OperatorMinus,
		itemAsterisk:           OperatorAsterisk,
		itemSlash:              OperatorSlash,
		itemCaret:              OperatorCaret,
		itemPercent:            OperatorPercent,
		itemExclamation:        OperatorExclamation,
		itemQuestionMark:       OperatorQuestionMark,
		itemNotEqual:           OperatorNotEqual,
		itemLessThanEqualTo:    OperatorLessThanEqualTo,
		itemGreaterThanEqualTo: OperatorGreaterThanEqualTo,
		itemAndAnd:             OperatorAndAnd,
		itemOrOr:               OperatorOrOr,
	}
)
