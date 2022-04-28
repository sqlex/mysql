package main

import "github.com/pingcap/tidb/parser/mysql"

// JavaSQLType is the sql type defined in class java.sql.Types in java sdk
type JavaSQLType int32

// jdk 1.8
const (
	JavaSQLTypeBIT           JavaSQLType = -7
	JavaSQLTypeTINYINT       JavaSQLType = -6
	JavaSQLTypeSMALLINT      JavaSQLType = 5
	JavaSQLTypeINTEGER       JavaSQLType = 4
	JavaSQLTypeBIGINT        JavaSQLType = -5
	JavaSQLTypeREAL          JavaSQLType = 7
	JavaSQLTypeDOUBLE        JavaSQLType = 8
	JavaSQLTypeDECIMAL       JavaSQLType = 3
	JavaSQLTypeCHAR          JavaSQLType = 1
	JavaSQLTypeVARCHAR       JavaSQLType = 12
	JavaSQLTypeDATE          JavaSQLType = 91
	JavaSQLTypeTIME          JavaSQLType = 92
	JavaSQLTypeTIMESTAMP     JavaSQLType = 93
	JavaSQLTypeBINARY        JavaSQLType = -2
	JavaSQLTypeVARBINARY     JavaSQLType = -3
	JavaSQLTypeLONGVARBINARY JavaSQLType = -4
	JavaSQLTypeNULL          JavaSQLType = 0
	JavaSQLTypeBLOB          JavaSQLType = 2004
	JavaSQLTypeCLOB          JavaSQLType = 2005
)

func mySQLType2JavaType(mysqlType byte, isBinary bool) JavaSQLType {
	switch mysqlType {
	case mysql.TypeTiny:
		return JavaSQLTypeTINYINT

	case mysql.TypeShort:
		return JavaSQLTypeSMALLINT

	case mysql.TypeLong:
		return JavaSQLTypeINTEGER

	case mysql.TypeFloat:
		return JavaSQLTypeREAL

	case mysql.TypeDouble:
		return JavaSQLTypeDOUBLE

	case mysql.TypeNull:
		return JavaSQLTypeNULL

	case mysql.TypeNewDecimal:
		return JavaSQLTypeDECIMAL

	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return JavaSQLTypeTIMESTAMP

	case mysql.TypeLonglong:
		return JavaSQLTypeBIGINT

	case mysql.TypeInt24:
		return JavaSQLTypeINTEGER

	case mysql.TypeDate, mysql.TypeNewDate:
		return JavaSQLTypeDATE

	case mysql.TypeDuration:
		return JavaSQLTypeTIME

	case mysql.TypeYear:
		return JavaSQLTypeVARCHAR

	case mysql.TypeEnum:
		return JavaSQLTypeINTEGER

	case mysql.TypeSet:
		return JavaSQLTypeBIT

	// Blob related is not identical to the official implementation, since we do not know `meta` at the moment.
	// see https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L222-L231
	// But this does not matter, they will be `JavaSQLTypeBlob` or `JavaSQLTypeClob` finally.
	case mysql.TypeTinyBlob:
		return JavaSQLTypeVARBINARY

	case mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		return JavaSQLTypeLONGVARBINARY

	case mysql.TypeVarString, mysql.TypeVarchar:
		if isBinary {
			return JavaSQLTypeVARBINARY
		}
		return JavaSQLTypeVARCHAR

	case mysql.TypeString:
		if isBinary {
			return JavaSQLTypeBINARY
		}
		return JavaSQLTypeCHAR

	case mysql.TypeGeometry:
		return JavaSQLTypeBINARY

	case mysql.TypeBit:
		return JavaSQLTypeBIT

	case mysql.TypeJSON:
		return JavaSQLTypeVARCHAR

	default:
		return JavaSQLTypeVARCHAR
	}
}
