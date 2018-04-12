package avro

import (
	"reflect"
	"strings"
)

func CreateSchema(obj interface{}) (string, error) {
	return CreateSchemaFromValue(reflect.ValueOf(obj))
}

func CreateSchemaFromValue(t reflect.Value) (string, error) {
	//for i := 0; i < t.NumField(); i++ {
	//	field := t.Field(i)
	//	t := field.Tag.Get(AvroTag)
	//	split := strings.Split(t, ",")
	//}
	// prefix := getVariadicString(pkgPrefix...)
	return "", nil
}

func createSchema(field *structField, t reflect.Value, pkgPrefix string) (*record, error) {
	if field.ignored {
		return nil, errIgnored
	}

	kindMatch, typeMatch, ret := primitives.Match(getValue(t))

	if ret != nil {
		ret.Name = field.name
	} else {
		ret = &record{}
	}

	// Matches a primitive type, there is a predefined record for it, returning.
	if kindMatch && typeMatch {
		return ret.apply(field), nil
	}

	// This scenario is when a type was created from a primitive type. Example:  type Status string.
	// We handle these scenarios as if they were enums.
	if kindMatch {
		logicalType := t.Type().Name()
		class := logicalType

		if pkgPrefix != "" {
			class = strings.Join([]string{pkgPrefix, class}, ".")
		}
		r := &record{Type: ret.Type, JavaClass: class, LogicalType: logicalType}
		ret.Type = r
		return ret.apply(field), nil
	}

	return nil, nil
}

func getVariadicString(str ...string) string {
	if len(str) > 0 {
		return str[0]
	}
	return ""
}
