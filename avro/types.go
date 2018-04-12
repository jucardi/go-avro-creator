package avro

import (
	"reflect"
	"fmt"
	"time"
	"strings"
	"errors"
)

var (
	primitives primitiveMapping
	composites compositeMapping
	errIgnored = errors.New("ignored")
)

const (
	AvroTag        = "avro"
	ignoreFlag     = "-"
	nullableFlag   = "nullable"
	stringableFlag = "stringable"
)

type (
	// Char: explicitly represents a character. In golang there is no way to differentiate 'rune' from 'in32'. This type is defined as a helper to easily define a 'char'
	// and will be serialized as such when creating an avro schema from a struct.
	Char rune

	// Byte: explicitly represents a byte. In golang there is no way to differentiate 'byte' from 'uint8'. This type is defined as a helper to easily define a 'byte' and will
	// be serialized as such when creating an avro schema from a struct.
	Byte byte

	structField struct {
		nullable   bool
		stringable bool
		ignored    bool
		name       string
	}

	// primitiveMapping: Defines Kind to Java primitives mapping.
	primitiveMapping map[reflect.Kind]*record

	// compositeMapping: Defines a mapping to Java primitives by using a composite key of Kind and Type.
	compositeMapping map[string]*record

	// record: Represents a record entry in an avro schema.
	record struct {
		Name        string      `json:"name,omitempty"`
		Type        interface{} `json:"type,omitempty"` // Indicates the type of the record, could be a string, object or array.
		Namespace   string      `json:"namespace,omitempty"`
		Default     interface{} `json:"default,omitempty"`
		JavaClass   string      `json:"java-class,omitempty"`
		LogicalType string      `json:"logicalType,omitempty"`
		Fields      []*record   `json:"fields"`
		Items       interface{} `json:"items"`
	}
)

func init() {
	primitives = primitiveMapping{
		reflect.String:  {Type: "string"},
		reflect.Bool:    {Type: "boolean"},
		reflect.Int:     {Type: "int"},
		reflect.Uint:    {Type: "int"},
		reflect.Int32:   {Type: "int"},
		reflect.Uint32:  {Type: "int"},
		reflect.Int64:   {Type: "long"},
		reflect.Uint64:  {Type: "long"},
		reflect.Float32: {Type: "float"},
		reflect.Float64: {Type: "double"},
		reflect.Int8:    {Type: &record{Type: "int", JavaClass: "java.lang.Short"}},
		reflect.Int16:   {Type: &record{Type: "int", JavaClass: "java.lang.Short"}},
		reflect.Uint8:   {Type: &record{Type: "int", JavaClass: "java.lang.Short"}},
		reflect.Uint16:  {Type: &record{Type: "int", JavaClass: "java.lang.Short"}},
	}

	composites = compositeMapping{
		getObjKey(Char(rune('a'))): {Type: &record{Type: "int", JavaClass: "java.lang.Character"}},
		getObjKey(Byte(byte(1))):   {Type: &record{Type: "int", JavaClass: "java.lang.Byte"}},
		getObjKey(time.Time{}):     {Type: &record{Type: "string", JavaClass: "java.time.Instant", LogicalType: "Instant"}},
	}
}

func (p primitiveMapping) Match(val reflect.Value) (kindMatch, typeMatch bool, ret *record) {
	ret, kindMatch = p[val.Kind()]
	if kindMatch {
		typeMatch = val.Kind().String() == val.Type().String()
	}
	return
}

//func (c compositeMapping) Match(val reflect.Value) *record {
//	key := getKey(obj)
//}

func (r *record) apply(field *structField) *record {
	r.Name = field.name
	if field.nullable {
		newType := []interface{}{"null", r.Type}
		r.Type = newType
	}
	return r
}

func getValue(v reflect.Value) (reflect.Value) {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

func getObjKey(obj interface{}) string {
	return getKey(reflect.ValueOf(obj))
}

func getKey(v reflect.Value) string {
	return fmt.Sprintf("{kind:%s, type:%s}", v.Kind().String(), v.Type().String())
}

func getFieldInfo(field reflect.StructField) *structField {
	flags := strings.Split(field.Tag.Get(AvroTag), ",")
	return &structField{
		ignored:    contains(flags, ignoreFlag),
		nullable:   contains(flags, nullableFlag),
		stringable: contains(flags, stringableFlag),
		name: func() string {
			if flags[0] != "" {
				return flags[0]
			}
			return field.Name
		}(),
	}
}

func contains(arr []string, val string) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

var basicTypes = []reflect.Kind{
	reflect.Invalid,
	reflect.Uintptr,
	reflect.Array,
	reflect.Chan,
	reflect.Func,
	reflect.Interface,
	reflect.Map,
	reflect.Ptr,
	reflect.Slice,
	reflect.String,
	reflect.Struct,
	reflect.UnsafePointer,
}
