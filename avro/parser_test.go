package avro

import (
	"testing"
	"time"
	"reflect"
	"github.com/stretchr/testify/assert"
	"strings"
	"fmt"
)

type KafkaMessage struct {
	Name    string `avro:"name,nullable"`
	Number  int    `avro:"number"`
	Ignored string `avro:"-"`
	Long    int64  `avro:"long,stringable"`
}

type testStr string

func TestCreateSchema(t *testing.T) {
	println(getObjKey(time.Time{}))
	_, _, a := primitives.Match(reflect.ValueOf("abcd"))
	_, _, b := primitives.Match(reflect.ValueOf("abcd"))

	assert.Equal(t, a, b)
	b.LogicalType = "1234567"

	assert.NotNil(t, a, b)

	x := strings.Split(",test,test", ",")
	fmt.Println(x)
	assert.Equal(t, 3, len(x))
	assert.Equal(t, "", x[0])
}
