package polarisdb

import (
	"fmt"
	"testing"
)

func TestLog_Open(t *testing.T) {
	l := Log{}
	err := l.Open("tmp")
	defer cleanTestData()

	if err != nil {
		t.Fatal(err)
	}
}

func TestLog_Append(t *testing.T) {
	l := Log{}
	err := l.Open("tmp")
	defer cleanTestData()

	if err != nil {
		t.Fatal(err)
	}
	err = l.Append(&Block{Data: []byte("Hello")})
	if err != nil {
		t.Fatal(err)
	}

}

func TestLog_Append2(t *testing.T) {
	l := Log{
		maxSegSize: 1024, // 1kb
	}
	err := l.Open("tmp")
	defer cleanTestData()

	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		err = l.Append(&Block{Data: []byte("Hello")})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLogIterator_Next(t *testing.T) {
	l := Log{
		maxSegSize: 1024,
	}
	err := l.Open("./tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanTestData()
	for i := 0; i < 100; i++ {
		err = l.Append(&Block{Data: []byte(fmt.Sprintf("Hello %d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}
	iter, err := l.NewLogIterator()
	if err != nil {
		t.Fatal(err)
	}
	for {
		block := iter.Next()
		if block == nil {
			break
		}
	}
}
