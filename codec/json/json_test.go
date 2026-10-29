package json

import (
	"reflect"
	"testing"
)

type sample struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	Raw  []byte `json:"raw"`
}

func TestRoundTrip(t *testing.T) {
	want := sample{Name: "alice", Age: 42, Raw: []byte{1, 2}}
	b, err := Encoding{}.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got sample
	if err := (Encoding{}).Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if ct := (Encoding{}).ContentType(); ct != "application/json" {
		t.Errorf("ContentType = %q", ct)
	}
}

func TestUnmarshalInvalid(t *testing.T) {
	var v sample
	if err := (Encoding{}).Unmarshal([]byte("not-json"), &v); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// mqttv5.Encoding requires Unmarshal to keep nothing of data: on a
// zero-copy subscription data is the network frame, reused after the ack.
func TestUnmarshalKeepsNothingOfData(t *testing.T) {
	data := []byte(`{"name":"alice","raw":"AQI="}`)
	var v sample
	if err := (Encoding{}).Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] = 'x'
	}
	if v.Name != "alice" || !reflect.DeepEqual(v.Raw, []byte{1, 2}) {
		t.Errorf("decoded value changed with its input: %+v", v)
	}
}
