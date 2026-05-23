package pb

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestProtoBinary_RoundTrip(t *testing.T) {
	origin := &EthTransaction{
		Hash:        "0xabc",
		From:        "0xfrom",
		To:          "0xto",
		Value:       "123",
		BlockNumber: 100,
		Gas:         21000,
		Nonce:       7,
		Success:     true,
	}

	b, err := proto.Marshal(origin)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	var decoded EthTransaction
	if err := proto.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}

	if !proto.Equal(origin, &decoded) {
		t.Fatalf("round-trip mismatch:\norigin=%v\ndecoded=%v", origin, &decoded)
	}
}

func TestProtoJSON_RoundTrip(t *testing.T) {
	origin := &EthTransaction{
		Hash:        "0xdef",
		From:        "0xfrom2",
		To:          "0xto2",
		Value:       "999",
		BlockNumber: 101,
		Gas:         30000,
		Nonce:       8,
		Success:     false,
	}

	opts := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}

	j, err := opts.Marshal(origin)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	var decoded EthTransaction
	if err := protojson.Unmarshal(j, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}

	if !proto.Equal(origin, &decoded) {
		t.Fatalf("json round-trip mismatch:\norigin=%v\ndecoded=%v\njson=%s", origin, &decoded, string(j))
	}
}

