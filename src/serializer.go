package main

import (
	"bytes"
	"encoding/gob"
)

func Serialize(order *Order) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(order); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func Deserialize(b []byte, value *Order) error {
	buf := new(bytes.Buffer)
	buf.Write(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&value); err != nil {
		return err
	}

	return nil
}
