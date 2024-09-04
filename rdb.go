package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

const (
	EOF          = 0xff
	SELECTDB     = 0xfe
	EXPIRETIME   = 0xfd
	EXPIRETIMEMS = 0xfc
	RESIZEDB     = 0xfb
	AUX          = 0xfa
)

type KeyValuePair struct {
	key, value string
	expiry     int64
}

type RDB struct {
	Header   [9]byte // Magic string + version number (ASCII): "REDIS0007".
	Metadata map[string]string
	Database []KeyValuePair
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func readString(file *os.File) (string, error) {
	buffer := make([]byte, 1)
	_, err := file.Read(buffer)
	if err != nil {
		return "", err
	}

	size := buffer[0]

	switch size {
	case 0xc0:
		buffer = make([]byte, 1)
		_, err := file.Read(buffer)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", uint8(buffer[0])), nil
	case 0xc1:
		buffer = make([]byte, 2)
		_, err := file.Read(buffer)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", binary.LittleEndian.Uint16(buffer)), nil
	case 0xc2:
		buffer = make([]byte, 4)
		_, err := file.Read(buffer)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", binary.LittleEndian.Uint32(buffer)), nil
	default:
		buffer = make([]byte, size)
		_, err = file.Read(buffer)
		if err != nil {
			return "", err
		}
		return string(buffer), nil
	}

}

func readExpireTimestampMS(file *os.File) (int64, error) {
	buffer := make([]byte, 8)
	_, err := file.Read(buffer)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buffer)), nil
}

func readExpireTimestamp(file *os.File) (int64, error) {
	buffer := make([]byte, 4)
	_, err := file.Read(buffer)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint32(buffer)), nil
}

func readInt(file *os.File) (int, error) {

	buffer := make([]byte, 1)
	_, err := file.Read(buffer)
	if err != nil {
		return 0, err
	}

	mostSignificantTwoBits := (buffer[0] >> 6) & 0x3
	switch mostSignificantTwoBits {
	case 0b00:
		x := buffer[0] & 0b00111111
		return int(x), nil
	case 0b01:
		sixBitsOfCurrentByte := buffer[0] & 0b00111111
		nextByte := make([]byte, 1)
		_, err := file.Read(nextByte)
		if err != nil {
			return 0, err
		}
		bytes := []byte{sixBitsOfCurrentByte, nextByte[0]}
		return int(binary.BigEndian.Uint16(bytes)), nil
	case 0b10:
		nextFourBytes := make([]byte, 4)
		_, err := file.Read(nextFourBytes)
		if err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(nextFourBytes)), nil
	}

	return 0, fmt.Errorf("not an int object")
}

func (r *RDB) Load(path string) error {

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Header Section
	_, err = file.Read(r.Header[:])
	check(err)

	fmt.Printf("Magic String: %s\n", r.Header[:])

	// Metadata Section

	r.Metadata = make(map[string]string)

	b := make([]byte, 1)
	_, err = file.Read(b)
	check(err)

	// Indicates the start of a metadata section
	if b[0] != AUX {
		return fmt.Errorf("error: expected meta data section magic byte")
	}

	// read auxiliary fields until database section
	for b[0] != SELECTDB {

		key, err := readString(file)
		check(err)

		value, err := readString(file)
		check(err)

		fmt.Printf("[0xFA] %s : %s\n", key, value)

		r.Metadata[key] = value

		b = make([]byte, 1)
		_, err = file.Read(b)
		check(err)
	}

	// Database Section

	// The index of the database
	b = make([]byte, 1)
	file.Read(b)
	fmt.Printf("[0xFE] db number: %d\n", b[0])

	b = make([]byte, 1)
	_, err = file.Read(b)
	check(err)

	// Indicates the start of a metadata section
	if b[0] != RESIZEDB {
		return fmt.Errorf("error: expected database section magic byte")
	}

	HashTableSize, err := readInt(file)
	fmt.Printf("[0xFB] hash table size: %d\n", HashTableSize)
	check(err)

	ExpireHashTableSize, err := readInt(file)
	fmt.Printf("[0xFB] expire hash table size: %d\n", ExpireHashTableSize)
	check(err)

	// get value type; it will always be string for test?
	b = make([]byte, 1)
	file.Read(b)

	// read database section until end of file
	for (b[0] == 0x00 || b[0] == EXPIRETIME || b[0] == EXPIRETIMEMS) && b[0] != EOF {

		var expiryTime int64
		ms := false

		if b[0] == EXPIRETIME {
			expiryTime, err = readExpireTimestamp(file)
			check(err)

			// value type
			b = make([]byte, 1)
			file.Read(b)
		}

		if b[0] == EXPIRETIMEMS {
			ms = true
			expiryTime, err = readExpireTimestampMS(file)
			check(err)

			// value type
			b = make([]byte, 1)
			file.Read(b)
		}

		key, err := readString(file)
		check(err)

		value, err := readString(file)
		check(err)

		pair := KeyValuePair{key, value, expiryTime}
		r.Database = append(r.Database, pair)

		fmt.Printf("[KeyValuePair] %s: %s [%d] %t\n", key, value, expiryTime, ms)

		b = make([]byte, 1)
		_, err = file.Read(b)
		check(err)
	}
	// End of File Section

	return nil
}
