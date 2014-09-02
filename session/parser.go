package session

import (
	"fmt"
	"strconv"
)

func lowReadString(src []byte) ([]byte, string, error) {

	ret := ""
	i := 0
	for {
		if src[i] == 13 && src[i+1] == 10 {
			break
		}
		ret += string(src[i])
		i++
	}

	return src[i+2:], ret, nil
}

func lowReadInteger(src []byte) ([]byte, int, error) {
	src, str, err := lowReadString(src)

	if err != nil {
		return src, -1, err
	}

	number, err := strconv.Atoi(str)
	return src, number, err
}

func readBulkString(src []byte) ([]byte, string, error) {

	// $
	if src[0] != '$' {
		return src, "", fmt.Errorf("Bulk string doesn't start with $ but with '%c' / %d", src[0], src[0])
	}

	src, _, err := lowReadInteger(src[1:])

	if err != nil {
		return src, "", err
	}

	src, str, err := lowReadString(src)

	if err != nil {
		return src, "", err
	}

	return src, str, nil
}

func readRequest(src []byte) ([]string, error) {

	if len(src) == 4 && string(src) == "PING" {
		return []string{"PING"}, nil
	}

	return readBulkStringArray(src)
}

func readBulkStringArray(src []byte) ([]string, error) {

	if src[0] != '*' {
		return nil, fmt.Errorf("Array doesn't start with * but with %c", src[0])
	}

	// we read the number of items
	src, arraySize, err := lowReadInteger(src[1:])

	if err != nil {
		return nil, err
	}

	retList := make([]string, arraySize)

	for i := 0; i < arraySize; i++ {

		src, retList[i], err = readBulkString(src)

		if err != nil {
			return nil, err
		}
	}

	return retList, nil

}
