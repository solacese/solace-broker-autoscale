package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

const defaultJWTVariable = "SOLACE_CLOUD_JWT"

func loadRawJWT(path, variable string) (string, error) {
	if variable == "" {
		return "", fmt.Errorf("JWT environment variable name is empty")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open credential file: %w", err)
	}
	defer file.Close()

	values, err := parseDotEnv(file)
	if err != nil {
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			return "", fmt.Errorf("parse credential file: %w", err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(file, 16<<10))
		if readErr != nil {
			return "", fmt.Errorf("read raw credential file: %w", readErr)
		}
		jwt := strings.TrimSpace(string(raw))
		if strings.Count(jwt, ".") == 2 && !strings.ContainsAny(jwt, " \t\r\n") {
			return jwt, nil
		}
		return "", fmt.Errorf("parse credential file: %w", err)
	}
	jwt := values[variable]
	if jwt == "" {
		return "", fmt.Errorf("credential file does not contain a non-empty %s value", variable)
	}
	return jwt, nil
}

func parseDotEnv(input io.Reader) (map[string]string, error) {
	values := make(map[string]string)
	scanner := bufio.NewScanner(input)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, found := strings.Cut(line, "=")
		if !found {
			return nil, fmt.Errorf("line %d has no assignment", lineNumber)
		}
		key = strings.TrimSpace(key)
		if key == "" || strings.ContainsAny(key, " \t\r\n") {
			return nil, fmt.Errorf("line %d has an invalid name", lineNumber)
		}
		parsed, err := dotenvValue(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNumber, err)
		}
		values[key] = parsed
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func dotenvValue(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if value[0] != '\'' && value[0] != '"' {
		if comment := strings.Index(value, " #"); comment >= 0 {
			value = value[:comment]
		}
		return strings.TrimSpace(value), nil
	}
	quote := value[0]
	if len(value) < 2 || value[len(value)-1] != quote {
		return "", fmt.Errorf("unterminated quoted value")
	}
	value = value[1 : len(value)-1]
	if quote == '\'' {
		return value, nil
	}
	var result strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' {
			result.WriteByte(value[index])
			continue
		}
		index++
		if index == len(value) {
			return "", fmt.Errorf("unterminated escape")
		}
		switch value[index] {
		case 'n':
			result.WriteByte('\n')
		case 'r':
			result.WriteByte('\r')
		case 't':
			result.WriteByte('\t')
		case '\\', '"':
			result.WriteByte(value[index])
		default:
			return "", fmt.Errorf("unsupported escape")
		}
	}
	return result.String(), nil
}
