package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"entmoot/pkg/entmoot/esphttp"
)

type espSignRequestOutput struct {
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	BodySHA256   string            `json:"body_sha256"`
	Headers      map[string]string `json:"headers"`
	SigningInput string            `json:"signing_input,omitempty"`
}

func cmdESPSignRequest(_ *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp sign-request", flag.ContinueOnError)
	deviceID := fs.String("device", "", "ESP device id")
	privateKeyPath := fs.String("private-key-file", "", "path containing base64 Ed25519 private key")
	method := fs.String("method", "", "HTTP method to sign")
	path := fs.String("path", "", "HTTP path with optional raw query")
	bodyPath := fs.String("body", "", "request body file; omit for empty body")
	timestampMillis := fs.Int64("timestamp-ms", 0, "Unix timestamp in milliseconds (default: now)")
	nonce := fs.String("nonce", "", "request nonce (default: random)")
	showInput := fs.Bool("show-input", false, "include canonical signing input in JSON output")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if strings.TrimSpace(*deviceID) == "" {
		fmt.Fprintln(os.Stderr, "esp sign-request: -device is required")
		return exitInvalidArgument
	}
	if strings.TrimSpace(*privateKeyPath) == "" {
		fmt.Fprintln(os.Stderr, "esp sign-request: -private-key-file is required")
		return exitInvalidArgument
	}
	if strings.TrimSpace(*method) == "" {
		fmt.Fprintln(os.Stderr, "esp sign-request: -method is required")
		return exitInvalidArgument
	}
	if strings.TrimSpace(*path) == "" {
		fmt.Fprintln(os.Stderr, "esp sign-request: -path is required")
		return exitInvalidArgument
	}
	methodUpper := strings.ToUpper(strings.TrimSpace(*method))
	body, err := readESPRequestBody(*bodyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp sign-request: %v\n", err)
		return exitInvalidArgument
	}
	priv, err := readESPDevicePrivateKey(*privateKeyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp sign-request: %v\n", err)
		return exitInvalidArgument
	}
	ts := *timestampMillis
	if ts < 0 {
		fmt.Fprintln(os.Stderr, "esp sign-request: -timestamp-ms must be non-negative")
		return exitInvalidArgument
	}
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	nonceValue := strings.TrimSpace(*nonce)
	if nonceValue == "" {
		nonceValue, err = generateESPRequestNonce()
		if err != nil {
			fmt.Fprintf(os.Stderr, "esp sign-request: generate nonce: %v\n", err)
			return exitTransport
		}
	}
	pathWithQuery := strings.TrimSpace(*path)
	input := esphttp.DeviceSigningInput(methodUpper, pathWithQuery, ts, nonceValue, body)
	sig := ed25519.Sign(priv, []byte(input))
	sum := sha256.Sum256(body)
	out := espSignRequestOutput{
		Method:     methodUpper,
		Path:       pathWithQuery,
		BodySHA256: base64.StdEncoding.EncodeToString(sum[:]),
		Headers: map[string]string{
			"X-Entmoot-Device-ID":    strings.TrimSpace(*deviceID),
			"X-Entmoot-Timestamp-Ms": fmt.Sprintf("%d", ts),
			"X-Entmoot-Nonce":        nonceValue,
			"X-Entmoot-Signature":    base64.StdEncoding.EncodeToString(sig),
		},
	}
	if *showInput {
		out.SigningInput = input
	}
	if err := emitJSON(out); err != nil {
		fmt.Fprintf(os.Stderr, "esp sign-request: %v\n", err)
		return exitTransport
	}
	return exitOK
}

func readESPRequestBody(path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	expanded, err := expandHome(path)
	if err != nil {
		return nil, err
	}
	body, err := os.ReadFile(expanded)
	if err != nil {
		return nil, fmt.Errorf("read body file %s: %w", expanded, err)
	}
	return body, nil
}

func readESPDevicePrivateKey(path string) (ed25519.PrivateKey, error) {
	expanded, err := expandHome(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(expanded)
	if err != nil {
		return nil, fmt.Errorf("read private key file %s: %w", expanded, err)
	}
	rawText := strings.TrimSpace(string(data))
	raw, err := base64.StdEncoding.DecodeString(rawText)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(rawText)
		if err != nil {
			return nil, fmt.Errorf("private key: invalid base64: %w", err)
		}
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("private key length %d, want %d", len(raw), ed25519.PrivateKeySize)
	}
	return ed25519.PrivateKey(raw), nil
}

func generateESPRequestNonce() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}
