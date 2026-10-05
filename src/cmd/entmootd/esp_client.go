package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/signing"
)

const (
	espDeviceKeyFile    = "esp-device.key"
	espClientConfigFile = "esp-client.json"
	// espMemberDevicesFile is the ESP-side registry of self-enrolled member
	// devices, kept apart from the operator registry esp-devices.json.
	espMemberDevicesFile = "esp-member-devices.json"
	espClientTimeout     = 30 * time.Second
	maxESPResponseBytes  = 32 << 20
)

// espClientConfig is what `esp connect` leaves behind for `esp history` and
// `esp publish`: which ESP to call and which device the ESP enrolled.
type espClientConfig struct {
	ESP      string            `json:"esp"`
	DeviceID string            `json:"device_id"`
	ClientID string            `json:"client_id"`
	Groups   []entmoot.GroupID `json:"groups"`
}

type espConnectOutput struct {
	DeviceID string            `json:"device_id"`
	ClientID string            `json:"client_id"`
	Groups   []entmoot.GroupID `json:"groups"`
	ESP      string            `json:"esp"`
}

type groupIDFlags []entmoot.GroupID

func (g *groupIDFlags) String() string { return fmt.Sprint(len(*g)) }

func (g *groupIDFlags) Set(raw string) error {
	gid, err := parseESPClientGroupID(raw)
	if err != nil {
		return err
	}
	*g = append(*g, gid)
	return nil
}

func cmdESPConnect(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp connect", flag.ContinueOnError)
	espURL := fs.String("esp", "", "ESP base URL, e.g. https://esp.example.org (required)")
	var groups groupIDFlags
	fs.Var(&groups, "group", "group id to request access to (repeatable, required)")
	clientID := fs.String("client", "", "optional client label; the ESP namespaces it under the device id")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	base, err := normalizeESPBaseURL(*espURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: %v\n", err)
		return exitInvalidArgument
	}
	if len(groups) == 0 {
		fmt.Fprintln(os.Stderr, "esp connect: at least one -group is required")
		return exitInvalidArgument
	}
	identity, err := keystore.Load(gf.identity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: load identity %s: %v\n", gf.identity, err)
		return exitInvalidArgument
	}
	devicePriv, err := loadOrCreateESPDeviceKey(filepath.Join(gf.data, espDeviceKeyFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: %v\n", err)
		return exitTransport
	}
	req, err := buildESPConnectRequest(identity, devicePriv, groups, strings.TrimSpace(*clientID), time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: %v\n", err)
		return exitInvalidArgument
	}
	body, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: %v\n", err)
		return exitTransport
	}
	var resp esphttp.MemberConnectResponse
	if err := doESPClientRequest(context.Background(), http.MethodPost, base+esphttp.MemberConnectPath, nil, body, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: %v\n", err)
		return exitTransport
	}
	cfg := espClientConfig{ESP: base, DeviceID: resp.DeviceID, ClientID: resp.ClientID, Groups: resp.Groups}
	if err := saveESPClientConfig(filepath.Join(gf.data, espClientConfigFile), cfg); err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: %v\n", err)
		return exitTransport
	}
	if err := emitJSON(espConnectOutput{DeviceID: resp.DeviceID, ClientID: resp.ClientID, Groups: resp.Groups, ESP: base}); err != nil {
		fmt.Fprintf(os.Stderr, "esp connect: %v\n", err)
		return exitTransport
	}
	return exitOK
}

func cmdESPHistory(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp history", flag.ContinueOnError)
	group := fs.String("group", "", "group id (required)")
	limit := fs.Int("limit", 0, "max messages (1-200; default: ESP default)")
	cursor := fs.String("cursor", "", "next_cursor from a previous page")
	topic := fs.String("topic", "", "restrict to one topic")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := parseESPClientGroupID(*group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp history: -group: %v\n", err)
		return exitInvalidArgument
	}
	if *limit < 0 {
		fmt.Fprintln(os.Stderr, "esp history: -limit must be positive")
		return exitInvalidArgument
	}
	client, err := openESPClient(gf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp history: %v\n", err)
		return exitInvalidArgument
	}
	query := url.Values{}
	query.Set("client_id", client.cfg.ClientID)
	if *limit > 0 {
		query.Set("limit", strconv.Itoa(*limit))
	}
	if v := strings.TrimSpace(*cursor); v != "" {
		query.Set("cursor", v)
	}
	if v := strings.TrimSpace(*topic); v != "" {
		query.Set("topic", v)
	}
	var out json.RawMessage
	if err := client.do(context.Background(), http.MethodGet, espGroupPath(gid, "history")+"?"+query.Encode(), nil, &out); err != nil {
		fmt.Fprintf(os.Stderr, "esp history: %v\n", err)
		return exitTransport
	}
	fmt.Println(string(out))
	return exitOK
}

func cmdESPPublish(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp publish", flag.ContinueOnError)
	group := fs.String("group", "", "group id (required)")
	topics := fs.String("topic", "", "comma-separated topics (required)")
	content := fs.String("content", "", "message content (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := parseESPClientGroupID(*group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp publish: -group: %v\n", err)
		return exitInvalidArgument
	}
	topicList := splitESPTopics(*topics)
	if len(topicList) == 0 {
		fmt.Fprintln(os.Stderr, "esp publish: -topic is required")
		return exitInvalidArgument
	}
	if *content == "" {
		fmt.Fprintln(os.Stderr, "esp publish: -content is required")
		return exitInvalidArgument
	}
	out, code, err := publishThroughESP(context.Background(), gf, gid, topicList, []byte(*content))
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp publish: %v\n", err)
		return code
	}
	fmt.Println(string(out))
	return exitOK
}

// publishThroughESP signs content with this member's identity against the
// roster head the connected ESP reports, posts it to the ESP, and returns the
// ESP's response. On failure it also returns the exit code for the error.
func publishThroughESP(ctx context.Context, gf *globalFlags, gid entmoot.GroupID, topics []string, content []byte) (json.RawMessage, int, error) {
	identity, err := keystore.Load(gf.identity)
	if err != nil {
		return nil, exitInvalidArgument, fmt.Errorf("load identity %s: %w", gf.identity, err)
	}
	client, err := openESPClient(gf)
	if err != nil {
		return nil, exitInvalidArgument, err
	}
	var summary esphttp.GroupSummary
	if err := client.do(ctx, http.MethodGet, espGroupPath(gid, ""), nil, &summary); err != nil {
		return nil, exitTransport, fmt.Errorf("read roster head: %w", err)
	}
	if summary.RosterHead == (entmoot.RosterEntryID{}) {
		return nil, exitTransport, errors.New("ESP did not report a roster head for the group")
	}
	msg, err := buildESPSignedMessage(ctx, identity, gid, summary.RosterHead, topics, content, time.Now())
	if err != nil {
		return nil, exitInvalidArgument, err
	}
	body, err := json.Marshal(map[string]entmoot.Message{"message": msg})
	if err != nil {
		return nil, exitTransport, err
	}
	var out json.RawMessage
	if err := client.do(ctx, http.MethodPost, espGroupPath(gid, "messages"), body, &out); err != nil {
		return nil, exitTransport, err
	}
	return out, exitOK, nil
}

// buildESPConnectRequest signs a connect request with the member's Entmoot
// identity, binding the device key, every requested group, and the client
// label under the connect signature domain.
func buildESPConnectRequest(identity *keystore.Identity, devicePriv ed25519.PrivateKey, groups []entmoot.GroupID, clientID string, now time.Time) (esphttp.MemberConnectRequest, error) {
	memberID, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		return esphttp.MemberConnectRequest{}, err
	}
	nonce, err := generateESPRequestNonce()
	if err != nil {
		return esphttp.MemberConnectRequest{}, fmt.Errorf("generate nonce: %w", err)
	}
	req := esphttp.MemberConnectRequest{
		DevicePublicKey: base64.StdEncoding.EncodeToString(devicePriv.Public().(ed25519.PublicKey)),
		GroupIDs:        append([]entmoot.GroupID(nil), groups...),
		ClientID:        clientID,
		MemberID:        memberID,
		EntmootPubKey:   base64.StdEncoding.EncodeToString(identity.PublicKey),
		TimestampMS:     now.UnixMilli(),
		Nonce:           nonce,
	}
	req.Signature = base64.StdEncoding.EncodeToString(identity.Sign([]byte(esphttp.MemberConnectSigningInput(req))))
	req.DeviceSignature = base64.StdEncoding.EncodeToString(ed25519.Sign(devicePriv, []byte(esphttp.MemberConnectDeviceSigningInput(req))))
	return req, nil
}

func buildESPSignedMessage(ctx context.Context, identity *keystore.Identity, gid entmoot.GroupID, head entmoot.RosterEntryID, topics []string, content []byte, now time.Time) (entmoot.Message, error) {
	memberID, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
	if err != nil {
		return entmoot.Message{}, err
	}
	peerID, err := entmoot.PeerIDFromPublicKey(identity.PublicKey)
	if err != nil {
		return entmoot.Message{}, err
	}
	author := entmoot.NodeInfo{MemberID: &memberID, PeerID: peerID, EntmootPubKey: append([]byte(nil), identity.PublicKey...)}
	signer, err := signing.NewLocalSigner(author, identity)
	if err != nil {
		return entmoot.Message{}, err
	}
	return signing.SignMessage(ctx, signer, entmoot.Message{
		Version:    2,
		GroupID:    gid,
		Author:     author,
		Timestamp:  now.UnixMilli(),
		Topics:     topics,
		Content:    content,
		RosterHead: &head,
	})
}

type espClient struct {
	cfg  espClientConfig
	priv ed25519.PrivateKey
}

func openESPClient(gf *globalFlags) (*espClient, error) {
	path := filepath.Join(gf.data, espClientConfigFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%s not found; run `entmootd esp connect` first", path)
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg espClientConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.ESP == "" || cfg.DeviceID == "" || cfg.ClientID == "" {
		return nil, fmt.Errorf("%s is incomplete; rerun `entmootd esp connect`", path)
	}
	priv, err := readESPDevicePrivateKey(filepath.Join(gf.data, espDeviceKeyFile))
	if err != nil {
		return nil, fmt.Errorf("device key: %w", err)
	}
	return &espClient{cfg: cfg, priv: priv}, nil
}

// do sends one device-signed request; pathWithQuery is signed exactly as sent.
func (c *espClient) do(ctx context.Context, method, pathWithQuery string, body []byte, out any) error {
	nonce, err := generateESPRequestNonce()
	if err != nil {
		return fmt.Errorf("generate nonce: %w", err)
	}
	ts := time.Now().UnixMilli()
	sig := ed25519.Sign(c.priv, []byte(esphttp.DeviceSigningInput(method, pathWithQuery, ts, nonce, body)))
	headers := map[string]string{
		"X-Entmoot-Device-ID":    c.cfg.DeviceID,
		"X-Entmoot-Timestamp-Ms": strconv.FormatInt(ts, 10),
		"X-Entmoot-Nonce":        nonce,
		"X-Entmoot-Signature":    base64.StdEncoding.EncodeToString(sig),
	}
	return doESPClientRequest(ctx, method, c.cfg.ESP+pathWithQuery, headers, body, out)
}

// doESPClientRequest uses the default transport, so HTTPS_PROXY and system TLS
// verification apply unchanged.
func doESPClientRequest(ctx context.Context, method, target string, headers map[string]string, body []byte, out any) error {
	ctx, cancel := context.WithTimeout(ctx, espClientTimeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, target, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxESPResponseBytes))
	if err != nil {
		return fmt.Errorf("%s %s: read response: %w", method, target, err)
	}
	if resp.StatusCode/100 != 2 {
		var envelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(data, &envelope) == nil && envelope.Error.Code != "" {
			return fmt.Errorf("ESP refused (%d %s): %s", resp.StatusCode, envelope.Error.Code, envelope.Error.Message)
		}
		return fmt.Errorf("ESP refused (%d): %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode ESP response: %w", err)
	}
	return nil
}

// loadOrCreateESPDeviceKey returns the agent's ESP device key, creating it
// with 0600 permissions on first use. O_EXCL keeps two concurrent first runs
// from silently replacing each other's key.
func loadOrCreateESPDeviceKey(path string) (ed25519.PrivateKey, error) {
	if priv, err := readESPDevicePrivateKey(path); err == nil {
		return priv, nil
	} else if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("device key %s: %w", path, err)
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate device key: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return readESPDevicePrivateKey(path)
		}
		return nil, fmt.Errorf("create device key %s: %w", path, err)
	}
	if _, err := f.WriteString(base64.StdEncoding.EncodeToString(priv) + "\n"); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write device key %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("close device key %s: %w", path, err)
	}
	return priv, nil
}

func saveESPClientConfig(path string, cfg espClientConfig) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".esp-client-*.json")
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func normalizeESPBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("-esp is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", fmt.Errorf("-esp %q must be an http(s) URL", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("-esp %q must not carry a query or fragment", raw)
	}
	return strings.TrimRight(u.String(), "/"), nil
}

func parseESPClientGroupID(raw string) (entmoot.GroupID, error) {
	var gid entmoot.GroupID
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return gid, errors.New("group id is required")
	}
	if err := gid.UnmarshalText([]byte(raw)); err != nil {
		return gid, err
	}
	return gid, nil
}

func espGroupPath(gid entmoot.GroupID, suffix string) string {
	p := "/v1/groups/" + url.PathEscape(gid.String())
	if suffix != "" {
		p += "/" + suffix
	}
	return p
}

func splitESPTopics(raw string) []string {
	var out []string
	for _, t := range strings.Split(raw, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}
