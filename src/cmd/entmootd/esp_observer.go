package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
)

// The owner token stays on the operator machine. The mobile exchange carries
// public keys/signatures only and never includes the agent or phone secret.
func cmdESPObserver(gf *globalFlags, action string, args []string) int {
	fs := flag.NewFlagSet("esp device "+action, flag.ContinueOnError)
	endpoint := fs.String("esp", "", "ESP base URL (required)")
	request := fs.String("request", "", "phone pairing request: base64url JSON or entmoot://pair-request link")
	deviceID := fs.String("id", "", "observer device ID to revoke")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	base, err := normalizeESPBaseURL(*endpoint)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitInvalidArgument
	}
	token := strings.TrimSpace(os.Getenv("ENTMOOT_ESP_TOKEN"))
	target, err := url.Parse(base)
	if err != nil || target.User != nil {
		fmt.Fprintln(os.Stderr, "invalid ESP endpoint")
		return exitInvalidArgument
	}
	ip := net.ParseIP(target.Hostname())
	if target.Scheme != "https" && !(target.Scheme == "http" && (target.Hostname() == "localhost" || ip != nil && ip.IsLoopback())) {
		fmt.Fprintln(os.Stderr, "owner authorization requires HTTPS except for loopback")
		return exitInvalidArgument
	}
	if token == "" {
		fmt.Fprintln(os.Stderr, "observer management requires ENTMOOT_ESP_TOKEN on the operator machine; never send that token to the phone")
		return exitInvalidArgument
	}
	method := http.MethodGet
	path := "/v1/observers"
	var body []byte
	var consent esphttp.ObserverPairConsent
	switch action {
	case "pair-observer":
		encoded := strings.TrimSpace(*request)
		if strings.HasPrefix(encoded, "entmoot://") {
			link, e := url.Parse(encoded)
			if e != nil || link.Host != "pair-request" {
				fmt.Fprintln(os.Stderr, "invalid phone request link")
				return exitInvalidArgument
			}
			encoded = link.Query().Get("payload")
		}
		raw, e := base64.RawURLEncoding.DecodeString(encoded)
		if e != nil || len(raw) > 8192 {
			fmt.Fprintln(os.Stderr, "invalid phone pairing request")
			return exitInvalidArgument
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if e = decoder.Decode(&consent.Request); e != nil {
			fmt.Fprintln(os.Stderr, "invalid phone pairing request:", e)
			return exitInvalidArgument
		}
		if _, e = esphttp.ValidateObserverPairRequest(consent.Request, time.Now()); e != nil {
			fmt.Fprintln(os.Stderr, e)
			return exitInvalidArgument
		}
		if consent.Request.ESP != base {
			fmt.Fprintln(os.Stderr, "phone pairing endpoint differs from -esp; refusing approval")
			return exitInvalidArgument
		}
		identity, e := keystore.Load(gf.identity)
		if e != nil {
			fmt.Fprintln(os.Stderr, "load agent identity:", e)
			return exitInvalidArgument
		}
		consent.MemberID, e = entmoot.MemberIDFromPublicKey(identity.PublicKey)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			return exitInvalidArgument
		}
		consent.EntmootPubKey = base64.StdEncoding.EncodeToString(identity.PublicKey)
		consent.Signature = base64.StdEncoding.EncodeToString(identity.Sign([]byte(esphttp.ObserverPairConsentSigningInput(consent))))
		body, e = json.Marshal(consent)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			return exitTransport
		}
		method = http.MethodPost
	case "revoke-observer":
		if strings.TrimSpace(*deviceID) == "" {
			fmt.Fprintln(os.Stderr, "-id is required")
			return exitInvalidArgument
		}
		method = http.MethodDelete
		path += "?device_id=" + url.QueryEscape(*deviceID)
	case "observers":
	default:
		return exitInvalidArgument
	}
	req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitInvalidArgument
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "observer request:", err)
		return exitTransport
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitTransport
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "observer request: HTTP %d: %s\n", resp.StatusCode, raw)
		return exitTransport
	}
	if action != "pair-observer" {
		fmt.Println(string(raw))
		return exitOK
	}
	var result esphttp.ObserverPairResult
	if err = json.Unmarshal(raw, &result); err != nil || result.Consent != consent || !result.FollowAgent {
		fmt.Fprintln(os.Stderr, "ESP did not confirm the exact read-only pairing request")
		return exitTransport
	}
	pub, _, err := esphttp.ValidateObserverPairConsent(result.Consent, time.Now())
	if err != nil || result.DeviceID != esphttp.SelfEnrolledDeviceID(pub) {
		fmt.Fprintln(os.Stderr, "invalid pairing confirmation")
		return exitTransport
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	returnCode := emitJSON(map[string]any{"device_id": result.DeviceID, "member_id": consent.MemberID, "follow_agent": true, "pairing_url": "entmoot://observe?payload=" + payload})
	if returnCode != nil {
		fmt.Fprintln(os.Stderr, returnCode)
		return exitTransport
	}
	return exitOK
}
