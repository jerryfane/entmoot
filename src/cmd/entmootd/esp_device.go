package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"entmoot/pkg/entmoot/esphttp"
)

func cmdESPDevice(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "esp device: expected list, add, onboard, rotate-key, enable, disable, or remove")
		return exitInvalidArgument
	}
	switch args[0] {
	case "list":
		return cmdESPDeviceList(gf, args[1:])
	case "add":
		return cmdESPDeviceAdd(gf, args[1:])
	case "onboard":
		return cmdESPDeviceOnboard(gf, args[1:])
	case "rotate-key":
		return cmdESPDeviceRotateKey(gf, args[1:])
	case "enable":
		return cmdESPDeviceSetDisabled(gf, args[1:], false)
	case "disable":
		return cmdESPDeviceSetDisabled(gf, args[1:], true)
	case "remove":
		return cmdESPDeviceRemove(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "esp device: unknown subcommand %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdESPDeviceRotateKey(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp device rotate-key", flag.ContinueOnError)
	id := fs.String("id", "", "device id")
	pubkey := fs.String("pubkey", "", "new base64 Ed25519 public key")
	path := fs.String("device-keys", "", "ESP device registry JSON path (default: <data>/esp-devices.json)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *id == "" {
		fmt.Fprintln(os.Stderr, "esp device rotate-key: -id is required")
		return exitInvalidArgument
	}
	if *pubkey == "" {
		fmt.Fprintln(os.Stderr, "esp device rotate-key: -pubkey is required")
		return exitInvalidArgument
	}
	reg, code, ok := loadESPDeviceRegistryForCLI(gf, *path)
	if !ok {
		return code
	}
	devices := append([]esphttp.Device(nil), reg.Devices...)
	found := false
	for i := range devices {
		if devices[i].ID == *id {
			pub, err := base64.StdEncoding.DecodeString(*pubkey)
			if err != nil || len(pub) != ed25519.PublicKeySize {
				fmt.Fprintf(os.Stderr, "esp device rotate-key: invalid -pubkey\n")
				return exitInvalidArgument
			}
			devices[i].PublicKey = append(ed25519.PublicKey(nil), pub...)
			found = true
			break
		}
	}
	if !found {
		fmt.Fprintf(os.Stderr, "esp device rotate-key: device %q not found\n", *id)
		return exitInvalidArgument
	}
	next, err := esphttp.NewDeviceRegistry(devices)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device rotate-key: %v\n", err)
		return exitInvalidArgument
	}
	return saveAndEmitESPDeviceRegistry(gf, *path, next, "esp device rotate-key")
}

func cmdESPDeviceList(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp device list", flag.ContinueOnError)
	path := fs.String("device-keys", "", "ESP device registry JSON path (default: <data>/esp-devices.json)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	reg, code, ok := loadESPDeviceRegistryForCLI(gf, *path)
	if !ok {
		return code
	}
	if err := emitJSON(esphttp.DeviceRegistryDocumentFromRegistry(reg)); err != nil {
		fmt.Fprintf(os.Stderr, "esp device list: %v\n", err)
		return exitTransport
	}
	return exitOK
}

func cmdESPDeviceAdd(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp device add", flag.ContinueOnError)
	id := fs.String("id", "", "device id")
	pubkey := fs.String("pubkey", "", "base64 Ed25519 public key")
	disabled := fs.Bool("disabled", false, "create the device disabled")
	path := fs.String("device-keys", "", "ESP device registry JSON path (default: <data>/esp-devices.json)")
	var groups repeatedStringFlag
	var clients repeatedStringFlag
	fs.Var(&groups, "group", "base64 group id; may be repeated")
	fs.Var(&clients, "client", "authorized mailbox client id; may be repeated")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *id == "" {
		fmt.Fprintln(os.Stderr, "esp device add: -id is required")
		return exitInvalidArgument
	}
	if *pubkey == "" {
		fmt.Fprintln(os.Stderr, "esp device add: -pubkey is required")
		return exitInvalidArgument
	}
	if len(groups) == 0 {
		fmt.Fprintln(os.Stderr, "esp device add: at least one -group is required")
		return exitInvalidArgument
	}
	if len(clients) == 0 {
		clients = append(clients, *id)
	}
	reg, code, ok := loadESPDeviceRegistryForCLI(gf, *path)
	if !ok {
		return code
	}
	for _, d := range reg.Devices {
		if d.ID == *id {
			fmt.Fprintf(os.Stderr, "esp device add: device %q already exists\n", *id)
			return exitInvalidArgument
		}
	}
	device, err := esphttp.DeviceFromRecord(esphttp.DeviceRecord{
		ID:        *id,
		PublicKey: *pubkey,
		Groups:    append([]string(nil), groups...),
		ClientIDs: append([]string(nil), clients...),
		Disabled:  *disabled,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device add: %v\n", err)
		return exitInvalidArgument
	}
	devices := append(append([]esphttp.Device(nil), reg.Devices...), device)
	next, err := esphttp.NewDeviceRegistry(devices)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device add: %v\n", err)
		return exitInvalidArgument
	}
	return saveAndEmitESPDeviceRegistry(gf, *path, next, "esp device add")
}

type espDeviceOnboardOutput struct {
	Device       esphttp.DeviceRecord `json:"device"`
	PrivateKey   string               `json:"private_key"`
	RegistryPath string               `json:"registry_path"`
}

func cmdESPDeviceOnboard(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp device onboard", flag.ContinueOnError)
	id := fs.String("id", "", "device id")
	disabled := fs.Bool("disabled", false, "create the device disabled")
	path := fs.String("device-keys", "", "ESP device registry JSON path (default: <data>/esp-devices.json)")
	var groups repeatedStringFlag
	var clients repeatedStringFlag
	fs.Var(&groups, "group", "base64 group id; may be repeated")
	fs.Var(&clients, "client", "authorized mailbox client id; may be repeated")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *id == "" {
		fmt.Fprintln(os.Stderr, "esp device onboard: -id is required")
		return exitInvalidArgument
	}
	if len(groups) == 0 {
		fmt.Fprintln(os.Stderr, "esp device onboard: at least one -group is required")
		return exitInvalidArgument
	}
	if len(clients) == 0 {
		clients = append(clients, *id)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device onboard: generate key: %v\n", err)
		return exitTransport
	}
	record := esphttp.DeviceRecord{
		ID:        *id,
		PublicKey: base64.StdEncoding.EncodeToString(pub),
		Groups:    append([]string(nil), groups...),
		ClientIDs: append([]string(nil), clients...),
		Disabled:  *disabled,
	}
	device, err := esphttp.DeviceFromRecord(record)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device onboard: %v\n", err)
		return exitInvalidArgument
	}
	reg, code, ok := loadESPDeviceRegistryForCLI(gf, *path)
	if !ok {
		return code
	}
	for _, d := range reg.Devices {
		if d.ID == *id {
			fmt.Fprintf(os.Stderr, "esp device onboard: device %q already exists\n", *id)
			return exitInvalidArgument
		}
	}
	devices := append(append([]esphttp.Device(nil), reg.Devices...), device)
	next, err := esphttp.NewDeviceRegistry(devices)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device onboard: %v\n", err)
		return exitInvalidArgument
	}
	registryPath, err := espDeviceRegistryPath(gf, *path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device onboard: %v\n", err)
		return exitInvalidArgument
	}
	if err := esphttp.SaveDeviceRegistry(registryPath, next); err != nil {
		fmt.Fprintf(os.Stderr, "esp device onboard: %v\n", err)
		return exitInvalidArgument
	}
	outputPath := registryPath
	if abs, err := filepath.Abs(registryPath); err == nil {
		outputPath = abs
	}
	if err := emitJSON(espDeviceOnboardOutput{
		Device:       record,
		PrivateKey:   base64.StdEncoding.EncodeToString(priv),
		RegistryPath: outputPath,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "esp device onboard: %v\n", err)
		return exitTransport
	}
	return exitOK
}

func cmdESPDeviceSetDisabled(gf *globalFlags, args []string, disabled bool) int {
	name := "enable"
	if disabled {
		name = "disable"
	}
	fs := flag.NewFlagSet("esp device "+name, flag.ContinueOnError)
	id := fs.String("id", "", "device id")
	path := fs.String("device-keys", "", "ESP device registry JSON path (default: <data>/esp-devices.json)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *id == "" {
		fmt.Fprintf(os.Stderr, "esp device %s: -id is required\n", name)
		return exitInvalidArgument
	}
	reg, code, ok := loadESPDeviceRegistryForCLI(gf, *path)
	if !ok {
		return code
	}
	devices := append([]esphttp.Device(nil), reg.Devices...)
	found := false
	for i := range devices {
		if devices[i].ID == *id {
			devices[i].Disabled = disabled
			found = true
			break
		}
	}
	if !found {
		fmt.Fprintf(os.Stderr, "esp device %s: device %q not found\n", name, *id)
		return exitInvalidArgument
	}
	next, err := esphttp.NewDeviceRegistry(devices)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device %s: %v\n", name, err)
		return exitInvalidArgument
	}
	return saveAndEmitESPDeviceRegistry(gf, *path, next, "esp device "+name)
}

func cmdESPDeviceRemove(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp device remove", flag.ContinueOnError)
	id := fs.String("id", "", "device id")
	path := fs.String("device-keys", "", "ESP device registry JSON path (default: <data>/esp-devices.json)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *id == "" {
		fmt.Fprintln(os.Stderr, "esp device remove: -id is required")
		return exitInvalidArgument
	}
	reg, code, ok := loadESPDeviceRegistryForCLI(gf, *path)
	if !ok {
		return code
	}
	devices := make([]esphttp.Device, 0, len(reg.Devices))
	found := false
	for _, d := range reg.Devices {
		if d.ID == *id {
			found = true
			continue
		}
		devices = append(devices, d)
	}
	if !found {
		fmt.Fprintf(os.Stderr, "esp device remove: device %q not found\n", *id)
		return exitInvalidArgument
	}
	next, err := esphttp.NewDeviceRegistry(devices)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device remove: %v\n", err)
		return exitInvalidArgument
	}
	return saveAndEmitESPDeviceRegistry(gf, *path, next, "esp device remove")
}

func loadESPDeviceRegistryForCLI(gf *globalFlags, rawPath string) (*esphttp.DeviceRegistry, int, bool) {
	path, err := espDeviceRegistryPath(gf, rawPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device: %v\n", err)
		return nil, exitInvalidArgument, false
	}
	reg, err := esphttp.LoadDeviceRegistryOrEmpty(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp device: %v\n", err)
		return nil, exitInvalidArgument, false
	}
	return reg, exitOK, true
}

func saveAndEmitESPDeviceRegistry(gf *globalFlags, rawPath string, reg *esphttp.DeviceRegistry, prefix string) int {
	path, err := espDeviceRegistryPath(gf, rawPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prefix, err)
		return exitInvalidArgument
	}
	if err := esphttp.SaveDeviceRegistry(path, reg); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prefix, err)
		return exitInvalidArgument
	}
	if err := emitJSON(esphttp.DeviceRegistryDocumentFromRegistry(reg)); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prefix, err)
		return exitTransport
	}
	return exitOK
}

func espDeviceRegistryPath(gf *globalFlags, rawPath string) (string, error) {
	path := rawPath
	if path == "" {
		path = filepath.Join(gf.data, "esp-devices.json")
	}
	return expandHome(path)
}

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string {
	return fmt.Sprint([]string(*f))
}

func (f *repeatedStringFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}
