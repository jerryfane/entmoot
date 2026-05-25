package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"entmoot/internal/plugininstall"
	"entmoot/internal/pluginpack"
	"entmoot/internal/subprocess"
	"entmoot/skills"
)

var pluginLookPath = exec.LookPath
var pluginInstallRunner subprocess.Runner = subprocess.ExecRunner{}
var pluginValidationRunner subprocess.Runner = subprocess.ExecRunner{}

type pluginCheck struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Detail   string `json:"detail"`
	Required bool   `json:"required"`
}

type pluginDoctorRuntime struct {
	Runtime string        `json:"runtime"`
	Path    string        `json:"path"`
	Healthy bool          `json:"healthy"`
	Checks  []pluginCheck `json:"checks"`
}

type pluginDoctorOutput struct {
	Home     string                `json:"home"`
	Runtimes []pluginDoctorRuntime `json:"runtimes"`
}

func cmdPlugin(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		printPluginUsage(os.Stdout)
		return exitOK
	}
	switch args[0] {
	case "build":
		return cmdPluginBuild(gf, args[1:])
	case "install":
		return cmdPluginInstall(gf, args[1:])
	case "path":
		return cmdPluginPath(gf, args[1:])
	case "doctor":
		return cmdPluginDoctor(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "plugin: unknown command %q\n\n", args[0])
		printPluginUsage(os.Stderr)
		return exitInvalidArgument
	}
}

func printPluginUsage(w *os.File) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  entmootd plugin build codex|claude [--out DIR] [--home DIR] [--force]")
	fmt.Fprintln(w, "  entmootd plugin install codex|claude [--scope user|project|local] [--home DIR] [--force]")
	fmt.Fprintln(w, "  entmootd plugin path codex|claude [--home DIR]")
	fmt.Fprintln(w, "  entmootd plugin doctor [codex|claude] [--home DIR] [--json]")
}

func cmdPluginBuild(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("plugin build", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd plugin build codex|claude [--out DIR] [--home DIR] [--force]")
		fs.PrintDefaults()
	}
	home := fs.String("home", "", "Entmoot home directory to use instead of -data")
	outDir := fs.String("out", "", "plugin package output directory")
	force := fs.Bool("force", false, "replace an existing generated package")
	provider, ok, help := parsePluginProviderArg(args, fs, "plugin build")
	if help {
		return exitOK
	}
	if !ok {
		return exitInvalidArgument
	}

	homePath := ""
	if *outDir == "" {
		resolved, err := pluginHome(gf, *home)
		if err != nil {
			fmt.Fprintf(os.Stderr, "plugin build: %v\n", err)
			return exitInvalidArgument
		}
		homePath = resolved
	}
	result, err := pluginpack.Build(pluginpack.BuildOptions{
		Provider: provider,
		Home:     homePath,
		OutDir:   *outDir,
		Force:    *force,
		Version:  version,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin build: %v\n", err)
		return exitInvalidArgument
	}
	fmt.Println(result.Path)
	return exitOK
}

func cmdPluginInstall(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("plugin install", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd plugin install codex|claude [--scope user|project|local] [--home DIR] [--force]")
		fs.PrintDefaults()
	}
	home := fs.String("home", "", "Entmoot home directory to use instead of -data")
	scope := fs.String("scope", plugininstall.DefaultScope, "Claude plugin scope: user, project, or local")
	force := fs.Bool("force", false, "replace existing generated plugin package")
	explicitScope := hasFlag(args, "scope")
	provider, ok, help := parsePluginProviderArg(args, fs, "plugin install")
	if help {
		return exitOK
	}
	if !ok {
		return exitInvalidArgument
	}

	homePath, err := pluginHome(gf, *home)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin install: %v\n", err)
		return exitInvalidArgument
	}
	result, err := plugininstall.Install(context.Background(), plugininstall.Options{
		Provider: provider,
		Home:     homePath,
		Scope:    *scope,
		Force:    *force,
		Version:  version,
		Runner:   pluginInstallRunner,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin install: %v\n", err)
		return exitInvalidArgument
	}
	fmt.Printf("package: %s\n", result.PackagePath)
	fmt.Printf("marketplace: %s\n", result.MarketplaceRoot)
	if provider == pluginpack.ProviderCodex && explicitScope {
		fmt.Println("scope: ignored for codex")
	}
	if result.RuntimeMissing {
		fmt.Printf("%s CLI was not found; generated files are ready.\n", provider)
		printPluginManualCommands(os.Stdout, result.ManualCommands)
		return exitOK
	}
	fmt.Printf("installed %s plugin\n", provider)
	return exitOK
}

func cmdPluginPath(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("plugin path", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd plugin path codex|claude [--home DIR]")
		fs.PrintDefaults()
	}
	home := fs.String("home", "", "Entmoot home directory to use instead of -data")
	provider, ok, help := parsePluginProviderArg(args, fs, "plugin path")
	if help {
		return exitOK
	}
	if !ok {
		return exitInvalidArgument
	}

	homePath, err := pluginHome(gf, *home)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin path: %v\n", err)
		return exitInvalidArgument
	}
	fmt.Println(pluginpack.DefaultPackageDir(homePath, provider))
	return exitOK
}

func cmdPluginDoctor(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("plugin doctor", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd plugin doctor [codex|claude] [--home DIR] [--json]")
		fs.PrintDefaults()
	}
	home := fs.String("home", "", "Entmoot home directory to use instead of -data")
	jsonOutput := fs.Bool("json", false, "print plugin diagnostics as JSON")
	selected, explicitRuntime, help, ok := parsePluginDoctorArgs(args, fs)
	if help {
		return exitOK
	}
	if !ok {
		return exitInvalidArgument
	}

	homePath, err := pluginHome(gf, *home)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin doctor: %v\n", err)
		return exitInvalidArgument
	}
	providers := []pluginpack.Provider{pluginpack.ProviderCodex, pluginpack.ProviderClaude}
	if explicitRuntime {
		providers = []pluginpack.Provider{selected}
	}

	output := pluginDoctorOutput{Home: homePath}
	for _, provider := range providers {
		output.Runtimes = append(output.Runtimes, doctorPluginRuntime(homePath, provider, explicitRuntime))
	}
	if *jsonOutput {
		if err := emitJSON(output); err != nil {
			fmt.Fprintf(os.Stderr, "plugin doctor: marshal: %v\n", err)
			return exitTransport
		}
	} else {
		printPluginDoctor(output)
	}

	if explicitRuntime {
		if len(output.Runtimes) == 1 && !output.Runtimes[0].Healthy {
			fmt.Fprintf(os.Stderr, "plugin doctor: %s runtime is unhealthy\n", output.Runtimes[0].Runtime)
			return exitTransport
		}
		return exitOK
	}
	for _, runtime := range output.Runtimes {
		if runtime.Healthy {
			return exitOK
		}
	}
	fmt.Fprintln(os.Stderr, "plugin doctor: no supported plugin runtime is healthy")
	return exitTransport
}

func parsePluginProviderArg(args []string, fs *flag.FlagSet, commandName string) (pluginpack.Provider, bool, bool) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "%s requires codex|claude\n", commandName)
		return "", false, false
	}
	if args[0] == "-h" || args[0] == "--help" {
		fs.Usage()
		return "", false, true
	}
	provider, err := pluginpack.ParseProvider(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", commandName, err)
		return "", false, false
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return provider, false, true
		}
		return "", false, false
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "%s does not accept extra positional arguments\n", commandName)
		return "", false, false
	}
	return provider, true, false
}

func parsePluginDoctorArgs(args []string, fs *flag.FlagSet) (pluginpack.Provider, bool, bool, bool) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		provider, err := pluginpack.ParseProvider(args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "plugin doctor: %v\n", err)
			return "", false, false, false
		}
		if err := fs.Parse(args[1:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return "", false, true, false
			}
			return "", false, false, false
		}
		if fs.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "plugin doctor does not accept extra positional arguments")
			return "", false, false, false
		}
		return provider, true, false, true
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", false, true, false
		}
		return "", false, false, false
	}
	if fs.NArg() > 1 {
		fmt.Fprintln(os.Stderr, "plugin doctor accepts at most one runtime")
		return "", false, false, false
	}
	if fs.NArg() == 0 {
		return "", false, false, true
	}
	provider, err := pluginpack.ParseProvider(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin doctor: %v\n", err)
		return "", false, false, false
	}
	return provider, true, false, true
}

func pluginHome(gf *globalFlags, override string) (string, error) {
	home := strings.TrimSpace(override)
	if home == "" {
		home = gf.data
	}
	expanded, err := expandHome(home)
	if err != nil {
		return "", err
	}
	return filepath.Clean(expanded), nil
}

func hasFlag(args []string, name string) bool {
	short := "-" + name
	long := "--" + name
	for _, arg := range args {
		if arg == short || arg == long || strings.HasPrefix(arg, short+"=") || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return false
}

func printPluginManualCommands(w *os.File, commands []string) {
	if len(commands) == 0 {
		return
	}
	fmt.Fprintln(w, "manual install commands:")
	for _, command := range commands {
		fmt.Fprintf(w, "  %s\n", command)
	}
}

func doctorPluginRuntime(home string, provider pluginpack.Provider, explicitRuntime bool) pluginDoctorRuntime {
	packagePath := pluginpack.DefaultPackageDir(home, provider)
	runtime := pluginDoctorRuntime{
		Runtime: string(provider),
		Path:    packagePath,
	}

	runtime.Checks = append(runtime.Checks, checkPluginHome(home))
	runtime.Checks = append(runtime.Checks, checkPluginCanonicalSkill())
	runtime.Checks = append(runtime.Checks, checkPluginPackage(packagePath))
	runtime.Checks = append(runtime.Checks, checkPluginManifest(packagePath, provider))
	runtime.Checks = append(runtime.Checks, checkPluginCopiedSkill(packagePath))
	runtime.Checks = append(runtime.Checks, checkPluginMarketplacePath(home, provider))
	runtime.Checks = append(runtime.Checks, checkPluginRuntimeCLI(provider, explicitRuntime))
	runtime.Checks = append(runtime.Checks, checkPluginValidationCommand(packagePath, provider, explicitRuntime))
	runtime.Healthy = pluginRuntimeChecksHealthy(runtime.Checks)
	return runtime
}

func checkPluginHome(home string) pluginCheck {
	if home == "" {
		return failPluginCheck("home", "Entmoot home is empty", true)
	}
	return okPluginCheck("home", home, true)
}

func checkPluginCanonicalSkill() pluginCheck {
	info, err := fs.Stat(skills.FS, "entmoot/SKILL.md")
	if err != nil {
		return failPluginCheck("canonical-skill", err.Error(), true)
	}
	if info.IsDir() {
		return failPluginCheck("canonical-skill", "embedded SKILL.md is a directory", true)
	}
	return okPluginCheck("canonical-skill", "embedded src/skills/entmoot/SKILL.md is available", true)
}

func checkPluginPackage(packagePath string) pluginCheck {
	info, err := os.Stat(packagePath)
	if err != nil {
		return failPluginCheck("package", packagePath, true)
	}
	if !info.IsDir() {
		return failPluginCheck("package", packagePath+" is not a directory", true)
	}
	return okPluginCheck("package", packagePath, true)
}

func checkPluginManifest(packagePath string, provider pluginpack.Provider) pluginCheck {
	manifest := pluginpack.ManifestPath(packagePath, provider)
	content, err := os.ReadFile(manifest)
	if err != nil {
		return failPluginCheck("manifest", manifest, true)
	}
	var decoded map[string]any
	if err := json.Unmarshal(content, &decoded); err != nil {
		return failPluginCheck("manifest", manifest+": "+err.Error(), true)
	}
	if decoded["name"] != pluginpack.PluginName {
		return failPluginCheck("manifest", manifest+": name must be "+pluginpack.PluginName, true)
	}
	return okPluginCheck("manifest", manifest, true)
}

func checkPluginCopiedSkill(packagePath string) pluginCheck {
	path := filepath.Join(packagePath, "skills", pluginpack.PluginName, "SKILL.md")
	info, err := os.Stat(path)
	if err != nil {
		return failPluginCheck("copied-skill", path, true)
	}
	if info.IsDir() {
		return failPluginCheck("copied-skill", path+" is a directory", true)
	}
	return okPluginCheck("copied-skill", path, true)
}

func checkPluginMarketplacePath(home string, provider pluginpack.Provider) pluginCheck {
	return okPluginCheck("marketplace-path", pluginpack.DefaultMarketplaceDir(home, provider), false)
}

func checkPluginRuntimeCLI(provider pluginpack.Provider, explicitRuntime bool) pluginCheck {
	binary := string(provider)
	path, err := pluginLookPath(binary)
	if err != nil {
		if explicitRuntime {
			return failPluginCheck("runtime-cli", binary+" was not found on PATH", true)
		}
		return warnPluginCheck("runtime-cli", binary+" was not found on PATH", false)
	}
	return okPluginCheck("runtime-cli", path, true)
}

func checkPluginValidationCommand(packagePath string, provider pluginpack.Provider, explicitRuntime bool) pluginCheck {
	switch provider {
	case pluginpack.ProviderClaude:
		if _, err := pluginLookPath("claude"); err != nil {
			return missingPluginCheck("validation-command", "claude plugin validate requires claude on PATH", explicitRuntime)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result, err := pluginValidationRunner.Run(ctx, "", "claude", "plugin", "validate", packagePath)
		if err != nil {
			detail := strings.TrimSpace(result.Stderr)
			if detail == "" {
				detail = err.Error()
			}
			return failPluginCheck("validation-command", detail, true)
		}
		return okPluginCheck("validation-command", "claude plugin validate", true)
	case pluginpack.ProviderCodex:
		return warnPluginCheck("validation-command", "codex plugin validation command is not exposed by the installed CLI", false)
	default:
		return failPluginCheck("validation-command", "unknown runtime", true)
	}
}

func missingPluginCheck(name, detail string, required bool) pluginCheck {
	if required {
		return failPluginCheck(name, detail, true)
	}
	return warnPluginCheck(name, detail, false)
}

func okPluginCheck(name, detail string, required bool) pluginCheck {
	return pluginCheck{Name: name, Status: "ok", Detail: detail, Required: required}
}

func warnPluginCheck(name, detail string, required bool) pluginCheck {
	return pluginCheck{Name: name, Status: "warn", Detail: detail, Required: required}
}

func failPluginCheck(name, detail string, required bool) pluginCheck {
	return pluginCheck{Name: name, Status: "fail", Detail: detail, Required: required}
}

func pluginRuntimeChecksHealthy(checks []pluginCheck) bool {
	for _, check := range checks {
		if check.Status == "fail" {
			return false
		}
		if check.Name == "runtime-cli" && check.Status != "ok" {
			return false
		}
	}
	return true
}

func printPluginDoctor(output pluginDoctorOutput) {
	fmt.Printf("home: %s\n", output.Home)
	for _, runtime := range output.Runtimes {
		status := "ok"
		if !runtime.Healthy {
			status = "fail"
		}
		fmt.Printf("%s: %s\n", runtime.Runtime, status)
		for _, check := range runtime.Checks {
			fmt.Printf("  %-18s %-5s %s\n", check.Name, check.Status, check.Detail)
		}
	}
}
