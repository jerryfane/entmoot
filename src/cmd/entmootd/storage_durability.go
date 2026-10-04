package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// dataStorageWarning reports when the data root sits on storage that does not
// keep Entmoot state across an environment restart: a memory-backed
// filesystem, or an overlay whose writable layer is mounted fsync=volatile.
// SQLite durability rests on fsync, which such an overlay skips, and platforms
// that use one may discard or roll back the writable layer when the
// environment restarts. Membership can then survive from an older layer while
// the message store, written later, disappears. It returns "" when the mount
// cannot be determined or is not one of these.
func dataStorageWarning(dataDir string) string {
	path, err := filepath.Abs(dataDir)
	if err != nil {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	mount, ok := mountContaining(filepath.Join(procRoot, "self", "mountinfo"), path)
	if !ok {
		return ""
	}
	switch mount.fsType {
	case "tmpfs", "ramfs":
		return fmt.Sprintf("data directory %s is on memory-backed %s mounted at %s: all Entmoot state, including stored messages, is lost when the environment restarts; use durable storage or keep an external backup", path, mount.fsType, mount.point)
	case "overlay":
		if slices.Contains(mount.options, "volatile") || slices.Contains(mount.options, "fsync=volatile") {
			return fmt.Sprintf("data directory %s is on an overlay mounted fsync=volatile at %s: fsync is skipped and the platform may discard or roll back the writable layer on restart, losing stored messages while older identity and membership files survive; use durable storage or keep an external backup", path, mount.point)
		}
	}
	return ""
}

type mountEntry struct {
	point   string
	fsType  string
	options []string
}

// mountContaining returns the mountinfo entry with the longest mount point
// that contains path. Later entries win ties, matching stacked mounts.
func mountContaining(mountinfo, path string) (mountEntry, bool) {
	f, err := os.Open(mountinfo)
	if err != nil {
		return mountEntry{}, false
	}
	defer f.Close()
	var best mountEntry
	found := false
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		entry, ok := parseMountinfoLine(scanner.Text())
		if !ok || !pathWithin(path, entry.point) {
			continue
		}
		if !found || len(entry.point) >= len(best.point) {
			best, found = entry, true
		}
	}
	return best, found
}

// parseMountinfoLine parses one proc(5) mountinfo line:
// id parent major:minor root point mount-options [optional...] - fstype source super-options
func parseMountinfoLine(line string) (mountEntry, bool) {
	fields := strings.Fields(line)
	separator := -1
	for i := 6; i < len(fields); i++ {
		if fields[i] == "-" {
			separator = i
			break
		}
	}
	if separator < 0 || separator+3 > len(fields) {
		return mountEntry{}, false
	}
	point := unescapeMountField(fields[4])
	options := strings.Split(fields[5], ",")
	options = append(options, strings.Split(fields[separator+3], ",")...)
	return mountEntry{point: point, fsType: fields[separator+1], options: options}, true
}

// unescapeMountField decodes the octal escapes the kernel uses for space, tab,
// newline and backslash in mountinfo paths.
func unescapeMountField(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var b strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] == '\\' && i+3 < len(field) {
			if value, err := strconv.ParseUint(field[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(value))
				i += 3
				continue
			}
		}
		b.WriteByte(field[i])
	}
	return b.String()
}

func pathWithin(path, point string) bool {
	if point == "/" {
		return strings.HasPrefix(path, "/")
	}
	return path == point || strings.HasPrefix(path, point+"/")
}
