package nodestorage

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const diskGenFileName = ".diskgen"

// DiskGen marks a storage root with a generation id. The marker is created the
// first time a node starts on the given storage root; its absence on a node
// that expects to hold data means the disk was replaced (or wiped) and the
// node must treat all its content as missing rather than deleted.
type DiskGen struct {
	// GenId is a random id generated when the storage root was initialized.
	GenId string `json:"genId"`
	// CreatedTime is when the storage root was initialized.
	CreatedTime time.Time `json:"createdTime"`
	// fresh is true if the marker was created by this process start.
	fresh bool
}

// Fresh reports whether the storage root was initialized on this start,
// i.e. no marker existed before (new node or replaced/wiped disk).
func (d DiskGen) Fresh() bool {
	return d.fresh
}

func loadOrCreateDiskGen(rootPath string) (gen DiskGen, err error) {
	path := filepath.Join(rootPath, diskGenFileName)
	data, readErr := os.ReadFile(path)
	switch {
	case readErr == nil:
		if jsonErr := json.Unmarshal(data, &gen); jsonErr == nil && gen.GenId != "" {
			return gen, nil
		}
		// corrupted marker: regenerate below, but this is not a fresh disk
	case os.IsNotExist(readErr):
		gen.fresh = true
	default:
		return gen, readErr
	}
	var buf [16]byte
	if _, err = rand.Read(buf[:]); err != nil {
		return
	}
	gen.GenId = hex.EncodeToString(buf[:])
	gen.CreatedTime = time.Now()
	if data, err = json.Marshal(gen); err != nil {
		return
	}
	err = os.WriteFile(path, data, 0o644)
	return
}
