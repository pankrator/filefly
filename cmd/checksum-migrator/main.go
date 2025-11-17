package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	storageDir := flag.String("storage_dir", "", "path to the directory that stores block files")
	force := flag.Bool("force", false, "overwrite checksum files when they already exist")

	flag.Parse()

	if *storageDir == "" {
		log.Fatal("--storage_dir is required")
	}

	info, err := os.Stat(*storageDir)
	if err != nil {
		log.Fatalf("stat storage_dir: %v", err)
	}

	if !info.IsDir() {
		log.Fatalf("storage_dir must be a directory: %s", *storageDir)
	}

	stats, err := migrateChecksums(*storageDir, *force)
	if err != nil {
		log.Fatalf("generate checksums: %v", err)
	}

	log.Printf("processed %d block files, wrote %d new checksum files (%d skipped)",
		stats.totalBlocks, stats.generatedChecksums, stats.skippedExisting)
}

type migrationStats struct {
	totalBlocks        int
	generatedChecksums int
	skippedExisting    int
}

func migrateChecksums(storageDir string, force bool) (migrationStats, error) {
	var stats migrationStats

	err := filepath.WalkDir(storageDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			if path == storageDir {
				return nil
			}

			return nil
		}

		if strings.HasSuffix(d.Name(), ".crc") {
			return nil
		}

		stats.totalBlocks++
		checksumPath := path + ".crc"

		if !force {
			if _, err := os.Stat(checksumPath); err == nil {
				stats.skippedExisting++
				return nil
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("stat checksum %s: %w", checksumPath, err)
			}
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read block %s: %w", path, err)
		}

		checksum := crc32.ChecksumIEEE(data)

		if err := writeChecksum(checksumPath, checksum); err != nil {
			return fmt.Errorf("write checksum for %s: %w", path, err)
		}

		stats.generatedChecksums++

		log.Printf("wrote checksum for %s", path)

		return nil
	})

	return stats, err
}

func writeChecksum(path string, checksum uint32) error {
	buf := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(buf, checksum)

	return os.WriteFile(path, buf, 0o600)
}
