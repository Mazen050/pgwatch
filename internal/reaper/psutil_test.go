package reaper

import (
	"slices"
	"testing"
	
	// "strings"
	"os"

	"github.com/cybertec-postgresql/pgwatch/v5/internal/metrics"

	"maps"

	"github.com/stretchr/testify/assert"

	"path/filepath"
	"runtime"

)

func TestGetGoPsutilCPU(t *testing.T) {
	a := assert.New(t)

	// Call the GetGoPsutilCPU function with a 1-second interval
	result, err := GetGoPsutilCPU(1.0)
	a.NoError(err)
	a.NotEmpty(result)

	// Check if the result contains the expected keys
	expectedKeys := []string{metrics.EpochColumnName, "cpu_utilization", "load_1m_norm", "load_1m", "load_5m_norm", "load_5m", "user", "system", "idle", "iowait", "irqs", "other"}
	resultKeys := slices.Collect(maps.Keys(result[0]))
	a.ElementsMatch(resultKeys, expectedKeys)

	// Check if the CPU utilization is within the expected range
	cpuUtilization := result[0]["cpu_utilization"].(float64)
	a.GreaterOrEqual(cpuUtilization, 0.0)
	a.LessOrEqual(cpuUtilization, 100.0)
}

func TestGetGoPsutilMem(t *testing.T) {
	a := assert.New(t)

	// Call the GetGoPsutilMem function
	result, err := GetGoPsutilMem()
	a.NoError(err)
	a.NotEmpty(result)

	// Check if the result contains the expected keys
	expectedKeys := []string{metrics.EpochColumnName, "total", "used", "free", "buff_cache", "available", "percent", "swap_total", "swap_used", "swap_free", "swap_percent"}
	resultKeys := slices.Collect(maps.Keys(result[0]))
	a.ElementsMatch(resultKeys, expectedKeys)
}

func TestGetGoPsutilDiskTotals(t *testing.T) {
	a := assert.New(t)

	// Call the GetGoPsutilDiskTotals function
	result, err := GetGoPsutilDiskTotals()
	if err != nil {
		t.Skip("skipping test; disk.IOCounters() failed")
	}
	a.NotEmpty(result)

	// Check if the result contains the expected keys
	expectedKeys := []string{metrics.EpochColumnName, "read_bytes", "write_bytes", "read_count", "write_count"}
	resultKeys := slices.Collect(maps.Keys(result[0]))
	a.ElementsMatch(resultKeys, expectedKeys)
}

func TestGetLoadAvgLocal(t *testing.T) {
	a := assert.New(t)

	// Call the GetLoadAvgLocal function
	result, err := GetLoadAvgLocal()
	a.NoError(err)
	a.NotEmpty(result)

	// Check if the result contains the expected keys
	expectedKeys := []string{metrics.EpochColumnName, "load_1min", "load_5min", "load_15min"}
	resultKeys := slices.Collect(maps.Keys(result[0]))
	a.ElementsMatch(resultKeys, expectedKeys)
}


// --- Test for CheckFolderExistsAndReadable ---

func TestCheckFolderExistsAndReadable(t *testing.T) {
	// 1. Valid Directory (Happy Path)
	tmpDir := t.TempDir()
	if !CheckFolderExistsAndReadable(tmpDir) {
		t.Errorf("Expected valid folder %s to be readable", tmpDir)
	}

	// 2. Non-existent Directory
	nonExistent := filepath.Join(tmpDir, "ghost_folder")
	if CheckFolderExistsAndReadable(nonExistent) {
		t.Error("Expected non-existent folder to return false")
	}

	// 3. File (not a directory)
	// ReadDir fails on files, so this should return false
	tmpFile := filepath.Join(tmpDir, "somefile.txt")
	f, _ := os.Create(tmpFile)
	f.Close()
	if CheckFolderExistsAndReadable(tmpFile) {
		t.Error("Expected file path to return false (not a folder)")
	}

	// 4. Unreadable Directory (Permissions)
	// Skip on Windows as chmod behavior is different
	if runtime.GOOS != "windows" {
		unreadable := filepath.Join(tmpDir, "no_access")
		os.Mkdir(unreadable, 0000) // No permissions
		// Restore permissions after test so `go test` can clean up
		defer os.Chmod(unreadable, 0755)

		// Note: Root user often bypasses permissions. If running as root (e.g., in docker), this might pass.
		if os.Geteuid() != 0 {
			if CheckFolderExistsAndReadable(unreadable) {
				t.Error("Expected 0000 permission folder to be unreadable")
			}
		}
	}
}








// --- Test for GetGoPsutilDiskPG ---

func TestGetGoPsutilDiskPG(t *testing.T) {
	// Setup a fake Postgres directory structure
	rootDir := t.TempDir()
	
	// Paths
	dataDir := filepath.Join(rootDir, "data")
	logDirRel := "log"                  // Relative path to test path joining logic
	logDirAbs := filepath.Join(dataDir, logDirRel)
	walDir := filepath.Join(dataDir, "pg_wal")
	tsDir := filepath.Join(rootDir, "tablespace_1")

	// Create directories
	dirs := []string{dataDir, logDirAbs, walDir, tsDir}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatal(err)
		}
	}

	// Prepare Input Maps
	dataDirs := []map[string]any{
		{
			"dd": dataDir,
			"ld": logDirRel, // Test relative path logic
		},
	}

	tblspaceDirs := []map[string]any{
		{
			"name":     "ts1",
			"location": tsDir,
		},
	}

	// Run the function
	results, err := GetGoPsutilDiskPG(dataDirs, tblspaceDirs)
	if err != nil {
		t.Fatalf("GetGoPsutilDiskPG returned error: %v", err)
	}

	// --- Assertions ---
	
	// 1. Should at least return the Data Directory ("dd") metrics
	if len(results) == 0 {
		t.Fatal("Expected results, got 0 rows")
	}

	foundDataDir := false
	for _, row := range results {
		// Basic validation of fields
		if _, ok := row["total"]; !ok {
			t.Error("Row missing 'total' field")
		}
		if _, ok := row["percent"]; !ok {
			t.Error("Row missing 'percent' field")
		}

		// Check tag matches
		tag := row["tag_dir_or_tablespace"]
		if tag == "data_directory" {
			foundDataDir = true
			if row["tag_path"] != dataDir {
				t.Errorf("Expected data_dir path %s, got %s", dataDir, row["tag_path"])
			}
		}
	}

	if !foundDataDir {
		t.Error("Did not find result for 'data_directory'")
	}
}

func TestGetGoPsutilDiskPG_EdgeCases(t *testing.T) {
	rootDir := t.TempDir()

	// Setup basic valid paths
	dataDir := filepath.Join(rootDir, "data_edge")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Case 1: Missing WAL Directory
	// We do NOT create the "pg_wal" folder inside dataDir.
	// The function should detect this and simply not report on WAL, rather than erroring.
	t.Run("Missing WAL Directory", func(t *testing.T) {
		dataDirs := []map[string]any{
			{"dd": dataDir, "ld": ""},
		}
		results, err := GetGoPsutilDiskPG(dataDirs, nil)
		if err != nil {
			t.Fatalf("Did not expect error for missing WAL dir, got: %v", err)
		}

		// Verify pg_wal tag is NOT present
		for _, row := range results {
			if row["tag_dir_or_tablespace"] == "pg_wal" {
				t.Error("Reported pg_wal even though directory does not exist")
			}
		}
	})

	// Case 2: Tablespace on Same Device (Deduplication)
	// We create a tablespace folder on the same partition.
	// Since it shares the same Device ID as dataDir, the function SHOULD skip it.
	t.Run("Tablespace Deduplication", func(t *testing.T) {
		tsDir := filepath.Join(rootDir, "ts_same_device")
		if err := os.MkdirAll(tsDir, 0755); err != nil {
			t.Fatal(err)
		}

		dataDirs := []map[string]any{
			{"dd": dataDir, "ld": ""},
		}
		tblspaceDirs := []map[string]any{
			{"name": "ts_duplicate", "location": tsDir},
		}

		results, err := GetGoPsutilDiskPG(dataDirs, tblspaceDirs)
		if err != nil {
			t.Fatal(err)
		}

		// We expect exactly 1 row (Data Directory). 
		// The Tablespace should be skipped because tsDevice == ddDevice.
		if len(results) != 1 {
			t.Errorf("Expected 1 row (deduplicated), got %d rows. (Note: This test assumes /tmp is a single partition)", len(results))
		}

		if results[0]["tag_dir_or_tablespace"] == "ts_duplicate" {
			t.Error("Tablespace should have been skipped due to being on the same device")
		}
	})

	// Case 3: Invalid Tablespace Path
	// This ensures the function returns an error if a configured tablespace path is unreadable/missing.
	t.Run("Invalid Tablespace Path", func(t *testing.T) {
		dataDirs := []map[string]any{
			{"dd": dataDir, "ld": ""},
		}
		tblspaceDirs := []map[string]any{
			{"name": "ts_missing", "location": "/path/that/does/not/exist/99999"},
		}

		_, err := GetGoPsutilDiskPG(dataDirs, tblspaceDirs)
		if err == nil {
			t.Error("Expected error for non-existent tablespace path, got nil")
		}
	})

	// Case 4: Absolute Log Path
	// Tests logic: if !strings.HasPrefix(logDirPath, "/")
	t.Run("Absolute Log Path", func(t *testing.T) {
		absLogDir := filepath.Join(rootDir, "abs_log")
		if err := os.MkdirAll(absLogDir, 0755); err != nil {
			t.Fatal(err)
		}

		dataDirs := []map[string]any{
			{"dd": dataDir, "ld": absLogDir}, // Passing absolute path
		}

		results, err := GetGoPsutilDiskPG(dataDirs, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Even if deduplicated, we just want to make sure the function didn't crash 
		// trying to Join an absolute path with the data dir.
		if len(results) == 0 {
			t.Fatal("Expected results, got none")
		}
	})
}