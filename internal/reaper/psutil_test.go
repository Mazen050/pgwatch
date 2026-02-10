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


func TestGetPathUnderlyingDeviceID(t *testing.T) {
	// Create a temporary directory for our test files.
	tmpDir := t.TempDir()

	// Helper function to create a dummy file
	createFile := func(name string) string {
		f, err := os.CreateTemp(tmpDir, name)
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		defer f.Close()
		return f.Name()
	}

	// 1. Happy Path: Test with a valid file
	file1Path := createFile("file1")
	devID1, err := GetPathUnderlyingDeviceID(file1Path)
	if err != nil {
		t.Fatalf("GetPathUnderlyingDeviceID failed: %v", err)
	}

	// 2. Consistency Check
	// Create a second file in the same temporary directory. 
	// Files in the same folder should generally have the same underlying Device ID.
	file2Path := createFile("file2")
	devID2, err := GetPathUnderlyingDeviceID(file2Path)
	if err != nil {
		t.Fatalf("GetPathUnderlyingDeviceID failed on second file: %v", err)
	}

	if devID1 != devID2 {
		t.Errorf("Expected files in same directory to have same DeviceID, got %d and %d", devID1, devID2)
	}
}

func TestGetPathUnderlyingDeviceID_NotFound(t *testing.T) {
	_, err := GetPathUnderlyingDeviceID("/this/path/should/not/exist/hopefully")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

// func TestGetPathUnderlyingDeviceID_LeakCheck(t *testing.T) {
// 	// 1. Create a dummy file
// 	f, err := os.CreateTemp("", "leak_test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer os.Remove(f.Name())
// 	f.Close()

// 	// 2. Count open files before the loop
// 	initialFDs, err := os.ReadDir("/proc/self/fd")
// 	if err != nil {
// 		t.Skip("Skipping test: cannot read /proc/self/fd (are you on Linux?)")
// 	}
// 	initialCount := len(initialFDs)

// 	// 3. Run the function 100 times
// 	for i := 0; i < 100; i++ {
// 		_, err := GetPathUnderlyingDeviceID(f.Name())
// 		if err != nil {
// 			t.Fatalf("Function failed: %v", err)
// 		}
// 	}

// 	// 4. Count open files after the loop
// 	finalFDs, err := os.ReadDir("/proc/self/fd")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	finalCount := len(finalFDs)

// 	// 5. Calculate the leak and log results
// 	diff := finalCount - initialCount

// 	t.Logf("Initial FDs: %d, Final FDs: %d, Leaked: %d", initialCount, finalCount, diff)

// 	if diff >= 100 {
// 		t.Errorf("FAIL: Resource leak detected! Leaked %d file descriptors in 100 iterations.", diff)
// 	} else {
// 		t.Log("PASS: No significant leak detected.")
// 	}
// }



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