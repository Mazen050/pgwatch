package reaper

import (
	"runtime"
	"slices"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v5/internal/metrics"

	"maps"
	"os"

	"path/filepath"

	"github.com/stretchr/testify/assert"
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
	if runtime.GOOS != "linux" {
		t.Skip("GetPathUnderlyingDeviceID is not implemented")
	}
	a := assert.New(t)
	tmpDir := t.TempDir()
	createFile := func(name string) string {
		f, err := os.CreateTemp(tmpDir, name)
		a.NoError(err)
		defer f.Close()
		return f.Name()
	}

	file1Path := createFile("file1")
	devID1, err := GetPathUnderlyingDeviceID(file1Path)
	a.NoError(err)

	file2Path := createFile("file2")
	devID2, err := GetPathUnderlyingDeviceID(file2Path)
	a.NoError(err)
	a.Equal(devID1, devID2)
}

func TestGetPathUnderlyingDeviceID_NotFound(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("GetPathUnderlyingDeviceID is not implemented")
	}
	_, err := GetPathUnderlyingDeviceID("/this/path/should/not/exist/nofile")
	assert.Error(t, err)
}

func TestGetGoPsutilDiskPG(t *testing.T) {
	rootDir := t.TempDir()
	
	dataDir := filepath.Join(rootDir, "data")
	logDirRel := "log"
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

	// Prepare Input maps
	dataDirs := []map[string]any{
		{
			"dd": dataDir,
			"ld": logDirRel,
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

	// Should at least return the Data Directory metrics
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
