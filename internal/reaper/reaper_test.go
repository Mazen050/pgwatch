package reaper

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cybertec-postgresql/pgwatch/v5/internal/cmdopts"
	"github.com/cybertec-postgresql/pgwatch/v5/internal/log"
	"github.com/cybertec-postgresql/pgwatch/v5/internal/metrics"
	"github.com/cybertec-postgresql/pgwatch/v5/internal/sinks"
	"github.com/cybertec-postgresql/pgwatch/v5/internal/sources"
	"github.com/cybertec-postgresql/pgwatch/v5/internal/testutil"
  "github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"fmt"
	"time"
)

func TestReaper_LoadSources(t *testing.T) {
	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

	t.Run("Test pause trigger file", func(t *testing.T) {
		pausefile := filepath.Join(t.TempDir(), "pausefile")
		require.NoError(t, os.WriteFile(pausefile, []byte("foo"), 0644))
		r := NewReaper(ctx, &cmdopts.Options{Metrics: metrics.CmdOpts{EmergencyPauseTriggerfile: pausefile}})
		assert.NoError(t, r.LoadSources(ctx))
		assert.True(t, len(r.monitoredSources) == 0, "Expected no monitored sources when pause trigger file exists")
	})

	t.Run("Test SyncFromReader errror", func(t *testing.T) {
		reader := &testutil.MockSourcesReaderWriter{
			GetSourcesFunc: func() (sources.Sources, error) {
				return nil, assert.AnError
			},
		}
		r := NewReaper(ctx, &cmdopts.Options{SourcesReaderWriter: reader})
		assert.Error(t, r.LoadSources(ctx))
		assert.Equal(t, 0, len(r.monitoredSources), "Expected no monitored sources after error")
	})

	t.Run("Test SyncFromReader success", func(t *testing.T) {
		source1 := sources.Source{Name: "Source 1", IsEnabled: true, Kind: sources.SourcePostgres}
		source2 := sources.Source{Name: "Source 2", IsEnabled: true, Kind: sources.SourcePostgres}
		reader := &testutil.MockSourcesReaderWriter{
			GetSourcesFunc: func() (sources.Sources, error) {
				return sources.Sources{source1, source2}, nil
			},
		}

		r := NewReaper(ctx, &cmdopts.Options{SourcesReaderWriter: reader})
		assert.NoError(t, r.LoadSources(ctx))
		assert.Equal(t, 2, len(r.monitoredSources), "Expected two monitored sources after successful load")
		assert.NotNil(t, r.monitoredSources.GetMonitoredDatabase(source1.Name))
		assert.NotNil(t, r.monitoredSources.GetMonitoredDatabase(source2.Name))
	})

	t.Run("Test repeated load", func(t *testing.T) {
		source1 := sources.Source{Name: "Source 1", IsEnabled: true, Kind: sources.SourcePostgres}
		source2 := sources.Source{Name: "Source 2", IsEnabled: true, Kind: sources.SourcePostgres}
		reader := &testutil.MockSourcesReaderWriter{
			GetSourcesFunc: func() (sources.Sources, error) {
				return sources.Sources{source1, source2}, nil
			},
		}

		r := NewReaper(ctx, &cmdopts.Options{SourcesReaderWriter: reader})
		assert.NoError(t, r.LoadSources(ctx))
		assert.Equal(t, 2, len(r.monitoredSources), "Expected two monitored sources after first load")

		// Load again with the same sources
		assert.NoError(t, r.LoadSources(ctx))
		assert.Equal(t, 2, len(r.monitoredSources), "Expected still two monitored sources after second load")
	})

	t.Run("Test group limited sources", func(t *testing.T) {
		source1 := sources.Source{Name: "Source 1", IsEnabled: true, Kind: sources.SourcePostgres, Group: ""}
		source2 := sources.Source{Name: "Source 2", IsEnabled: true, Kind: sources.SourcePostgres, Group: "group1"}
		source3 := sources.Source{Name: "Source 3", IsEnabled: true, Kind: sources.SourcePostgres, Group: "group1"}
		source4 := sources.Source{Name: "Source 4", IsEnabled: true, Kind: sources.SourcePostgres, Group: "group2"}
		source5 := sources.Source{Name: "Source 5", IsEnabled: true, Kind: sources.SourcePostgres, Group: "default"}
		newReader := &testutil.MockSourcesReaderWriter{
			GetSourcesFunc: func() (sources.Sources, error) {
				return sources.Sources{source1, source2, source3, source4, source5}, nil
			},
		}

		r := NewReaper(ctx, &cmdopts.Options{SourcesReaderWriter: newReader, Sources: sources.CmdOpts{Groups: []string{"group1", "group2"}}})
		assert.NoError(t, r.LoadSources(ctx))
		assert.Equal(t, 3, len(r.monitoredSources), "Expected three monitored sources after load")

		r = NewReaper(ctx, &cmdopts.Options{SourcesReaderWriter: newReader, Sources: sources.CmdOpts{Groups: []string{"group1"}}})
		assert.NoError(t, r.LoadSources(ctx))
		assert.Equal(t, 2, len(r.monitoredSources), "Expected two monitored source after group filtering")

		r = NewReaper(ctx, &cmdopts.Options{SourcesReaderWriter: newReader})
		assert.NoError(t, r.LoadSources(ctx))
		assert.Equal(t, 5, len(r.monitoredSources), "Expected five monitored sources after resetting groups")
	})

	t.Run("Test source config changes trigger restart", func(t *testing.T) {
		baseSource := sources.Source{
			Name:                 "TestSource",
			IsEnabled:            true,
			Kind:                 sources.SourcePostgres,
			ConnStr:              "postgres://localhost:5432/testdb",
			Metrics:              map[string]float64{"cpu": 10, "memory": 20},
			MetricsStandby:       map[string]float64{"cpu": 30},
			CustomTags:           map[string]string{"env": "test"},
			Group:                "default",
		}

		testCases := []struct {
			name         string
			modifySource func(s *sources.Source)
			expectCancel bool
		}{
			{
				name: "custom tags change",
				modifySource: func(s *sources.Source) {
					s.CustomTags = map[string]string{"env": "production"}
				},
				expectCancel: true,
			},
			{
				name: "custom tags add new tag",
				modifySource: func(s *sources.Source) {
					s.CustomTags = map[string]string{"env": "test", "region": "us-east"}
				},
				expectCancel: true,
			},
			{
				name: "custom tags remove tag",
				modifySource: func(s *sources.Source) {
					s.CustomTags = map[string]string{}
				},
				expectCancel: true,
			},
			{
				name: "preset metrics change",
				modifySource: func(s *sources.Source) {
					s.PresetMetrics = "exhaustive"
				},
				expectCancel: true,
			},
			{
				name: "preset standby metrics change",
				modifySource: func(s *sources.Source) {
					s.PresetMetricsStandby = "standby-preset"
				},
				expectCancel: true,
			},
			{
				name: "connection string change",
				modifySource: func(s *sources.Source) {
					s.ConnStr = "postgres://localhost:5433/newdb"
				},
				expectCancel: true,
			},
			{
				name: "custom metrics change interval",
				modifySource: func(s *sources.Source) {
					s.Metrics = map[string]float64{"cpu": 15, "memory": 20}
				},
				expectCancel: true,
			},
			{
				name: "custom metrics add new metric",
				modifySource: func(s *sources.Source) {
					s.Metrics = map[string]float64{"cpu": 10, "memory": 20, "disk": 30}
				},
				expectCancel: true,
			},
			{
				name: "custom metrics remove metric",
				modifySource: func(s *sources.Source) {
					s.Metrics = map[string]float64{"cpu": 10}
				},
				expectCancel: true,
			},
			{
				name: "standby metrics change",
				modifySource: func(s *sources.Source) {
					s.MetricsStandby = map[string]float64{"cpu": 60}
				},
				expectCancel: true,
			},
			{
				name: "group change",
				modifySource: func(s *sources.Source) {
					s.Group = "new-group"
				},
				expectCancel: true,
			},
			{
				name: "kind change",
				modifySource: func(s *sources.Source) {
					s.Kind = sources.SourcePgBouncer
				},
				expectCancel: true,
			},
			{
				name: "only if master change",
				modifySource: func(s *sources.Source) {
					s.OnlyIfMaster = true
				},
				expectCancel: true,
			},
			{
				name: "no change - same config",
				modifySource: func(_ *sources.Source) {
					// No modifications - source stays the same
				},
				expectCancel: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				initialSource := *baseSource.Clone()
				initialReader := &testutil.MockSourcesReaderWriter{
					GetSourcesFunc: func() (sources.Sources, error) {
						return sources.Sources{initialSource}, nil
					},
				}

				r := NewReaper(ctx, &cmdopts.Options{
					SourcesReaderWriter: initialReader,
					SinksWriter:         &sinks.MultiWriter{},
				})
				assert.NoError(t, r.LoadSources(ctx))
				assert.Equal(t, 1, len(r.monitoredSources), "Expected one monitored source after initial load")

				mockConn, err := pgxmock.NewPool()
				require.NoError(t, err)
				mockConn.ExpectClose()
				r.monitoredSources[0].Conn = mockConn

				// Add a mock cancel function for a metric gatherer
				cancelCalled := make(map[string]bool)
				for metric := range initialSource.Metrics {
					dbMetric := initialSource.Name + "¤¤¤" + metric
					r.cancelFuncs[dbMetric] = func() {
						cancelCalled[dbMetric] = true
					}
				}

				// Create modified source
				modifiedSource := *baseSource.Clone()
				tc.modifySource(&modifiedSource)

				modifiedReader := &testutil.MockSourcesReaderWriter{
					GetSourcesFunc: func() (sources.Sources, error) {
						return sources.Sources{modifiedSource}, nil
					},
				}
				r.SourcesReaderWriter = modifiedReader

				// Reload sources
				assert.NoError(t, r.LoadSources(ctx))
				assert.Equal(t, 1, len(r.monitoredSources), "Expected one monitored source after reload")
				assert.Equal(t, modifiedSource, r.monitoredSources[0].Source)

				for metric := range initialSource.Metrics {
					dbMetric := initialSource.Name + "¤¤¤" + metric
					assert.Equal(t, tc.expectCancel, cancelCalled[dbMetric])
					if tc.expectCancel {
						assert.Nil(t, mockConn.ExpectationsWereMet(), "Expected all mock expectations to be met")
						_, exists := r.cancelFuncs[dbMetric]
						assert.False(t, exists, "Expected cancel func to be removed from map after cancellation")
					}
				}
			})
		}
	})

	t.Run("Test only changed source cancelled in multi-source setup", func(t *testing.T) {
		source1 := sources.Source{
			Name:      "Source1",
			IsEnabled: true,
			Kind:      sources.SourcePostgres,
			ConnStr:   "postgres://localhost:5432/db1",
			Metrics:   map[string]float64{"cpu": 10},
		}
		source2 := sources.Source{
			Name:      "Source2",
			IsEnabled: true,
			Kind:      sources.SourcePostgres,
			ConnStr:   "postgres://localhost:5432/db2",
			Metrics:   map[string]float64{"memory": 20},
		}

		initialReader := &testutil.MockSourcesReaderWriter{
			GetSourcesFunc: func() (sources.Sources, error) {
				return sources.Sources{source1, source2}, nil
			},
		}

		r := NewReaper(ctx, &cmdopts.Options{
			SourcesReaderWriter: initialReader,
			SinksWriter:         &sinks.MultiWriter{},
		})
		assert.NoError(t, r.LoadSources(ctx))

		// Set mock connections for both sources to avoid nil pointer on Close()
		mockConn1, err := pgxmock.NewPool()
		require.NoError(t, err)
		mockConn1.ExpectClose()
		r.monitoredSources[0].Conn = mockConn1

		source1Cancelled := false
		source2Cancelled := false
		r.cancelFuncs[source1.Name+"¤¤¤"+"cpu"] = func() { source1Cancelled = true }
		r.cancelFuncs[source2.Name+"¤¤¤"+"memory"] = func() { source2Cancelled = true }

		// Only modify source1
		modifiedSource1 := *source1.Clone()
		modifiedSource1.ConnStr = "postgres://localhost:5433/db1_new"

		modifiedReader := &testutil.MockSourcesReaderWriter{
			GetSourcesFunc: func() (sources.Sources, error) {
				return sources.Sources{modifiedSource1, source2}, nil
			},
		}
		r.SourcesReaderWriter = modifiedReader

		assert.NoError(t, r.LoadSources(ctx))

		assert.True(t, source1Cancelled, "Source1 should be cancelled due to config change")
		assert.False(t, source2Cancelled, "Source2 should NOT be cancelled as it was not modified")
		assert.Nil(t, mockConn1.ExpectationsWereMet(), "Expected all mock expectations to be met")
	})
}

// func TestReaper_LoadSources_PauseClearsExisting(t *testing.T) {
// 	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

// 	source := sources.Source{Name: "db1", IsEnabled: true, Kind: sources.SourcePostgres}
// 	reader := &testutil.MockSourcesReaderWriter{
// 		GetSourcesFunc: func() (sources.Sources, error) {
// 			return sources.Sources{source}, nil
// 		},
// 	}

// 	pausefile := filepath.Join(t.TempDir(), "pausefile")
// 	r := NewReaper(ctx, &cmdopts.Options{
// 		SourcesReaderWriter: reader,
// 		Metrics:             metrics.CmdOpts{EmergencyPauseTriggerfile: pausefile},
// 	})

// 	// initial load
// 	require.NoError(t, r.LoadSources(ctx))
// 	require.Len(t, r.monitoredSources, 1)

// 	// enable pause
// 	require.NoError(t, os.WriteFile(pausefile, []byte("1"), 0644))

// 	require.NoError(t, r.LoadSources(ctx))
// 	assert.Len(t, r.monitoredSources, 0)
// }


// func TestReaper_LoadSources_DisabledSourcesIgnored(t *testing.T) {
// 	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

// 	enabled := sources.Source{Name: "enabled", IsEnabled: true, Kind: sources.SourcePostgres}
// 	disabled := sources.Source{Name: "disabled", IsEnabled: false, Kind: sources.SourcePostgres}

// 	reader := &testutil.MockSourcesReaderWriter{
// 		GetSourcesFunc: func() (sources.Sources, error) {
// 			return sources.Sources{enabled, disabled}, nil
// 		},
// 	}

// 	r := NewReaper(ctx, &cmdopts.Options{SourcesReaderWriter: reader})
// 	require.NoError(t, r.LoadSources(ctx))

// 	assert.Len(t, r.monitoredSources, 1)
// 	assert.NotNil(t, r.monitoredSources.GetMonitoredDatabase("enabled"))
// 	assert.Nil(t, r.monitoredSources.GetMonitoredDatabase("disabled"))
// }

// func TestReaper_LoadSources_GroupAndEnabledCombined(t *testing.T) {
// 	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

// 	sourcesList := sources.Sources{
// 		{Name: "a", IsEnabled: true, Group: "g1", Kind: sources.SourcePostgres},
// 		{Name: "b", IsEnabled: false, Group: "g1", Kind: sources.SourcePostgres},
// 		{Name: "c", IsEnabled: true, Group: "g2", Kind: sources.SourcePostgres},
// 	}

// 	reader := &testutil.MockSourcesReaderWriter{
// 		GetSourcesFunc: func() (sources.Sources, error) {
// 			return sourcesList, nil
// 		},
// 	}

// 	r := NewReaper(ctx, &cmdopts.Options{
// 		SourcesReaderWriter: reader,
// 		Sources:             sources.CmdOpts{Groups: []string{"g1"}},
// 	})

// 	require.NoError(t, r.LoadSources(ctx))
// 	assert.Len(t, r.monitoredSources, 1)
// 	assert.Equal(t, "a", r.monitoredSources[0].Name)
// }


// func TestReaper_ShutdownOldWorkers_MetricRemovedFromPreset(t *testing.T) {
// 	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

// 	src := sources.Source{
// 		Name:    "db1",
// 		Kind:    sources.SourcePostgres,
// 		Metrics: map[string]float64{"cpu": 10},
// 	}

// 	r := NewReaper(ctx, &cmdopts.Options{
// 		SinksWriter: &sinks.MultiWriter{},
// 	})

// 	md := sources.NewSourceConn(src)
// 	r.monitoredSources = sources.SourceConns{md}

// 	cancelled := false
// 	r.cancelFuncs["db1¤¤¤cpu"] = func() { cancelled = true }

// 	// Remove metric
// 	md.Metrics = map[string]float64{}

// 	r.ShutdownOldWorkers(ctx, nil)

// 	assert.True(t, cancelled)
// 	_, exists := r.cancelFuncs["db1¤¤¤cpu"]
// 	assert.False(t, exists)
// }


// func TestReaper_ShutdownOldWorkers_WholeDBShutdown(t *testing.T) {
// 	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

// 	r := NewReaper(ctx, &cmdopts.Options{
// 		SinksWriter: &sinks.MultiWriter{},
// 	})

// 	cancel1 := false
// 	cancel2 := false

// 	r.cancelFuncs["db1¤¤¤cpu"] = func() { cancel1 = true }
// 	r.cancelFuncs["db1¤¤¤memory"] = func() { cancel2 = true }

// 	r.ShutdownOldWorkers(ctx, map[string]bool{"db1": true})

// 	assert.True(t, cancel1)
// 	assert.True(t, cancel2)
// 	assert.Len(t, r.cancelFuncs, 0)
// }


// // func TestReaper_FetchMetric_PrimaryOnlySkippedOnStandby(t *testing.T) {
// // 	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

// // 	r := NewReaper(ctx, &cmdopts.Options{})

// // 	md := &sources.SourceConn{
// // 		Source: sources.Source{Name: "db1"},
// // 		IsInRecovery: true,
// // 	}

// // 	metricDefs.SetMetricDef(metrics.Metric{
// // 		Name:       "primary_metric",
// // 		Scope:      metrics.ScopeInstance,
// // 		OnlyMaster: true,
// // 	})

// // 	env, err := r.FetchMetric(ctx, md, "primary_metric")
// // 	assert.NoError(t, err)
// // 	assert.Nil(t, env)
// // }


// // func TestReaper_FetchMetric_UsesInstanceCache(t *testing.T) {
// // 	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())

// // 	r := NewReaper(ctx, &cmdopts.Options{
// // 		Metrics: metrics.CmdOpts{InstanceLevelCacheMaxSeconds: 60},
// // 	})

// // 	md := &sources.SourceConn{
// // 		Source: sources.Source{Name: "db1"},
// // 	}

// // 	metricDefs.SetMetricDef(metrics.Metric{
// // 		Name:           "cached_metric",
// // 		IsInstanceLevel: true,
// // 	})

// // 	// Prime cache
// // 	r.measurementCache.Put("db1:cached_metric", metrics.Measurements{
// // 		metrics.NewMeasurement(time.Now().UnixNano()),
// // 	})

// // 	env, err := r.FetchMetric(ctx, md, "cached_metric")
// // 	assert.NoError(t, err)
// // 	assert.NotNil(t, env)
// // }


func TestReaper_Ready(t *testing.T) {
	ctx := context.Background()
	r := NewReaper(ctx, &cmdopts.Options{})

	assert.False(t, r.Ready())

	r.ready.Store(true)
	assert.True(t, r.Ready())
}
func TestReaper_PrintMemStats(t *testing.T) {
	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())
	r := NewReaper(ctx, &cmdopts.Options{})

	assert.NotPanics(t, func() {
		r.PrintMemStats()
	})
}
// func TestReaper_WriteMeasurements_ContextCancel(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	r := NewReaper(ctx, &cmdopts.Options{
// 		SinksWriter: &sinks.MultiWriter{},
// 	})

// 	go r.WriteMeasurements(ctx)
// 	cancel()

// 	time.Sleep(10 * time.Millisecond)
// }
func TestReaper_CreateSourceHelpers_EarlyReturn(t *testing.T) {
	ctx := log.WithLogger(context.Background(), log.NewNoopLogger())
	r := NewReaper(ctx, &cmdopts.Options{})

	md := &sources.SourceConn{
		Source: sources.Source{
			Name: "db1",
			Kind: sources.SourcePgBouncer, // non-postgres
		},
	}

	r.CreateSourceHelpers(ctx, r.logger, md)
}















// --- Mocks ---

// MockSinkWriter simulates the Sinks interface
// MockSinkWriter simulates the Sinks interface
type MockSinkWriter struct {
	WriteCalled  bool
	LastMsg      metrics.MeasurementEnvelope
	SyncCalled   bool
	DeleteCalled bool
}

func (m *MockSinkWriter) Write(msg metrics.MeasurementEnvelope) error {
	m.WriteCalled = true
	m.LastMsg = msg
	return nil
}

// FIX: Change 'op int' to 'op sinks.SyncOp'
func (m *MockSinkWriter) SyncMetric(dbUnique, metricName string, op sinks.SyncOp) error {
	m.SyncCalled = true
	return nil
}

// --- Tests ---

func TestWriteMeasurements(t *testing.T) {
	// Setup
	mockSink := &MockSinkWriter{}
	opts := &cmdopts.Options{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := NewReaper(ctx, opts)
	r.SinksWriter = mockSink // Inject Mock

	// 1. Start the worker in a goroutine
	go r.WriteMeasurements(ctx)

	// 2. Send a dummy message
	dummyMsg := metrics.MeasurementEnvelope{
		DBName:     "test_db",
		MetricName: "test_metric",
		Data:       metrics.Measurements{{"value": 1}},
	}
	r.measurementCh <- dummyMsg

	// 3. Allow brief time for channel processing
	time.Sleep(50 * time.Millisecond)

	// 4. Assertions
	assert.True(t, mockSink.WriteCalled, "Sink Write should have been called")
	assert.Equal(t, "test_db", mockSink.LastMsg.DBName)
	assert.Equal(t, "test_metric", mockSink.LastMsg.MetricName)
}

func TestWriteMonitoredSources(t *testing.T) {
	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	r := NewReaper(ctx, &cmdopts.Options{})
	
	// Add a fake monitored source
	r.monitoredSources = append(r.monitoredSources, &sources.SourceConn{
		Source: sources.Source{
			Name:         "test_db_source",
			Group:        "test_group",
			OnlyIfMaster: true,
			CustomTags: map[string]string{"env": "prod"},
		},
	})

	// 1. Start the worker
	go r.WriteMonitoredSources(ctx)

	// 2. Listen to the channel to verify output
	select {
	case msg := <-r.measurementCh:
		// 3. Assertions
		assert.Equal(t, "test_db_source", msg.DBName)
		assert.Equal(t, monitoredDbsDatastoreSyncMetricName, msg.MetricName)
		
		assert.NotEmpty(t, msg.Data)
		row := msg.Data[0]
		
		assert.Equal(t, "test_group", row["tag_group"])
		assert.Equal(t, true, row["master_only"])
		assert.Equal(t, "prod", row["tag_env"]) // checking custom tag prefix
		
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for WriteMonitoredSources to produce data")
	}
}

func TestAddSysinfoToMeasurements(t *testing.T) {
	// Setup
	opts := &cmdopts.Options{}
	opts.Sinks.RealDbnameField = "real_dbname"
	opts.Sinks.SystemIdentifierField = "sys_id"

	r := NewReaper(context.Background(), opts)

	// FIX 1: Assign fields via dot notation to handle embedded structs safely
	md := &sources.SourceConn{}
	md.RealDbname = "postgres_prod"
	md.SystemIdentifier = "123456789"

	data := metrics.Measurements{
		{"value": 10},
		{"value": 20},
	}

	// 1. Execute
	r.AddSysinfoToMeasurements(data, md)

	// 2. Assertions
	for _, row := range data {
		assert.Equal(t, "postgres_prod", row["real_dbname"])
		assert.Equal(t, "123456789", row["sys_id"])
	}

	assert.Equal(t, 10, data[0]["value"])
	assert.Equal(t, 20, data[1]["value"])
}

func TestFetchMetric_CacheHit(t *testing.T) {
	// Setup
	ctx := context.Background()
	opts := &cmdopts.Options{}
	opts.Metrics.InstanceLevelCacheMaxSeconds = 60

	r := NewReaper(ctx, opts)
	metricName := "cached_metric"

	// FIX: Create struct first, then assign promoted fields
	md := &sources.SourceConn{
		Source: sources.Source{
			Name: "db1",
		},
	}
	md.SystemIdentifier = "sys_id_123"            // Set promoted field safely
	md.Metrics = map[string]float64{metricName: 10} // Set map safely

	// Setup Metric Definition
	metricDefs.Lock()
	metricDefs.MetricDefs[metricName] = metrics.Metric{
		SQLs:            map[int]string{0: "SELECT 1"},
		IsInstanceLevel: true,
	}
	metricDefs.Unlock()

	// Pre-populate Cache
	// We generate the key using the exact method the app uses
	// (usually "ClusterIdentifier:MetricName")
	cacheKey := fmt.Sprintf("%s:%s", md.GetClusterIdentifier(), metricName)
	cachedData := metrics.Measurements{{"cached_val": 999}}

	r.measurementCache.Put(cacheKey, cachedData)

	// Execute
	envelope, err := r.FetchMetric(ctx, md, metricName)

	// Assertions
	assert.NoError(t, err)
	
	// If envelope is nil, it means cache MISS (and it swallowed the error/returned nil)
	if envelope == nil {
		t.Fatal("Cache Miss! FetchMetric returned nil envelope. Check GetClusterIdentifier() logic.")
	}

	assert.Equal(t, metricName, envelope.MetricName)
	assert.Equal(t, 999, envelope.Data[0]["cached_val"])
}

func TestFetchMetric_NotFound(t *testing.T) {
	ctx := context.Background()
	r := NewReaper(ctx, &cmdopts.Options{})
	md := &sources.SourceConn{Source: sources.Source{Name: "db1"}}

	// 1. Execute with non-existent metric
	envelope, err := r.FetchMetric(ctx, md, "ghost_metric")

	// 2. Assertions
	assert.Error(t, err)
	assert.Equal(t, metrics.ErrMetricNotFound, err)
	assert.Nil(t, envelope)
}

func TestFetchMetric_EmptySQL_Ignored(t *testing.T) {
	ctx := context.Background()
	r := NewReaper(ctx, &cmdopts.Options{})
	md := &sources.SourceConn{Source: sources.Source{Name: "db1"}}

	metricName := "empty_sql_metric"

	// FIX 3: Initialize with empty map or specific empty version
	metricDefs.Lock()
	metricDefs.MetricDefs[metricName] = metrics.Metric{
		SQLs: map[int]string{}, // Empty map simulates no SQL found
	}
	metricDefs.Unlock()

	// 2. Execute
	envelope, err := r.FetchMetric(ctx, md, metricName)

	// 3. Assertions
	assert.NoError(t, err)
	assert.Nil(t, envelope)
}