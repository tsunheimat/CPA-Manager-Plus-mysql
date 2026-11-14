package analyticsbench_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	monitoring "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

const sentinelCount = 16_384

type sentinelCase struct {
	days        int
	cardinality string
	groups      int
}

func routineSentinelCases() []sentinelCase {
	return []sentinelCase{{1, "low", 350}, {7, "low", 2352}, {30, "low", 2352}, {1, "independent", 7168}}
}

// An ordinary DSN integration test, not a benchmark accidentally omitted by CI.
// Duration comparisons keep N, dimensions, payload profile and zone identical;
// only the timestamp spread changes. A separate same-24h high-G control tests
// cardinality. Routine CI is bounded to these four 16,384-event fixtures, with
// count/4 outside rows plus one upper-edge row each, and 20s reader deadlines.
// The 100k payload matrix remains an opt-in benchmark, never part of this test.
// Native acceptance stays pending when no disposable MySQL DSN is provided.
func TestMySQLAnalyticsPageSentinel(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set; native page acceptance pending")
	}
	for _, tc := range routineSentinelCases() {
		t.Run(fmt.Sprintf("%dd_%s", tc.days, tc.cardinality), func(t *testing.T) {
			fixture := newPageSentinel(t, sentinelCount, tc.days, tc.cardinality, "mixed").inHongKong(t)
			if len(fixture.groups) != tc.groups {
				t.Fatalf("complete pricing G=%d want=%d", len(fixture.groups), tc.groups)
			}
			checkPageSentinel(t, fixture, true)
			if tc.days == 1 && tc.cardinality == "low" {
				checkOverviewPageProfiles(t, fixture)
			}
		})
	}
}

// Small offline contract coverage for the fixture and its independent oracle;
// this is not a stand-in for native plans or MySQL timing acceptance.
func TestAnalyticsPageSentinelOracleOffline(t *testing.T) {
	if testdb.MySQL() {
		t.Skip("small SQLite fixture/oracle check; native sentinel runs separately")
	}
	// Keep a small skewed case as coverage, not a duration timing comparison.
	for _, tc := range append(routineSentinelCases(), sentinelCase{7, "skewed", 0}) {
		t.Run(fmt.Sprintf("%dd_%s", tc.days, tc.cardinality), func(t *testing.T) {
			checkPageSentinel(t, newPageSentinel(t, 256, tc.days, tc.cardinality, "mixed"), false)
		})
	}
}

func TestAnalyticsPageSentinelCompletePricingCardinality(t *testing.T) {
	// Enumerate the full-size fixture offline, without inserting rows or allocating
	// diagnostic payloads. G includes cell, model/billing/pricing, band, tier, key
	// and provider; counting just contributors or cells would miss D3.
	cases := routineSentinelCases()
	if len(cases) != 4 || sentinelCount != 16384 {
		t.Fatal("routine native sentinel resource bound changed")
	}
	counts := make([]int, len(cases))
	for c, tc := range cases {
		const toMS int64 = 1794380110560
		fromMS := toMS - int64(tc.days)*86400000
		groups := map[sentinelGroup]struct{}{}
		for i := 0; i < sentinelCount; i++ {
			timestamp := fromMS + int64(i/8)*(toMS-fromMS)/int64((sentinelCount+7)/8)
			_, p := sentinelEvent(i, timestamp, tc.cardinality, "none")
			groups[keyForSentinel(p)] = struct{}{}
		}
		counts[c] = len(groups)
		if counts[c] != tc.groups {
			t.Fatalf("%dd/%s complete pricing G=%d want=%d", tc.days, tc.cardinality, counts[c], tc.groups)
		}
		t.Logf("offline full-size enumeration N=%d days=%d profile=%s complete_pricing_G=%d", sentinelCount, tc.days, tc.cardinality, counts[c])
	}
	if counts[0] > sentinelCount/40 || counts[3] < 20*counts[0] {
		t.Fatalf("24h low/high G no longer contrasts: low=%d high=%d N=%d", counts[0], counts[3], sentinelCount)
	}
}

// Optional extended native payload/cardinality matrix, all at seven days:
// go test -run '^$' -bench BenchmarkMySQLAnalyticsExtendedPageMatrix -benchtime=1x
// ./internal/analyticsbench. Skewed native coverage lives here, not in the fixed
// routine duration comparison. CPA_MANAGER_MONITORING_BENCH_EVENTS can bound a
// trial; the default is 100,000. No application flags or timeouts are changed.
func BenchmarkMySQLAnalyticsExtendedPageMatrix(b *testing.B) {
	if !testdb.MySQL() {
		b.Skip("native matrix requires CPA_TEST_MYSQL_DSN")
	}
	for _, cardinality := range []string{"independent", "skewed"} {
		for _, payload := range []string{"4KiB", "heavy_tail"} {
			b.Run(cardinality+"/"+payload, func(b *testing.B) {
				fixture := newPageSentinel(b, eventCount(), 7, cardinality, payload)
				checkPageSentinel(b, fixture, false)
				svc := monitoring.New(fixture.store)
				req := fixture.request(monitoring.Include{Heatmap: true, Granularity: "day"})
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := svc.Analytics(context.Background(), req); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type pageSentinel struct {
	store        *store.Store
	raw          *sql.DB
	fromMS, toMS int64
	count        int
	location     *time.Location
	groups       map[sentinelGroup]store.HeatmapPoint
}

type sentinelGroup struct {
	weekday, hour                                int
	model, billing, pricing, tier, key, provider string
	band                                         int64
}

func (f pageSentinel) request(include monitoring.Include) monitoring.Request {
	return monitoring.Request{FromMS: f.fromMS, ToMS: f.toMS, NowMS: f.toMS,
		TimeZone: f.location.String(), Include: include}
}

func (f pageSentinel) inHongKong(t testing.TB) pageSentinel {
	t.Helper()
	location, err := time.LoadLocation("Asia/Hong_Kong")
	if err != nil {
		t.Fatal(err)
	}
	// This frozen November 2026 fixture has a constant UTC+8 offset. Shift the
	// independent oracle arithmetically, not with the production calendar helper.
	for _, stamp := range []int64{f.fromMS, f.toMS} {
		if _, offset := time.UnixMilli(stamp).In(location).Zone(); offset != 8*3600 {
			t.Fatal("frozen Hong Kong fixture no longer has UTC+8 bounds")
		}
	}
	if f.location != time.UTC {
		t.Fatal("Hong Kong oracle shift requires the original UTC fixture")
	}
	groups := f.groups
	f.location, f.groups = location, map[sentinelGroup]store.HeatmapPoint{}
	for _, p := range groups {
		p.Weekday = (p.Weekday + (p.Hour+8)/24) % 7
		p.Hour = (p.Hour + 8) % 24
		f.add(p)
	}
	return f
}

func newPageSentinel(t testing.TB, count, days int, cardinality, payload string) pageSentinel {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "page-sentinel.sqlite")
	raw, err := testdb.Open(t, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	cfg := config.Config{DBDriver: "sqlite", DBPath: path}
	if testdb.MySQL() {
		cfg.DBDriver, cfg.DBDSN = "mysql", testdb.DSN(t, path)
		// A DSN alone does not opt into the production native reader.
		cfg.MySQLAnalyticsEnabled, cfg.MySQLBulkEnabled = true, true
	}
	st, err := store.OpenConfigured(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if testdb.MySQL() && !st.ConfiguredMySQLOptions().AnalyticsEnabled {
		t.Fatal("sentinel did not use the explicitly native constructor")
	}
	book := make(map[string]store.ModelPrice)
	for i := 0; i < 4; i++ {
		book[fmt.Sprintf("sentinel-model-%d", i)] = store.ModelPrice{
			Prompt: 1, Completion: 2, Cache: 3, CacheRead: 4, CacheCreation: 5,
			PromptConfigured: true, CompletionConfigured: true, CacheReadConfigured: true, CacheCreationConfigured: true,
			Source: "manual", RawJSON: `{"catalog_metadata":"` + strings.Repeat("m", 32768) + `"}`,
			ContextTiers: []store.ModelPriceContextTier{
				{ThresholdTokens: 100, Prompt: 7, CacheRead: 0, PromptConfigured: true, CacheReadConfigured: true},
				{ThresholdTokens: 200, Completion: 11, CompletionConfigured: true},
			},
			ServiceTiers: []store.ModelPriceServiceTier{{Mode: "fast", ServiceTier: "priority", Prompt: 13, Completion: 17, Cache: 29, CacheRead: 19, CacheCreation: 23,
				PromptConfigured: true, CompletionConfigured: true, CacheConfigured: true, CacheReadConfigured: true, CacheCreationConfigured: true}},
		}
	}
	// Membership is present even though all configured rates are zero.
	book["zero-billing"] = store.ModelPrice{PromptConfigured: true, CompletionConfigured: true, CacheReadConfigured: true, CacheCreationConfigured: true}
	if err := st.SaveModelPrices(ctx, book); err != nil {
		t.Fatal(err)
	}
	const toMS int64 = 1794380110560
	f := pageSentinel{store: st, raw: raw, fromMS: toMS - int64(days)*86400000, toMS: toMS, count: count, location: time.UTC, groups: map[sentinelGroup]store.HeatmapPoint{}}
	var rawBytes, bodyBytes int64
	var rawMax, bodyMax int
	for offset := 0; offset < count; offset += 200 {
		batch := make([]usage.Event, 0, min(200, count-offset))
		for i := offset; i < min(offset+200, count); i++ {
			// Eight adjacent boundary/clamping cases share contributor identity.
			// Use the same timestamp for each octet so the clamping pair cannot
			// accidentally split across hour/cell boundaries.
			timestamp := f.fromMS + int64(i/8)*(f.toMS-f.fromMS)/int64((count+7)/8)
			e, expected := sentinelEvent(i, timestamp, cardinality, payload)
			batch = append(batch, e)
			f.add(expected)
			rawBytes += int64(len(e.RawJSON))
			bodyBytes += int64(len(e.FailBody))
			rawMax, bodyMax = max(rawMax, len(e.RawJSON)), max(bodyMax, len(e.FailBody))
		}
		if _, err := st.InsertEvents(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	// Selective time bounds have real outside-window history, not an empty
	// table around the selected window. These rows must never affect answers.
	for offset := 0; offset < count/4; offset += 200 {
		batch := make([]usage.Event, 0, 200)
		for i := offset; i < min(offset+200, count/4); i++ {
			timestamp := f.fromMS - int64((i+1)*86400000)
			if i%2 != 0 {
				timestamp = f.toMS + int64(i)*3600000
			}
			e, _ := sentinelEvent(count+i, timestamp, cardinality, payload)
			batch = append(batch, e)
		}
		if _, err := st.InsertEvents(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	// Exactly on the upper bound, unlike the later history rows above. It must
	// not enter heatmap, summary, timeline or percentile populations; the first
	// octet at fromMS must enter all of them. Keep the event hash unique.
	boundary, _ := sentinelEvent(count+count/4, f.toMS, cardinality, payload)
	if _, err := st.InsertEvents(ctx, []usage.Event{boundary}); err != nil {
		t.Fatal(err)
	}
	for _, edge := range []struct{ timestamp, want int64 }{{f.fromMS, int64(min(8, count))}, {f.toMS, 1}} {
		var got int64
		if err := raw.QueryRowContext(ctx, `select count(*) from usage_events where timestamp_ms = ?`, edge.timestamp).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != edge.want {
			t.Fatalf("persisted edge %d has %d rows, want %d", edge.timestamp, got, edge.want)
		}
	}
	length := "length"
	if testdb.MySQL() {
		length = "octet_length"
	}
	var gotN, gotRaw, gotBody, gotRawMax, gotBodyMax int64
	query := fmt.Sprintf(`select count(*), coalesce(sum(%[1]s(coalesce(raw_json,''))),0), coalesce(sum(%[1]s(coalesce(fail_body,''))),0), coalesce(max(%[1]s(coalesce(raw_json,''))),0), coalesce(max(%[1]s(coalesce(fail_body,''))),0) from usage_events where timestamp_ms >= ? and timestamp_ms < ?`, length)
	if err := raw.QueryRowContext(ctx, query, f.fromMS, f.toMS).Scan(&gotN, &gotRaw, &gotBody, &gotRawMax, &gotBodyMax); err != nil {
		t.Fatal(err)
	}
	if gotN != int64(count) || gotRaw != rawBytes || gotBody != bodyBytes || gotRawMax != int64(rawMax) || gotBodyMax != int64(bodyMax) {
		t.Fatalf("persisted fixture widths/count=%d/%d/%d/%d/%d want=%d/%d/%d/%d/%d", gotN, gotRaw, gotBody, gotRawMax, gotBodyMax, count, rawBytes, bodyBytes, rawMax, bodyMax)
	}
	t.Logf("fixture engine=%s N=%d outside=%d upper_exclusive_rows=1 days=%d cardinality=%s complete_pricing_G=%d payload=%s stored_raw_bytes=%d stored_failure_bytes=%d max_raw_bytes=%d max_failure_bytes=%d; no ready-rollup assumption", cfg.DBDriver, count, count/4+1, days, cardinality, len(f.groups), payload, gotRaw, gotBody, gotRawMax, gotBodyMax)
	return f
}

func sentinelEvent(i int, timestamp int64, cardinality, payload string) (usage.Event, store.HeatmapPoint) {
	pattern, dimension := i%8, i/8
	modelIndex := dimension % 4
	name := fmt.Sprintf("sentinel-model-%d", modelIndex)
	billing, pricing := name, name
	if modelIndex == 1 {
		billing = "unpriced-backend"
	}
	if modelIndex == 2 {
		billing, pricing = "zero-billing", "zero-billing"
	}
	// Four models retain membership/fallback coverage, but one key/provider
	// makes low-G collapse to at most 14 complete pricing groups per hour-cell.
	// Independent dimensions deliberately avoid the old 4*7*3 low-G cycle.
	keyID, providerID := 0, 0
	if cardinality != "low" {
		keyID, providerID = dimension%1009, (dimension*7)%5
	}
	if cardinality == "skewed" && dimension%4 != 0 {
		keyID, providerID = 0, 0
	}
	key, provider := hash(fmt.Sprintf("sentinel-key-%d", keyID)), fmt.Sprintf("sentinel-provider-%d", providerID)
	input := []int64{100, 101, 200, 201, 272000, 272001, 0, 100}[pattern]
	cached, read, creation, output := int64(1), int64(2), int64(3), int64(9)
	tier := "priority"
	if pattern >= 6 {
		tier, cached, read, creation, output = "", 0, 0, 0, 0
		if pattern == 6 {
			cached = 100
		}
	}
	mode, rawInput := usage.CacheInputModeIncluded, input
	if pattern == 1 || pattern == 5 {
		mode, rawInput = usage.CacheInputModeSeparate, input-cached-read-creation
	}
	latency, ttft := int64(20+i%1700), int64(1+(i*19)%380)
	if i%13 == 0 {
		latency = 0
	}
	if i%17 == 0 {
		ttft = 0
	}
	e := usage.Event{EventHash: hash(fmt.Sprintf("sentinel-event-%d", i)), TimestampMS: timestamp,
		Timestamp: time.UnixMilli(timestamp).UTC().Format(time.RFC3339Nano), CreatedAtMS: timestamp,
		Model: name, RequestedModel: name + "(high)", ResolvedModel: billing,
		Provider: provider, AuthProviderSnapshot: provider, APIKeyHash: key,
		AuthIndex: fmt.Sprintf("auth-%d", dimension%41), AuthFileSnapshot: fmt.Sprintf("account-%d.json", dimension%41),
		AccountSnapshot: fmt.Sprintf("account-%d", dimension%41), Source: "sentinel", SourceHash: hash(fmt.Sprintf("source-%d", dimension%41)),
		CacheInputMode: mode, InputTokens: rawInput, CachedTokens: cached + read + creation, CacheReadTokens: read, CacheCreationTokens: creation,
		OutputTokens: output, TotalTokens: input + output, ServiceTier: tier, LatencyMS: &latency, TTFTMS: &ttft, Failed: i%11 == 0,
		Endpoint: "POST /v1/responses", Method: "POST", Path: "/v1/responses"}
	width := []int{0, 1024, 4096, 1024}[(i/8)%4]
	if payload == "4KiB" {
		width = 4096
	}
	if payload == "heavy_tail" || payload == "mixed" {
		if (i/8)%31 == 0 {
			width = 16384
		}
	}
	if payload == "none" {
		width = 0 // Full-size offline cardinality enumeration needs no wide payloads.
	}
	if width > 0 {
		e.RawJSON = `{"diagnostic":"` + strings.Repeat("d", width) + `"}`
	}
	if e.Failed {
		e.FailStatusCode = 429
		e.FailSummary = "sentinel-search retryable upstream failure"
		e.FailBody = `{"error":{"message":"` + strings.Repeat("retry detail ", max(1, width/13)) + `"}}`
	}
	band := int64(-1)
	if pricing != "zero-billing" {
		if input > 100 {
			band = 100
		}
		if input > 200 {
			band = 200
		}
	}
	local := time.UnixMilli(timestamp).UTC()
	p := store.HeatmapPoint{Weekday: int(local.Weekday()), Hour: local.Hour(), Model: name, BillingModel: billing,
		ServiceTier: tier, APIKeyHash: key, Provider: provider, Calls: 1, SuccessCalls: 1,
		InputTokens: input, OutputTokens: output, CachedTokens: cached, CacheReadTokens: read, CacheCreationTokens: creation, TotalTokens: input + output}
	p.PricingModel, p.ContextThresholdTokens = pricing, band
	if e.Failed {
		p.SuccessCalls, p.FailureCalls = 0, 1
	}
	if input > 272000 {
		p.LongInputTokens, p.LongOutputTokens, p.LongCachedTokens, p.LongCacheReadTokens, p.LongCacheCreationTokens = input, output, cached, read, creation
	}
	return e, p
}

func keyForSentinel(p store.HeatmapPoint) sentinelGroup {
	return sentinelGroup{p.Weekday, p.Hour, p.Model, p.BillingModel, p.PricingModel, p.ServiceTier, p.APIKeyHash, p.Provider, p.ContextThresholdTokens}
}

func (f pageSentinel) add(p store.HeatmapPoint) {
	key := keyForSentinel(p)
	old, ok := f.groups[key]
	if !ok {
		f.groups[key] = p
		return
	}
	old.Calls += p.Calls
	old.SuccessCalls += p.SuccessCalls
	old.FailureCalls += p.FailureCalls
	old.InputTokens += p.InputTokens
	old.OutputTokens += p.OutputTokens
	old.CachedTokens += p.CachedTokens
	old.CacheReadTokens += p.CacheReadTokens
	old.CacheCreationTokens += p.CacheCreationTokens
	old.TotalTokens += p.TotalTokens
	old.LongInputTokens += p.LongInputTokens
	old.LongOutputTokens += p.LongOutputTokens
	old.LongCachedTokens += p.LongCachedTokens
	old.LongCacheReadTokens += p.LongCacheReadTokens
	old.LongCacheCreationTokens += p.LongCacheCreationTokens
	f.groups[key] = old
}

// No production classifier or monetary helper is used by this oracle. The
// fixture rates are integral microdollars, so sum at the existing complete
// pricing/contributor grain before applying the non-distributive prompt clamp.
func sentinelMicrodollars(p store.HeatmapPoint) int64 {
	if p.PricingModel == "zero-billing" {
		return 0
	}
	prompt, completion, cache, read, creation := int64(1), int64(2), int64(3), int64(4), int64(5)
	switch p.ContextThresholdTokens {
	case 100:
		prompt, read = 7, 0
	case 200:
		completion = 11 // applies to base, never inherits band 100
	default:
		if p.ServiceTier == "priority" {
			prompt, completion, cache, read, creation = 13, 17, 29, 19, 23
		}
	}
	return max(p.InputTokens-p.CachedTokens-p.CacheReadTokens-p.CacheCreationTokens, 0)*prompt +
		p.OutputTokens*completion + p.CachedTokens*cache + p.CacheReadTokens*read + p.CacheCreationTokens*creation
}

func checkPageSentinel(t testing.TB, f pageSentinel, reference bool) {
	t.Helper()
	filter := store.AnalyticsFilter{FromMS: f.fromMS, ToMS: f.toMS, IncludeFailed: true}
	for _, native := range []bool{true, false} {
		if !native && !reference {
			continue
		}
		repo := f.store.UsageEvents
		if !native {
			repo = usageevent.New(f.raw, usageevent.WithMySQLAnalytics(false))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		start := time.Now()
		points, err := repo.HeatmapWithFilter(ctx, filter, f.location)
		elapsed := time.Since(start)
		cancel()
		if err != nil {
			t.Fatalf("heatmap native=%t: %v", native, err)
		}
		actual := make(map[sentinelGroup]store.HeatmapPoint, len(points))
		for _, p := range points {
			key := keyForSentinel(p)
			if _, ok := actual[key]; ok {
				t.Fatalf("duplicate pricing grain: %+v", key)
			}
			actual[key] = p
		}
		if !reflect.DeepEqual(actual, f.groups) {
			for key, want := range f.groups {
				if got, ok := actual[key]; !ok || got != want {
					t.Fatalf("native=%t group=%+v\ngot=%+v\nwant=%+v", native, key, got, want)
				}
			}
			t.Fatalf("native=%t groups=%d want=%d", native, len(actual), len(f.groups))
		}
		t.Logf("engine=%s native_enabled=%t N=%d timezone=%s returned_pricing_groups=%d elapsed=%s (observed, not a speedup assertion)", f.store.DatabaseDriver(), native && testdb.MySQL(), f.count, f.location, len(points), elapsed)
	}
	start := time.Now()
	result, err := monitoring.New(f.store).Analytics(context.Background(), f.request(monitoring.Include{Heatmap: true, Granularity: "day"}))
	if err != nil {
		t.Fatal(err)
	}
	want := sentinelCells(f.groups)
	if len(result.Heatmap) != len(want) {
		t.Fatalf("cells=%d want=%d", len(result.Heatmap), len(want))
	}
	for i, p := range result.Heatmap {
		w := want[i]
		if p.Weekday != w.Weekday || p.Hour != w.Hour || p.Calls != w.Calls || p.Success != w.Success || p.Failure != w.Failure || p.Tokens != w.Tokens || math.Abs(p.Cost-w.Cost) > 1e-9 || p.FailureRate != w.FailureRate {
			t.Fatalf("cell %d=%+v want=%+v", i, p, w)
		}
		for j, pair := range [][2][]monitoring.HeatmapContributor{{p.ModelContributors, w.ModelContributors}, {p.APIKeyContributors, w.APIKeyContributors}, {p.ProviderContributors, w.ProviderContributors}} {
			if len(pair[0]) != len(pair[1]) {
				t.Fatalf("cell %d dimension %d contributors=%d want=%d", i, j, len(pair[0]), len(pair[1]))
			}
			for k, got := range pair[0] {
				expected := pair[1][k]
				if math.Abs(got.Cost-expected.Cost) > 1e-9 {
					t.Fatalf("contributor cost=%v want=%v", got, expected)
				}
				got.Cost = expected.Cost
				if got != expected {
					t.Fatalf("contributor=%+v want=%+v", got, expected)
				}
			}
		}
	}
	t.Logf("actual heatmap-only service request cells=%d elapsed=%s; unchanged default budget=20s", len(result.Heatmap), time.Since(start))
}

type sentinelCell struct {
	point        monitoring.HeatmapPoint
	micro        int64
	contributors [3]map[string]monitoring.HeatmapContributor
	costs        [3]map[string]int64
}

func sentinelCells(groups map[sentinelGroup]store.HeatmapPoint) []monitoring.HeatmapPoint {
	cells := map[[2]int]*sentinelCell{}
	for _, p := range groups {
		key := [2]int{p.Weekday, p.Hour}
		c := cells[key]
		if c == nil {
			c = &sentinelCell{point: monitoring.HeatmapPoint{Weekday: p.Weekday, Hour: p.Hour}}
			for i := range c.contributors {
				c.contributors[i] = map[string]monitoring.HeatmapContributor{}
				c.costs[i] = map[string]int64{}
			}
			cells[key] = c
		}
		micro := sentinelMicrodollars(p)
		c.point.Calls += p.Calls
		c.point.Success += p.SuccessCalls
		c.point.Failure += p.FailureCalls
		c.point.Tokens += p.TotalTokens
		c.micro += micro
		for i, key := range []string{p.Model, p.APIKeyHash, p.Provider} {
			x := c.contributors[i][key]
			x.Key = key
			x.Label = key
			x.Calls += p.Calls
			x.Success += p.SuccessCalls
			x.Failure += p.FailureCalls
			x.Tokens += p.TotalTokens
			c.contributors[i][key] = x
			c.costs[i][key] += micro
		}
	}
	result := make([]monitoring.HeatmapPoint, 0, len(cells))
	for _, c := range cells {
		c.point.Cost = float64(c.micro) / 1e6
		c.point.FailureRate = float64(c.point.Failure) / float64(c.point.Calls)
		var lists [3][]monitoring.HeatmapContributor
		for i, values := range c.contributors {
			for key, x := range values {
				x.Cost = float64(c.costs[i][key]) / 1e6
				x.Share = float64(x.Calls) / float64(c.point.Calls)
				x.FailureRate = float64(x.Failure) / float64(x.Calls)
				lists[i] = append(lists[i], x)
			}
			sort.Slice(lists[i], func(a, b int) bool {
				x, y := lists[i][a], lists[i][b]
				if x.Calls != y.Calls {
					return x.Calls > y.Calls
				}
				if x.Cost != y.Cost {
					return x.Cost > y.Cost
				}
				return x.Key < y.Key
			})
			if len(lists[i]) > 5 {
				lists[i] = lists[i][:5]
			}
		}
		c.point.ModelContributors, c.point.APIKeyContributors, c.point.ProviderContributors = lists[0], lists[1], lists[2]
		result = append(result, c.point)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Weekday < result[j].Weekday || result[i].Weekday == result[j].Weekday && result[i].Hour < result[j].Hour
	})
	return result
}

type deadlineBook struct {
	modelprice.Repository
	remaining time.Duration
	calls     int
	err       error
}

func (r *deadlineBook) LoadRates(ctx context.Context) (map[string]model.ModelPrice, error) {
	r.calls++
	deadline, ok := ctx.Deadline()
	if ok {
		r.remaining = time.Until(deadline)
	}
	return nil, r.err
}

func TestAnalyticsPageDefaultBudgetAndCancellation(t *testing.T) {
	if config.DefaultAnalyticsTimeout != 20*time.Second {
		t.Fatalf("default request budget changed: %s", config.DefaultAnalyticsTimeout)
	}
	f := newPageSentinel(t, 8, 1, "low", "mixed")
	failure := errors.New("injected book read failure")
	spy := &deadlineBook{Repository: f.store.ModelPrices, err: failure}
	f.store.ModelPrices = spy
	svc := monitoring.New(f.store)
	req := f.request(monitoring.Include{Heatmap: true})
	if _, err := svc.Analytics(context.Background(), req); !errors.Is(err, failure) {
		t.Fatalf("attempted book error not terminal: %v", err)
	}
	if spy.calls != 1 || spy.remaining <= 0 || spy.remaining > 20*time.Second {
		t.Fatalf("book deadline calls=%d remaining=%s", spy.calls, spy.remaining)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := svc.Analytics(ctx, req); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if spy.remaining <= 0 || spy.remaining > time.Second {
		t.Fatalf("caller deadline enlarged: %s", spy.remaining)
	}
	cancel()
	if _, err := svc.Analytics(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if spy.calls != 2 {
		t.Fatalf("canceled request attempted a book read: %d", spy.calls)
	}
}
