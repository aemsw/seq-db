package seq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestChainWord(t *testing.T) {
	for _, chain := range [][]codecID{
		nil,
		{codecZstd},
		{codecFrontEncode, codecZstd},
		{codecDeltaBitpack, codecZstd, codecFrontEncode, codecZstd},
	} {
		require.Equal(t, chain, decodeChainWord(encodeChainWord(chain)))
	}
	require.Empty(t, decodeChainWord(0))
}

func requireColumnRows(t *testing.T, col Column[[]byte], want ...string) {
	t.Helper()
	var got []string
	it := col.NewIter()
	rows := 0
	for it.Next() {
		got = append(got, string(it.Value()))
		rows++
	}
	require.False(t, it.Next(), "iteration must stay exhausted")
	require.Equal(t, col.Len(), rows, "iterator row count matches Len()")
	require.Equal(t, want, got)
}

func buildStringColumn(t *testing.T, values ...string) *StringColumn {
	t.Helper()
	col := NewStringColumn(len(values))
	for _, v := range values {
		col.AppendString(v)
	}
	requireColumnRows(t, col, values...)
	return col
}

func multiSegmentStringValues(segments int) []string {
	var values []string
	total := 0
	for i := 0; total <= segments*columnSegmentSize; i++ {
		v := fmt.Sprintf("row-%06d-%s", i, bytes.Repeat([]byte("x"), 92))
		values = append(values, v)
		total += len(v)
	}
	return values
}

func checkStringColumnChains(t *testing.T, values []string) {
	t.Helper()
	raw := buildStringColumn(t, values...)
	for _, chain := range [][]codecID{
		{codecZstd},
		{codecFrontEncode, codecZstd},
	} {
		col := raw.Compress(chain...)
		requireColumnRows(t, col, values...)

		decoded, err := UnpackStringColumn(col.AppendTo(nil))
		require.NoError(t, err)
		requireColumnRows(t, decoded, values...)
	}
}

func TestStringColumnIteration(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	randomValues := func(rows int) []string {
		values := make([]string, rows)
		for i := range values {
			n := rng.Intn(64)
			if rng.Intn(10) == 0 {
				n = rng.Intn(40 * 1024)
			}
			buf := make([]byte, n)
			rng.Read(buf)
			values[i] = string(buf)
		}
		return values
	}
	tests := []struct {
		name   string
		values []string
	}{
		{"empty", nil},
		{"single row", []string{"a"}},
		{"empty rows", []string{"", "", ""}},
		{"mixed empty", []string{"a", "", "abc", ""}},
		{"duplicates", []string{"b", "a", "a", "b", "a"}},
		{"unicode", []string{"héllo wörld", "привет", "日本語", ""}},
		{"multi segment", multiSegmentStringValues(2)},
	}
	for _, rows := range []int{1, 2, 127, 128, 129, 1000, 5000} {
		tests = append(tests, struct {
			name   string
			values []string
		}{fmt.Sprintf("random rows=%d", rows), randomValues(rows)})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkStringColumnChains(t, tt.values)
		})
	}
}

func TestStringColumnSegmentedLayout(t *testing.T) {
	values := multiSegmentStringValues(3)
	col := buildStringColumn(t, values...).Segment().Compress(codecZstd)
	segmented := col.(*segmentedColumn[[]byte])
	require.GreaterOrEqual(t, len(segmented.segments), 3)

	it := col.NewIter().(*segmentedIter[[]byte])
	rows := 0
	for i := 0; it.Next(); i++ {
		require.Equal(t, values[i], string(it.Value()))
		rows++
	}
	require.Equal(t, len(values), rows)
	require.Equal(t, len(segmented.segments), it.segIdx, "all segments must be visited in order")
}

func TestStringColumnIteratorsAreIndependent(t *testing.T) {
	values := multiSegmentStringValues(2)
	col := buildStringColumn(t, values...).Segment().Compress(codecFrontEncode, codecZstd)
	a := col.NewIter()
	b := col.NewIter()
	require.True(t, a.Next())
	require.True(t, b.Next())
	i := 0
	for {
		require.Equal(t, values[i], string(a.Value()))
		require.Equal(t, values[i], string(b.Value()))
		aNext, bNext := a.Next(), b.Next()
		require.Equal(t, aNext, bNext)
		if !aNext {
			break
		}
		i++
	}
	require.Equal(t, len(values)-1, i)
}

func TestSegmentThenCompressComposition(t *testing.T) {
	values := multiSegmentStringValues(2)
	raw := buildStringColumn(t, values...)

	segmented := raw.Segment().(*segmentedColumn[[]byte])
	require.GreaterOrEqual(t, len(segmented.segments), 2)
	for i := range segmented.segments {
		require.IsType(t, &StringColumn{}, segmented.segments[i], "segments must be raw before compression")
	}

	compressed := segmented.Compress(codecFrontEncode, codecZstd).(*segmentedColumn[[]byte])
	require.Len(t, compressed.segments, len(segmented.segments))
	for i := range compressed.segments {
		require.IsType(t, &compressedColumn[[]byte]{}, compressed.segments[i])
	}
	requireColumnRows(t, compressed, values...)

	decoded, err := UnpackStringColumn(compressed.AppendTo(nil))
	require.NoError(t, err)
	requireColumnRows(t, decoded, values...)

	compressedDirect := raw.Compress(codecZstd)
	require.IsType(t, &compressedColumn[[]byte]{}, compressedDirect, "plain Compress must yield a compressed column")
	requireColumnRows(t, compressedDirect, values...)
}

func TestSegmentedColumnMixesRawAndCompressedSegments(t *testing.T) {
	values := multiSegmentStringValues(2)
	raw := buildStringColumn(t, values...)

	segmented := raw.Segment().Compress(codecZstd).(*segmentedColumn[[]byte])
	rawSegment := NewStringColumn(0)
	it := raw.NewIter()
	for range segmented.segments[0].Len() {
		require.True(t, it.Next())
		rawSegment.Append(it.Value())
	}
	segmented.segments[0] = rawSegment

	requireColumnRows(t, segmented, values...)

	decoded, err := UnpackStringColumn(segmented.AppendTo(nil))
	require.NoError(t, err)
	requireColumnRows(t, decoded, values...)
}

func TestStringColumnSegmentedAppendPanics(t *testing.T) {
	col := buildStringColumn(t, "a").Compress(codecZstd)
	require.Panics(t, func() { col.Append([]byte("x")) })
}

func TestStringColumnCompressIsIdempotent(t *testing.T) {
	col := buildStringColumn(t, "a", "b", "c")
	compressed := col.Compress(codecFrontEncode, codecZstd)
	before := compressed.AppendTo(nil)
	again := compressed.Compress(codecFrontEncode, codecZstd)
	require.Equal(t, before, again.AppendTo(nil))
	requireColumnRows(t, again, "a", "b", "c")
}

func TestStringColumnEmptyEncoding(t *testing.T) {
	col := NewStringColumn(0)
	encoded := col.AppendTo(nil)
	require.Len(t, encoded, columnHeaderSize)

	decoded, err := UnpackStringColumn(encoded)
	require.NoError(t, err)
	require.Zero(t, decoded.Len())

	compressed := col.Compress(codecZstd)
	require.Zero(t, compressed.Len())
	requireColumnRows(t, compressed)

	decodedCompressed, err := UnpackStringColumn(compressed.AppendTo(nil))
	require.NoError(t, err)
	require.Zero(t, decodedCompressed.Len())
}

func TestUnpackStringColumnErrors(t *testing.T) {
	valid := buildStringColumn(t, "a", "b", "c").AppendTo(nil)

	tests := []struct {
		name string
		data []byte
		err  string
	}{
		{"empty", nil, "not enough data"},
		{"short header", valid[:10], "not enough data"},
		{"bad magic", append([]byte{0x00}, valid[1:]...), "invalid column magic"},
		{"bad version", append([]byte{valid[0], 0x00}, valid[2:]...), "unsupported column format version"},
		{"bad layout", append([]byte{valid[0], valid[1], 0x7f}, valid[3:]...), "unsupported column layout"},
		{"truncated offsets", valid[:columnHeaderSize+sizeOfUint32], "truncated string column offsets"},
		{"truncated data", valid[:len(valid)-1], "data has"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnpackStringColumn(tt.data)
			require.ErrorContains(t, err, tt.err)
		})
	}

	corrupt := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint64(corrupt[8:16], math.MaxUint64)
	_, err := UnpackStringColumn(corrupt)
	require.ErrorContains(t, err, "too many rows")

	corrupt = append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(corrupt[columnHeaderSize+sizeOfUint32:], 100)
	_, err = UnpackStringColumn(corrupt)
	require.ErrorContains(t, err, "not monotonic")

	corrupt = append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(corrupt[columnHeaderSize+3*sizeOfUint32:], 100)
	_, err = UnpackStringColumn(corrupt)
	require.ErrorContains(t, err, "data has")
}

func TestUnmarshalSegmentedStringColumnErrors(t *testing.T) {
	valid := buildStringColumn(t, "a", "b", "c").Segment().Compress(codecZstd).AppendTo(nil)

	tests := []struct {
		name string
		data []byte
		err  string
	}{
		{"truncated segment count", valid[:columnHeaderSize+2], "truncated segment count"},
		{"truncated segment header", valid[:columnHeaderSize+sizeOfUint32], "truncated segment 0"},
		{"truncated segment payload", func() []byte {
			corrupt := append([]byte(nil), valid...)
			return corrupt[:len(corrupt)-1]
		}(), "segment 0"},
		{"rows mismatch", func() []byte {
			corrupt := append([]byte(nil), valid...)
			binary.LittleEndian.PutUint64(corrupt[8:16], 42)
			return corrupt
		}(), "do not match column rows"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnpackStringColumn(tt.data)
			require.ErrorContains(t, err, tt.err)
		})
	}

	corrupt := append([]byte(nil), valid...)
	_, n := binary.Uvarint(corrupt[columnHeaderSize+sizeOfUint32:])
	chainAt := columnHeaderSize + sizeOfUint32 + n + columnHeaderSize
	binary.LittleEndian.PutUint32(corrupt[chainAt:], 0x7f7f7f7f)
	column, err := UnpackStringColumn(corrupt)
	require.NoError(t, err)
	require.Panics(t, func() { column.NewIter().Next() }, "unknown codec must fail on iteration")
}

func TestUnmarshalSegmentedColumnAliasesInput(t *testing.T) {
	encoded := buildStringColumn(t, "aaa", "bbbb").Segment().Compress(codecZstd).AppendTo(nil)
	column, err := UnpackStringColumn(encoded)
	require.NoError(t, err)
	segmented := column.(*segmentedColumn[[]byte])
	segment := segmented.segments[0].(*compressedColumn[[]byte])

	_, n := binary.Uvarint(encoded[columnHeaderSize+sizeOfUint32:])
	blobStart := columnHeaderSize + sizeOfUint32 + n
	payloadLen, payloadLenBytes := binary.Uvarint(encoded[blobStart+columnHeaderSize+sizeOfUint32:])
	payloadStart := blobStart + columnHeaderSize + sizeOfUint32 + payloadLenBytes
	require.Equal(t, len(segment.payload), int(payloadLen))
	require.Same(t, &encoded[payloadStart], &segment.payload[0])
}

func TestStringColumnCompressionRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("compression report is skipped in short mode")
	}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xFF, (i>>8)&0xFF, i&0xFF)
	}
	raw := 0
	for _, v := range values {
		raw += len(v)
	}

	col := buildStringColumn(t, values...)
	encoded := col.Compress(codecFrontEncode, codecZstd).AppendTo(nil)
	t.Logf("rows=%d raw=%d encoded=%d ratio=%.2f", len(values), raw, len(encoded), float64(raw)/float64(len(encoded)))
	require.Less(t, len(encoded), raw)
}

func requireInt64Values(t *testing.T, col Column[int64], want ...int64) {
	t.Helper()
	got := make([]int64, 0, len(want))
	it := col.NewIter()
	rows := 0
	for it.Next() {
		got = append(got, it.Value())
		rows++
	}
	require.False(t, it.Next(), "iteration must stay exhausted")
	require.Equal(t, col.Len(), rows, "iterator count matches Len()")
	if len(want) == 0 {
		require.Empty(t, got)
		return
	}
	require.Equal(t, want, got)
}

func int64Column(values ...int64) *Int64Column {
	col := NewInt64Column(len(values))
	for _, v := range values {
		col.Append(v)
	}
	return col
}

func checkInt64ColumnChains(t *testing.T, values []int64) {
	t.Helper()
	raw := int64Column(values...)
	requireInt64Values(t, raw, values...)
	for _, chain := range [][]codecID{
		{codecDeltaBitpack},
		{codecZstd},
	} {
		col := raw.Compress(chain...)
		requireInt64Values(t, col, values...)

		decoded, err := UnpackInt64Column(col.AppendTo(nil))
		require.NoError(t, err)
		requireInt64Values(t, decoded, values...)
	}
}

func genInt64Values(n int) []int64 {
	rng := rand.New(rand.NewSource(9))
	values := make([]int64, n)
	for i := range values {
		switch rng.Intn(4) {
		case 0:
			values[i] = int64(rng.Int63n(1 << 20))
		case 1:
			values[i] = -int64(rng.Int63n(1 << 20))
		case 2:
			values[i] = 0
		default:
			values[i] = int64(rng.Int63n(100))
		}
	}
	return values
}

func TestInt64ColumnIteration(t *testing.T) {
	for _, rows := range []int{0, 1, 2, 127, 128, 129, 1000, 5000} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			checkInt64ColumnChains(t, genInt64Values(rows))
		})
	}
}

func TestNewEmptyInt64Column(t *testing.T) {
	column := NewEmptyInt64Column()
	require.Zero(t, column.Len())
	require.Equal(t, int(unsafe.Sizeof(Int64Column{})), column.MemUsage())
	require.False(t, column.NewIter().Next())

	encoded := column.AppendTo(nil)
	require.Len(t, encoded, columnHeaderSize)
	decoded, err := UnpackInt64Column(encoded)
	require.NoError(t, err)
	requireInt64Values(t, decoded)

	allocations := testing.AllocsPerRun(100, func() {
		encoded = column.AppendTo(encoded[:0])
	})
	require.Zero(t, allocations)
}

func TestUnpackInt64ColumnErrors(t *testing.T) {
	valid := int64Column(1, 2, 3).AppendTo(nil)

	_, err := UnpackInt64Column(nil)
	require.ErrorContains(t, err, "not enough data")
	_, err = UnpackInt64Column([]byte{0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.ErrorContains(t, err, "magic")
	_, err = UnpackInt64Column([]byte{int64ColumnMagic, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.ErrorContains(t, err, "unsupported column format version")
	_, err = UnpackInt64Column([]byte{int64ColumnMagic, 0x01, 0x7f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	require.ErrorContains(t, err, "unsupported column layout")

	corrupt := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint64(corrupt[8:16], 42)
	_, err = UnpackInt64Column(corrupt)
	require.ErrorContains(t, err, "malformed int64 column data")

	_, err = UnpackInt64Column(valid[:len(valid)-1])
	require.ErrorContains(t, err, "malformed int64 column data")
}

func TestInt64ColumnCompressionRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("compression report is skipped in short mode")
	}
	rng := rand.New(rand.NewSource(7))
	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(1 + rng.Intn(1<<14))
	}
	col := int64Column(values...).Compress(codecDeltaBitpack)
	encoded := col.AppendTo(nil)
	t.Logf("values=%d raw=%d encoded=%d ratio=%.2f", len(values), 8*len(values), len(encoded),
		float64(8*len(values))/float64(len(encoded)))
	require.Less(t, len(encoded), 8*len(values))
}

func TestInt64ColumnDescendingMIDsCompressionRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("compression report is skipped in short mode")
	}
	const rows = 100_000
	col := NewInt64Column(rows)
	for i := range rows {
		bucket := (rows - 1 - i) / 1000
		col.Append(int64(MID(uint64(bucket) * 60_000_000_000)))
	}

	encoded := col.Compress(codecDeltaBitpack).AppendTo(nil)
	t.Logf("values=%d raw=%d encoded=%d ratio=%.2f", rows, 8*rows, len(encoded),
		float64(8*rows)/float64(len(encoded)))
	require.Less(t, len(encoded), 8*rows/20)
}

func requireFloat64Values(t *testing.T, column Column[float64], want ...float64) {
	t.Helper()
	require.Equal(t, len(want), column.Len(), "column count matches expected values")

	it := column.NewIter()
	got := make([]float64, 0, len(want))
	for it.Next() {
		got = append(got, it.Value())
	}
	require.Len(t, got, len(want))
	for i := range want {
		if math.IsNaN(want[i]) {
			require.True(t, math.IsNaN(got[i]), "value %d must be NaN", i)
			continue
		}
		require.Equal(t, want[i], got[i], "value %d", i)
	}
}

func TestFloat64ColumnChains(t *testing.T) {
	want := []float64{1, -2, math.NaN(), math.Inf(-1), math.Inf(1), 0}
	raw := NewFloat64Column(len(want))
	for _, value := range want {
		raw.Append(value)
	}
	requireFloat64Values(t, raw, want...)

	col := raw.Compress(codecZstd)
	requireFloat64Values(t, col, want...)

	decoded, err := UnpackFloat64Column(col.AppendTo(nil))
	require.NoError(t, err)
	requireFloat64Values(t, decoded, want...)

	empty := NewFloat64Column(0).Compress(codecZstd)
	require.Zero(t, empty.Len())
	decodedEmpty, err := UnpackFloat64Column(empty.AppendTo(nil))
	require.NoError(t, err)
	require.Zero(t, decodedEmpty.Len())
}

func TestUnpackFloat64ColumnErrors(t *testing.T) {
	valid := NewFloat64Column(2)
	valid.Append(1)
	valid.Append(2)
	encoded := valid.AppendTo(nil)

	tests := []struct {
		name string
		data []byte
		err  string
	}{
		{name: "empty", data: nil, err: "not enough data"},
		{name: "bad magic", data: append([]byte{0}, encoded[1:]...), err: "invalid column magic"},
		{
			name: "bad version",
			data: append([]byte{float64ColumnMagic, 0}, encoded[2:]...),
			err:  "unsupported column format version",
		},
		{
			name: "truncated values",
			data: encoded[:len(encoded)-1],
			err:  "malformed float64 column data",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnpackFloat64Column(tt.data)
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestArraysColumnChains(t *testing.T) {
	quantiles := NewArraysColumn[float64](2)
	quantiles.Append([]float64{1})
	quantiles.Append([]float64{-2, 3})
	requireRowsEqual := func(col Column[[]float64], want ...[]float64) {
		it := col.NewIter()
		for _, row := range want {
			require.True(t, it.Next())
			require.Equal(t, row, it.Value())
		}
		require.False(t, it.Next())
	}

	requireRowsEqual(quantiles, []float64{1}, []float64{-2, 3})
	requireRowsEqual(quantiles.Compress(codecZstd), []float64{1}, []float64{-2, 3})

	decoded, err := UnpackArraysColumn[float64](quantiles.Compress(codecZstd).AppendTo(nil))
	require.NoError(t, err)
	requireRowsEqual(decoded, []float64{1}, []float64{-2, 3})

	empty := NewArraysColumn[uint32](0).Compress(codecZstd)
	require.Zero(t, empty.Len())
	decodedEmpty, err := UnpackArraysColumn[uint32](empty.AppendTo(nil))
	require.NoError(t, err)
	require.Zero(t, decodedEmpty.Len())
}

func TestColumnMemUsageTracksRetainedCapacity(t *testing.T) {
	rawString := NewStringColumn(0)
	rawString.data = make([]byte, 0, 64)
	rawString.offsets = make([]uint32, 0, 10)
	require.Equal(t,
		int(unsafe.Sizeof(*rawString))+cap(rawString.data)+sizeOfUint32*cap(rawString.offsets),
		rawString.MemUsage(),
	)

	intColumn := int64Column(1, 2)
	require.Equal(t,
		int(unsafe.Sizeof(Int64Column{}))+cap(intColumn.values)*sizeOfUint64,
		intColumn.MemUsage(),
	)

	compressed := buildStringColumn(t, "a", "b").Compress(codecZstd).(*compressedColumn[[]byte])
	require.Equal(t,
		int(unsafe.Sizeof(*compressed))+cap(compressed.payload)+len(compressed.chain),
		compressed.MemUsage())

	segmentedString := buildStringColumn(t, "a", "b").Segment().Compress(codecZstd).(*segmentedColumn[[]byte])
	var segment Column[[]byte]
	total := int(unsafe.Sizeof(*segmentedString)) + cap(segmentedString.segments)*int(unsafe.Sizeof(segment))
	for i := range segmentedString.segments {
		total += segmentedString.segments[i].MemUsage()
	}
	require.Equal(t, total, segmentedString.MemUsage())
}

func TestEmptyAggregatableSamplesPerFunc(t *testing.T) {
	for _, fn := range []AggFunc{
		AggFuncCount, AggFuncSum, AggFuncMin, AggFuncMax,
		AggFuncAvg, AggFuncQuantile, AggFuncUnique, AggFuncUniqueCount,
	} {
		agg := NewAggregatableSamples(fn, 1, 0)
		require.Len(t, agg.Tokens, 1)
		require.NotNil(t, agg.Values.Totals)
		require.NotNil(t, agg.Values.NotExists)

		result := agg.Aggregate(AggregateArgs{Func: fn, Quantiles: []float64{0.5}})
		require.Emptyf(t, result.Buckets, "func %d", fn)
		agg.MemUsage()
	}
}
