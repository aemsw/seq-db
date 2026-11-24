package seq

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

// Column Used only for legacy engine QPR, do not use anywhere else.
// TODO(cheb0) delete this when query engine is complete
type Column[T any] interface {
	Len() int
	Append(value T)
	MemUsage() int
	AppendTo(dst []byte) []byte
	NewIter() Iter[T]
	Segment() Column[T]
	Compress(codecs ...codecID) Column[T]
}

type Iter[T any] interface {
	Next() bool
	Value() T
}

type codecID byte

const (
	codecZstd         codecID = 1
	codecFrontEncode  codecID = 2
	codecDeltaBitpack codecID = 3
)

const maxCodecsInChain = 4

func encodeChainWord(chain []codecID) uint32 {
	var word uint32
	for i, codec := range chain {
		word |= uint32(codec) << (i * 8)
	}
	return word
}

func decodeChainWord(word uint32) []codecID {
	var chain []codecID
	for range maxCodecsInChain {
		codec := codecID(word & 0xff)
		if codec == 0 {
			break
		}
		chain = append(chain, codec)
		word >>= 8
	}
	return chain
}

func validateChain(chain []codecID, allowed ...codecID) {
codec:
	for _, codec := range chain {
		for _, a := range allowed {
			if codec == a {
				continue codec
			}
		}
		panic(fmt.Errorf("unsupported column codec %d", codec))
	}
}

func encodeWith(codec codecID, payload []byte, rows int) ([]byte, error) {
	switch codec {
	case codecZstd:
		return zstd.CompressLevel(payload, nil, columnZstdLevel), nil
	case codecFrontEncode:
		return frontEncodePayload(payload, rows)
	case codecDeltaBitpack:
		return bitpackEncodePayload(payload, rows)
	default:
		return nil, fmt.Errorf("unsupported column codec %d", codec)
	}
}

func decodeWith(codec codecID, payload []byte, rows int) ([]byte, error) {
	switch codec {
	case codecZstd:
		return zstd.Decompress(payload, nil)
	case codecFrontEncode:
		return frontDecodePayload(payload, rows)
	case codecDeltaBitpack:
		return bitpackDecodePayload(payload, rows)
	default:
		return nil, fmt.Errorf("unsupported column codec %d", codec)
	}
}

const (
	columnFormatVersion byte = 1
	columnHeaderSize         = 16
	sizeOfUint32             = 4
	sizeOfUint64             = 8
	sizeOfFloat64            = 8
	columnSegmentSize        = 1024 * 1024
	columnZstdLevel          = 3
)

const (
	stringColumnMagic  byte = 0x43
	int64ColumnMagic   byte = 0x49
	float64ColumnMagic byte = 0x46
	arraysColumnMagic  byte = 0x41
)

const (
	columnLayoutFlat       byte = 0
	columnLayoutSegmented  byte = 1
	columnLayoutCompressed byte = 2
)

// newCompressedColumn applies the codec chain to a flat payload of one column
// kind and returns a compressed column bound to the kind decode function.
func newCompressedColumn[T any](magic byte, payload []byte, rows int, codecs []codecID, decode func(int, []byte) (Column[T], error)) *compressedColumn[T] {
	for _, codec := range codecs {
		encoded, err := encodeWith(codec, payload, rows)
		if err != nil {
			panic(fmt.Errorf("failed to compress column with codec %d: %w", codec, err))
		}
		payload = encoded
	}
	return &compressedColumn[T]{
		magic:   magic,
		payload: payload,
		chain:   append([]codecID(nil), codecs...),
		rows:    rows,
		decode:  decode,
	}
}

// compressedColumn is a column payload encoded with (possibly) multiple codecs
type compressedColumn[T any] struct {
	magic   byte
	payload []byte
	chain   []codecID
	rows    int
	decode  func(rows int, payload []byte) (Column[T], error)
}

func (c *compressedColumn[T]) Len() int {
	return c.rows
}

func (c *compressedColumn[T]) Append(value T) {
	panic("append to a compressed column is not supported")
}

func (c *compressedColumn[T]) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.payload) + len(c.chain)
}

func (c *compressedColumn[T]) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, c.magic, c.rows, columnLayoutCompressed)
	dst = binary.LittleEndian.AppendUint32(dst, encodeChainWord(c.chain))
	dst = binary.AppendUvarint(dst, uint64(len(c.payload)))
	return append(dst, c.payload...)
}

func (c *compressedColumn[T]) Segment() Column[T] {
	column, err := c.decompress()
	if err != nil {
		panic(err)
	}
	return column.Segment()
}

func (c *compressedColumn[T]) Compress(codecs ...codecID) Column[T] {
	return c
}

func (c *compressedColumn[T]) NewIter() Iter[T] {
	column, err := c.decompress()
	if err != nil {
		panic(err)
	}
	return column.NewIter()
}

func (c *compressedColumn[T]) decompress() (Column[T], error) {
	payload := c.payload
	for i := len(c.chain) - 1; i >= 0; i-- {
		decoded, err := decodeWith(c.chain[i], payload, c.rows)
		if err != nil {
			return nil, fmt.Errorf("failed to decode column with codec %d: %w", c.chain[i], err)
		}
		payload = decoded
	}
	column, err := c.decode(c.rows, payload)
	if err != nil {
		return nil, err
	}
	if column.Len() != c.rows {
		return nil, fmt.Errorf("decoded column rows %d do not match %d", column.Len(), c.rows)
	}
	return column, nil
}

// segmentedColumn is a column split into nested columns. Iteration keeps at
// most one segment iterated at a time; segments decode their payloads lazily.
type segmentedColumn[T any] struct {
	magic    byte
	segments []Column[T]
	cnt      int
}

func (c *segmentedColumn[T]) Len() int {
	return c.cnt
}

func (c *segmentedColumn[T]) Append(value T) {
	panic("append to a segmented column is not supported")
}

func (c *segmentedColumn[T]) MemUsage() int {
	var segment Column[T]
	total := int(unsafe.Sizeof(*c)) + cap(c.segments)*int(unsafe.Sizeof(segment))
	for i := range c.segments {
		total += c.segments[i].MemUsage()
	}
	return total
}

func (c *segmentedColumn[T]) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, c.magic, c.cnt, columnLayoutSegmented)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(c.segments)))
	for i := range c.segments {
		blob := c.segments[i].AppendTo(nil)
		dst = binary.AppendUvarint(dst, uint64(len(blob)))
		dst = append(dst, blob...)
	}
	return dst
}

func (c *segmentedColumn[T]) Segment() Column[T] {
	return c
}

func (c *segmentedColumn[T]) Compress(codecs ...codecID) Column[T] {
	segments := make([]Column[T], len(c.segments))
	for i := range c.segments {
		segments[i] = c.segments[i].Compress(codecs...)
	}
	return &segmentedColumn[T]{magic: c.magic, segments: segments, cnt: c.cnt}
}

func (c *segmentedColumn[T]) NewIter() Iter[T] {
	return &segmentedIter[T]{col: c, rowIdx: -1}
}

type segmentedIter[T any] struct {
	col    *segmentedColumn[T]
	iter   Iter[T]
	segIdx int
	rowIdx int
}

func (it *segmentedIter[T]) Next() bool {
	if it.rowIdx+1 >= it.col.cnt {
		it.rowIdx = it.col.cnt
		return false
	}
	if it.iter != nil && it.iter.Next() {
		it.rowIdx++
		return true
	}
	for it.segIdx < len(it.col.segments) {
		it.iter = it.col.segments[it.segIdx].NewIter()
		it.segIdx++
		if it.iter.Next() {
			it.rowIdx++
			return true
		}
	}
	it.rowIdx = it.col.cnt
	return false
}

func (it *segmentedIter[T]) Value() T {
	return it.iter.Value()
}

func appendColumnHeader(dst []byte, magic byte, rows int, layout byte) []byte {
	var header [columnHeaderSize]byte
	header[0] = magic
	header[1] = columnFormatVersion
	header[2] = layout
	binary.LittleEndian.PutUint64(header[8:16], uint64(rows))
	return append(dst, header[:]...)
}

func parseColumnHeader(data []byte, magic byte) (rows int, layout byte, err error) {
	if len(data) < columnHeaderSize {
		return 0, 0, fmt.Errorf("not enough data for column header: %d", len(data))
	}
	if data[0] != magic {
		return 0, 0, fmt.Errorf("invalid column magic byte %d", data[0])
	}
	if data[1] != columnFormatVersion {
		return 0, 0, fmt.Errorf("unsupported column format version %d", data[1])
	}
	if data[2] > columnLayoutCompressed {
		return 0, 0, fmt.Errorf("unsupported column layout %d", data[2])
	}
	rows64 := binary.LittleEndian.Uint64(data[8:16])
	if rows64 > uint64(math.MaxInt) {
		return 0, 0, fmt.Errorf("too many rows in column %d", rows64)
	}
	return int(rows64), data[2], nil
}

func unpackColumn[T any](decode func(int, []byte) (Column[T], error), data []byte, magic byte) (Column[T], error) {
	rows, layout, err := parseColumnHeader(data, magic)
	if err != nil {
		return nil, err
	}
	switch layout {
	case columnLayoutFlat:
		return decode(rows, data[columnHeaderSize:])
	case columnLayoutCompressed:
		return unpackCompressedColumn(decode, data[columnHeaderSize:], rows, magic)
	default:
		return unpackSegmentedColumn(decode, data[columnHeaderSize:], rows, magic)
	}
}

func unpackCompressedColumn[T any](decode func(int, []byte) (Column[T], error), src []byte, rows int, magic byte) (Column[T], error) {
	if len(src) < sizeOfUint32 {
		return nil, fmt.Errorf("truncated codec chain")
	}
	column := &compressedColumn[T]{
		magic:  magic,
		rows:   rows,
		chain:  decodeChainWord(binary.LittleEndian.Uint32(src)),
		decode: decode,
	}
	src = src[sizeOfUint32:]
	payloadLen, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, fmt.Errorf("invalid payload length")
	}
	src = src[n:]
	if uint64(len(src)) < payloadLen {
		return nil, fmt.Errorf("truncated payload")
	}
	column.payload = src[:payloadLen]
	return column, nil
}

func unpackSegmentedColumn[T any](decode func(int, []byte) (Column[T], error), src []byte, rows int, magic byte) (Column[T], error) {
	if len(src) < sizeOfUint32 {
		return nil, fmt.Errorf("truncated segment count")
	}
	segments := int(binary.LittleEndian.Uint32(src))
	src = src[sizeOfUint32:]
	column := &segmentedColumn[T]{magic: magic, cnt: rows, segments: make([]Column[T], 0, segments)}
	decodedRows := 0
	for i := range segments {
		blobLen, n := binary.Uvarint(src)
		if n <= 0 || uint64(len(src[n:])) < blobLen {
			return nil, fmt.Errorf("truncated segment %d", i)
		}
		segment, err := unpackColumn(decode, src[n:n+int(blobLen)], magic)
		if err != nil {
			return nil, fmt.Errorf("segment %d: %w", i, err)
		}
		src = src[n+int(blobLen):]
		decodedRows += segment.Len()
		column.segments = append(column.segments, segment)
	}
	if decodedRows != rows {
		return nil, fmt.Errorf("segment rows %d do not match column rows %d", decodedRows, rows)
	}
	return column, nil
}

// StringColumn is the mutable string column with rows kept in a single flat
// byte slice divided by offsets.
type StringColumn struct {
	data    []byte
	offsets []uint32
}

func NewStringColumn(capacity int) *StringColumn {
	return &StringColumn{offsets: make([]uint32, 1, capacity+1)}
}

func (c *StringColumn) Len() int {
	if len(c.offsets) == 0 {
		return 0
	}
	return len(c.offsets) - 1
}

func (c *StringColumn) Append(value []byte) {
	c.data = append(c.data, value...)
	c.offsets = append(c.offsets, uint32(len(c.data)))
}

func (c *StringColumn) AppendString(value string) {
	c.data = append(c.data, value...)
	c.offsets = append(c.offsets, uint32(len(c.data)))
}

func (c *StringColumn) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.data) + sizeOfUint32*cap(c.offsets)
}

func (c *StringColumn) AppendTo(dst []byte) []byte {
	rows := c.Len()
	dst = appendColumnHeader(dst, stringColumnMagic, rows, columnLayoutFlat)
	if rows > 0 {
		for _, offset := range c.offsets {
			dst = binary.LittleEndian.AppendUint32(dst, offset)
		}
	}
	return append(dst, c.data...)
}

func (c *StringColumn) NewIter() Iter[[]byte] {
	return &stringColumnIter{c: c, idx: -1}
}

func (c *StringColumn) Compress(codecs ...codecID) Column[[]byte] {
	validateChain(codecs, codecZstd, codecFrontEncode)
	rows := c.Len()
	return newCompressedColumn(stringColumnMagic, c.appendRows(nil, 0, rows), rows, codecs, decodeStringColumn)
}

func (c *StringColumn) Segment() Column[[]byte] {
	rows := c.Len()
	var segments []Column[[]byte]
	for start := 0; start < rows; {
		end := start + 1
		for end < rows && c.offsets[end+1]-c.offsets[start] <= columnSegmentSize {
			end++
		}
		segment, err := decodeStringColumn(end-start, c.appendRows(nil, start, end))
		if err != nil {
			panic(err)
		}
		segments = append(segments, segment)
		start = end
	}
	return &segmentedColumn[[]byte]{magic: stringColumnMagic, segments: segments, cnt: rows}
}

func UnpackStringColumn(data []byte) (Column[[]byte], error) {
	return unpackColumn(decodeStringColumn, data, stringColumnMagic)
}

func (c *StringColumn) appendRows(dst []byte, start, end int) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, 0)
	for row := start; row < end; row++ {
		dst = binary.LittleEndian.AppendUint32(dst, c.offsets[row+1]-c.offsets[start])
	}
	return append(dst, c.data[c.offsets[start]:c.offsets[end]]...)
}

type stringColumnIter struct {
	c   *StringColumn
	idx int
}

func (it *stringColumnIter) Next() bool {
	if it.idx+1 >= it.c.Len() {
		it.idx = it.c.Len()
		return false
	}
	it.idx++
	return true
}

func (it *stringColumnIter) Value() []byte {
	return it.c.data[it.c.offsets[it.idx]:it.c.offsets[it.idx+1]]
}

func decodeStringColumn(rows int, payload []byte) (Column[[]byte], error) {
	if rows == 0 {
		return &StringColumn{}, nil
	}
	offsetsSize := (rows + 1) * sizeOfUint32
	if len(payload) < offsetsSize {
		return nil, fmt.Errorf("truncated string column offsets: %d bytes", len(payload))
	}
	offsets := make([]uint32, rows+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(payload[i*sizeOfUint32:])
	}
	data := payload[offsetsSize:]
	if uint64(offsets[len(offsets)-1]) != uint64(len(data)) {
		return nil, fmt.Errorf("string column offsets end at %d but data has %d bytes", offsets[len(offsets)-1], len(data))
	}
	for i := 1; i < len(offsets)-1; i++ {
		if offsets[i] > offsets[i+1] {
			return nil, fmt.Errorf("string column offsets are not monotonic at row %d", i)
		}
	}
	return &StringColumn{data: data, offsets: offsets}, nil
}

func frontEncodePayload(payload []byte, rows int) ([]byte, error) {
	offsets := make([]uint32, rows+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(payload[i*sizeOfUint32:])
	}
	data := payload[(rows+1)*sizeOfUint32:]

	var lens []byte
	suffix := make([]byte, 0, len(data))
	frontOffsets := make([]uint32, rows+1)
	for row := 0; row < rows; row++ {
		cur := data[offsets[row]:offsets[row+1]]
		shared := 0
		if row > 0 {
			prev := data[offsets[row-1]:offsets[row]]
			shared = commonPrefix(prev, cur)
		}
		suffix = append(suffix, cur[shared:]...)
		frontOffsets[row+1] = uint32(len(suffix))
		lens = binary.AppendUvarint(lens, uint64(shared))
	}

	out := make([]byte, 0, len(payload))
	for _, offset := range frontOffsets {
		out = binary.LittleEndian.AppendUint32(out, offset)
	}
	out = append(out, suffix...)
	return append(out, lens...), nil
}

func frontDecodePayload(payload []byte, rows int) ([]byte, error) {
	frontOffsets := make([]uint32, rows+1)
	for i := range frontOffsets {
		frontOffsets[i] = binary.LittleEndian.Uint32(payload[i*sizeOfUint32:])
	}
	if frontOffsets[0] != 0 {
		return nil, fmt.Errorf("invalid front coded offsets")
	}
	suffixLens := payload[(rows+1)*sizeOfUint32:]
	suffixTotal := int(frontOffsets[rows])
	if suffixTotal > len(suffixLens) {
		return nil, fmt.Errorf("front coded suffixes exceed data: %d > %d", suffixTotal, len(suffixLens))
	}

	lens := make([]uint32, rows)
	pos := suffixTotal
	for i := range lens {
		value, n := binary.Uvarint(suffixLens[pos:])
		if n <= 0 || value > 1<<31 {
			return nil, fmt.Errorf("invalid front coded shared length")
		}
		lens[i] = uint32(value)
		pos += n
	}
	if pos != len(suffixLens) {
		return nil, fmt.Errorf("trailing bytes in front coded shared lengths")
	}

	data := make([]byte, 0, suffixTotal)
	offsets := make([]uint32, rows+1)
	prevStart := 0
	for row := 0; row < rows; row++ {
		keep := int(lens[row])
		start := len(data)
		if row > 0 {
			if keep > start-prevStart {
				return nil, fmt.Errorf("front coded shared length %d exceeds previous row", keep)
			}
			data = append(data, data[prevStart:prevStart+keep]...)
		} else if keep != 0 {
			return nil, fmt.Errorf("first front coded row has non-zero shared length")
		}
		data = append(data, suffixLens[frontOffsets[row]:frontOffsets[row+1]]...)
		offsets[row] = uint32(start)
		prevStart = start
	}
	offsets[rows] = uint32(len(data))

	out := make([]byte, 0, (rows+1)*sizeOfUint32+len(data))
	for _, offset := range offsets {
		out = binary.LittleEndian.AppendUint32(out, offset)
	}
	return append(out, data...), nil
}

func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

type Int64Column struct {
	values []int64
}

func NewInt64Column(capacity int) *Int64Column {
	if capacity == 0 {
		return &Int64Column{}
	}
	return &Int64Column{values: make([]int64, 0, capacity)}
}

func NewEmptyInt64Column() *Int64Column {
	return &Int64Column{}
}

func (c *Int64Column) Len() int {
	return len(c.values)
}

func (c *Int64Column) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.values)*sizeOfUint64
}

func (c *Int64Column) Append(v int64) {
	c.values = append(c.values, v)
}

func (c *Int64Column) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, int64ColumnMagic, c.Len(), columnLayoutFlat)
	for _, value := range c.values {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(value))
	}
	return dst
}

func (c *Int64Column) NewIter() Iter[int64] {
	return &int64ColumnIter{c: c, row: -1}
}

func (c *Int64Column) Compress(codecs ...codecID) Column[int64] {
	validateChain(codecs, codecZstd, codecDeltaBitpack)
	return newCompressedColumn(int64ColumnMagic, c.appendRows(nil, 0, len(c.values)), len(c.values), codecs, decodeInt64Column)
}

func (c *Int64Column) Segment() Column[int64] {
	var segments []Column[int64]
	stride := columnSegmentSize / sizeOfUint64
	for start := 0; start < len(c.values); start += stride {
		end := min(start+stride, len(c.values))
		segment, err := decodeInt64Column(end-start, c.appendRows(nil, start, end))
		if err != nil {
			panic(err)
		}
		segments = append(segments, segment)
	}
	return &segmentedColumn[int64]{magic: int64ColumnMagic, segments: segments, cnt: len(c.values)}
}

func (c *Int64Column) appendRows(dst []byte, start, end int) []byte {
	for _, value := range c.values[start:end] {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(value))
	}
	return dst
}

func UnpackInt64Column(data []byte) (Column[int64], error) {
	return unpackColumn(decodeInt64Column, data, int64ColumnMagic)
}

type int64ColumnIter struct {
	c   *Int64Column
	row int
}

func (it *int64ColumnIter) Next() bool {
	if it.row+1 >= len(it.c.values) {
		it.row = len(it.c.values)
		return false
	}
	it.row++
	return true
}

func (it *int64ColumnIter) Value() int64 {
	return it.c.values[it.row]
}

func decodeInt64Column(rows int, payload []byte) (Column[int64], error) {
	if len(payload) != rows*sizeOfUint64 {
		return nil, fmt.Errorf("malformed int64 column data")
	}
	column := &Int64Column{}
	if rows > 0 {
		column.values = make([]int64, rows)
		for i := range column.values {
			column.values[i] = int64(binary.LittleEndian.Uint64(payload[i*sizeOfUint64:]))
		}
	}
	return column, nil
}

func bitpackEncodePayload(payload []byte, rows int) ([]byte, error) {
	values := make([]uint64, rows)
	for i := range values {
		values[i] = binary.LittleEndian.Uint64(payload[i*sizeOfUint64:])
	}
	return packer.CompressDeltaBitpackUint64(nil, values, nil), nil
}

func bitpackDecodePayload(payload []byte, rows int) ([]byte, error) {
	rest, values, err := packer.DecompressDeltaBitpackUint64(payload, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 || len(values) != rows {
		return nil, fmt.Errorf("inconsistent bitpacked values")
	}
	out := make([]byte, 0, rows*sizeOfUint64)
	for _, value := range values {
		out = binary.LittleEndian.AppendUint64(out, value)
	}
	return out, nil
}

type Float64Column struct {
	values []float64
}

func NewFloat64Column(capacity int) *Float64Column {
	if capacity == 0 {
		return &Float64Column{}
	}
	return &Float64Column{values: make([]float64, 0, capacity)}
}

func (c *Float64Column) Len() int {
	return len(c.values)
}

func (c *Float64Column) MemUsage() int {
	return int(unsafe.Sizeof(*c)) + cap(c.values)*sizeOfFloat64
}

func (c *Float64Column) Append(value float64) {
	c.values = append(c.values, value)
}

func (c *Float64Column) AppendTo(dst []byte) []byte {
	dst = appendColumnHeader(dst, float64ColumnMagic, c.Len(), columnLayoutFlat)
	for _, value := range c.values {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
	}
	return dst
}

func (c *Float64Column) NewIter() Iter[float64] {
	return &float64ColumnIter{c: c, rowIdx: -1}
}

func (c *Float64Column) Compress(codecs ...codecID) Column[float64] {
	validateChain(codecs, codecZstd)
	return newCompressedColumn(float64ColumnMagic, c.appendRows(nil, 0, len(c.values)), len(c.values), codecs, decodeFloat64Column)
}

func (c *Float64Column) Segment() Column[float64] {
	var segments []Column[float64]
	stride := columnSegmentSize / sizeOfFloat64
	for start := 0; start < len(c.values); start += stride {
		end := min(start+stride, len(c.values))
		segment, err := decodeFloat64Column(end-start, c.appendRows(nil, start, end))
		if err != nil {
			panic(err)
		}
		segments = append(segments, segment)
	}
	return &segmentedColumn[float64]{magic: float64ColumnMagic, segments: segments, cnt: len(c.values)}
}

func (c *Float64Column) appendRows(dst []byte, start, end int) []byte {
	for _, value := range c.values[start:end] {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
	}
	return dst
}

func UnpackFloat64Column(data []byte) (Column[float64], error) {
	return unpackColumn(decodeFloat64Column, data, float64ColumnMagic)
}

type float64ColumnIter struct {
	c      *Float64Column
	rowIdx int
}

func (it *float64ColumnIter) Next() bool {
	if it.rowIdx+1 >= len(it.c.values) {
		it.rowIdx = len(it.c.values)
		return false
	}
	it.rowIdx++
	return true
}

func (it *float64ColumnIter) Value() float64 {
	return it.c.values[it.rowIdx]
}

func decodeFloat64Column(rows int, payload []byte) (Column[float64], error) {
	if len(payload) != rows*sizeOfFloat64 {
		return nil, fmt.Errorf("malformed float64 column data")
	}
	column := &Float64Column{}
	if rows > 0 {
		column.values = make([]float64, rows)
		for i := range column.values {
			column.values[i] = math.Float64frombits(binary.LittleEndian.Uint64(payload[i*sizeOfFloat64:]))
		}
	}
	return column, nil
}

type arrayItem interface {
	float64 | uint32
}

// ArraysColumn is the mutable column of rows of type T with values kept in a
// single flat slice divided by offsets.
type ArraysColumn[T arrayItem] struct {
	Values  []T
	Offsets []int
}

func NewArraysColumn[T arrayItem](capacity int) *ArraysColumn[T] {
	return &ArraysColumn[T]{
		Values:  make([]T, 0, capacity),
		Offsets: make([]int, 1, capacity+1),
	}
}

func (c *ArraysColumn[T]) Len() int {
	if c == nil || len(c.Offsets) == 0 {
		return 0
	}
	return len(c.Offsets) - 1
}

func (c *ArraysColumn[T]) Append(values []T) {
	if len(c.Offsets) == 0 {
		c.Offsets = []int{0}
	}
	c.Values = append(c.Values, values...)
	c.Offsets = append(c.Offsets, len(c.Values))
}

func (c *ArraysColumn[T]) MemUsage() int {
	if c == nil {
		return 0
	}
	var value T
	return int(unsafe.Sizeof(*c)) + cap(c.Values)*int(unsafe.Sizeof(value)) + cap(c.Offsets)*util.SizeOfInt
}

func (c *ArraysColumn[T]) AppendTo(dst []byte) []byte {
	rows := c.Len()
	dst = appendColumnHeader(dst, arraysColumnMagic, rows, columnLayoutFlat)
	dst = appendArraysValues(dst, c.Values)
	offsets := c.Offsets
	if len(offsets) == 0 {
		offsets = []int{0}
	}
	for _, offset := range offsets {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(offset))
	}
	return dst
}

func (c *ArraysColumn[T]) NewIter() Iter[[]T] {
	return &arraysColumnIter[T]{c: c, rowIdx: -1}
}

func (c *ArraysColumn[T]) Compress(codecs ...codecID) Column[[]T] {
	validateChain(codecs, codecZstd)
	rows := c.Len()
	return newCompressedColumn(arraysColumnMagic, c.appendRows(nil, 0, rows), rows, codecs, decodeArraysColumn[T])
}

func (c *ArraysColumn[T]) Segment() Column[[]T] {
	rows := c.Len()
	var segments []Column[[]T]
	for start := 0; start < rows; {
		end := start + 1
		for end < rows && (c.Offsets[end+1]-c.Offsets[start])*arraysValueSize[T]() <= columnSegmentSize {
			end++
		}
		segment, err := decodeArraysColumn[T](end-start, c.appendRows(nil, start, end))
		if err != nil {
			panic(err)
		}
		segments = append(segments, segment)
		start = end
	}
	return &segmentedColumn[[]T]{magic: arraysColumnMagic, segments: segments, cnt: rows}
}

func UnpackArraysColumn[T arrayItem](data []byte) (Column[[]T], error) {
	return unpackColumn(decodeArraysColumn[T], data, arraysColumnMagic)
}

func (c *ArraysColumn[T]) appendRows(dst []byte, start, end int) []byte {
	dst = appendArraysValues(dst, c.Values[c.Offsets[start]:c.Offsets[end]])
	dst = binary.LittleEndian.AppendUint64(dst, 0)
	for row := start; row < end; row++ {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(c.Offsets[row+1]-c.Offsets[start]))
	}
	return dst
}

type arraysColumnIter[T arrayItem] struct {
	c      *ArraysColumn[T]
	rowIdx int
}

func (it *arraysColumnIter[T]) Next() bool {
	if it.rowIdx+1 >= it.c.Len() {
		it.rowIdx = it.c.Len()
		return false
	}
	it.rowIdx++
	return true
}

func (it *arraysColumnIter[T]) Value() []T {
	return it.c.Values[it.c.Offsets[it.rowIdx]:it.c.Offsets[it.rowIdx+1]]
}

func appendArraysValues[T arrayItem](dst []byte, values []T) []byte {
	switch any(values).(type) {
	case []float64:
		for _, value := range any(values).([]float64) {
			dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
		}
	case []uint32:
		for _, value := range any(values).([]uint32) {
			dst = binary.LittleEndian.AppendUint32(dst, value)
		}
	default:
		panic(fmt.Errorf("unsupported array column value type %T", values))
	}
	return dst
}

func arraysValueSize[T arrayItem]() int {
	var zero T
	switch any(zero).(type) {
	case float64:
		return sizeOfFloat64
	default:
		return sizeOfUint32
	}
}

func decodeArraysColumn[T arrayItem](rows int, payload []byte) (Column[[]T], error) {
	valueSize := arraysValueSize[T]()
	offsetsSize := (rows + 1) * sizeOfUint64
	if len(payload) < offsetsSize {
		return nil, fmt.Errorf("truncated array column offsets")
	}
	valuesLen := len(payload) - offsetsSize
	if valuesLen%valueSize != 0 {
		return nil, fmt.Errorf("malformed array column data")
	}
	values := make([]T, valuesLen/valueSize)
	switch typed := any(values).(type) {
	case []float64:
		for i := range typed {
			typed[i] = math.Float64frombits(binary.LittleEndian.Uint64(payload[i*sizeOfFloat64:]))
		}
	case []uint32:
		for i := range typed {
			typed[i] = binary.LittleEndian.Uint32(payload[i*sizeOfUint32:])
		}
	}
	offsets := make([]int, rows+1)
	for i := range offsets {
		offset := binary.LittleEndian.Uint64(payload[valuesLen+i*sizeOfUint64:])
		if offset > uint64(math.MaxInt) {
			return nil, fmt.Errorf("invalid array offset %d", offset)
		}
		offsets[i] = int(offset)
	}
	return &ArraysColumn[T]{Values: values, Offsets: offsets}, nil
}
