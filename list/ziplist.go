package list

import "encoding/binary"

var (
	MaxZiplistSize = 1024 * 256 // 256KB
)
var (
	endOfList = []byte{0xff}
)

type ziplistEntry struct {
	entry []byte
}

func (z *ziplistEntry) Encode() []byte {
	return z.entry
}
func (z *ziplistEntry) Decode(data []byte) {
	z.entry = data
}
func (z *ziplistEntry) Size() int {
	return len(z.entry)
}
func (z *ziplistEntry) Count() int {
	iter := z.GetIterator()
	count := 0
	for iter.Next() != nil {
		count++
	}
	return count
}

type zlentry struct {
	prevrawlensize uint64
	len            uint64
	data           []byte
	headerData     []byte
}
type iterator struct {
	// current position
	pos          int
	offset       uint64
	ziplistEntry *ziplistEntry
}

func (it *iterator) Next() *zlentry {
	if it.offset >= uint64(len(it.ziplistEntry.entry)) {
		return nil
	}
	entry := Newzlentry(it.ziplistEntry.entry[it.offset : it.offset+16])
	entry.DecodeHeader()
	entry.data = it.ziplistEntry.entry[it.offset+16 : 16+it.offset+entry.len]
	it.offset = it.offset + 16 + entry.len
	it.pos++
	return entry
}
func NewZiplistEntry() *ziplistEntry {
	return &ziplistEntry{}
}
func (z *ziplistEntry) GetIterator() *iterator {
	return &iterator{
		pos:          -1,
		offset:       0,
		ziplistEntry: z,
	}
}
func Newzlentry(data []byte) *zlentry {
	return &zlentry{
		data: data,
	}
}

func (z *ziplistEntry) Insert(pos int, data []byte) {
	iter := z.GetIterator()
	for i := 0; i < pos; i++ {
		iter.Next()
	}
	// encode data
	entry := Newzlentry(data)
	entry.prevrawlensize = iter.offset
	entry.len = uint64(len(data))
	encodeData := entry.Encode()
	// adjust
	newData := make([]byte, 0)
	var offset uint64
	offset = iter.offset
	for offset < uint64(len(z.entry)) {
		entry := Newzlentry(z.entry[offset : offset+16])
		entry.DecodeHeader()
		entry.prevrawlensize = entry.prevrawlensize + uint64(len(encodeData))
		entry.data = z.entry[offset+16 : offset+16+entry.len]
		offset = offset + 16 + entry.len
		newData = append(newData, entry.Encode()...)
	}
	// insert
	z.entry = append(z.entry[:iter.offset], append(encodeData, newData...)...)
}
func (z *ziplistEntry) CanInsert(data []byte) bool {
	return z.Size()+len(data)+16 <= MaxZiplistSize
}
func (z *ziplistEntry) SplitAt(pos int) *ziplistEntry {
	iter := z.GetIterator()
	for i := 0; i < pos; i++ {
		iter.Next()
	}
	newZl := NewZiplistEntry()
	newZl.entry = z.entry[iter.offset:]
	z.entry = z.entry[:iter.offset]
	return newZl
}

//func (z *ziplistEntry) Append(data []byte) {
//	if z.entry == nil {
//		z.entry = make([]byte, 0)
//	}
//
//	entry := Newzlentry(data)
//	entry.prevrawlensize = uint64(len(z.entry))
//	entry.len = uint64(len(data))
//	encodeData := entry.Encode()
//	z.entry = append(z.entry, encodeData...)
//}
func (z *ziplistEntry) Delete(pos int) {
	iter := z.GetIterator()
	var targetEntry *zlentry
	for i := 0; i < pos+1; i++ {
		targetEntry = iter.Next()
	}
	// decode data to delete
	entry := Newzlentry(z.entry[iter.offset : iter.offset+16])
	entry.DecodeHeader()
	entry.data = z.entry[iter.offset+16 : iter.offset+16+entry.len]
	// adjust
	newData := make([]byte, 0)
	offset := iter.offset
	for offset < uint64(len(z.entry)) {
		entry := Newzlentry(z.entry[offset : offset+16])
		entry.DecodeHeader()
		entry.prevrawlensize = entry.prevrawlensize - (targetEntry.len + 16)
		entry.data = z.entry[offset+16 : offset+16+entry.len]
		offset = offset + 16 + entry.len
		newData = append(newData, entry.Encode()...)
	}
	newEntry := z.entry[:targetEntry.prevrawlensize]
	newEntry = append(newEntry, newData...)
	// delete
	z.entry = newEntry
}
func (z *ziplistEntry) Index(pos int) *zlentry {
	iter := z.GetIterator()
	for i := 0; i < pos; i++ {
		iter.Next()
	}
	return iter.Next()
}
func (z *zlentry) Encode() []byte {
	z.EncodeHeader()
	return append(z.headerData, z.data...)
}
func (z *zlentry) DecodeHeader() {
	// get prev raw len size
	rawFirstPreviewLenSize := z.data[0:8]
	z.prevrawlensize = binary.BigEndian.Uint64(rawFirstPreviewLenSize)
	// get prev raw len
	rawLen := z.data[8:16]
	z.len = binary.BigEndian.Uint64(rawLen)
}

func (z *zlentry) EncodeHeader() {
	z.headerData = make([]byte, 16)
	binary.BigEndian.PutUint64(z.headerData[0:8], z.prevrawlensize)
	binary.BigEndian.PutUint64(z.headerData[8:16], z.len)
}
