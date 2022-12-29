package polarisdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Log struct {
	path           string
	lastIndex      int
	lastFilePath   string
	maxSegSize     int64
	indexToSegFile map[int]string
}

type Segment struct {
	Index  int
	path   string
	Blocks []*Block
}

type Block struct {
	Data []byte
}
type BlockIndex struct {
	start int
	end   int
}

func (s *Segment) Serialize() (io.Reader, error) {
	// serialize by gob
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(s)
	if err != nil {
		return nil, err
	}
	return &buf, nil
}
func (s *Segment) Deserialize(reader io.Reader) error {
	return gob.NewDecoder(reader).Decode(s)
}
func OpenSegment(path string) (*Segment, error) {
	segFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	seg := &Segment{}
	err = seg.Deserialize(segFile)
	if err != nil {
		return nil, err
	}
	return seg, nil
}
func (l *Log) Open(path string) error {
	if l.maxSegSize == 0 {
		l.maxSegSize = 20 * 1024 * 1024 // 20MB
	}
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return err
	}
	l.path = path
	items, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	l.indexToSegFile = make(map[int]string)
	for _, item := range items {
		if item.IsDir() {
			continue
		}
		// read and parse to segment
		itemPath := filepath.Join(path, item.Name())
		seg, err := OpenSegment(itemPath)
		if err != nil {
			return err
		}
		l.indexToSegFile[seg.Index] = itemPath
	}
	if len(l.indexToSegFile) == 0 {
		return nil
	}
	// find last
	l.lastIndex = -1
	for key, _ := range l.indexToSegFile {
		if key > l.lastIndex {
			l.lastIndex = key
		}
	}
	l.lastFilePath = l.indexToSegFile[l.lastIndex]
	if err != nil {
		return err
	}
	return nil
}
func (l *Log) Append(block *Block) error {
	// check if need to create new segment
	if len(l.lastFilePath) != 0 {
		stat, err := os.Stat(l.lastFilePath)
		if err != nil {
			return err
		}
		if (stat.Size() + int64(len(block.Data))) > l.maxSegSize {
			l.lastIndex++
			l.lastFilePath = ""
		}
	}

	seg := &Segment{}
	if len(l.lastFilePath) == 0 {
		saveName := fmt.Sprintf("%d", l.lastIndex)
		seg.Blocks = make([]*Block, 0)
		seg.Index = l.lastIndex
		l.lastFilePath = filepath.Join(l.path, saveName)
	} else {
		file, err := os.Open(l.lastFilePath)
		if err != nil {
			return err
		}
		err = seg.Deserialize(file)
		if err != nil {
			return err
		}
	}
	seg.Blocks = append(seg.Blocks, block)
	// write to file
	segReader, err := seg.Serialize()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(segReader)
	if err != nil {
		return err
	}
	err = os.WriteFile(l.lastFilePath, buf.Bytes(), os.ModePerm)
	if err != nil {
		return err
	}
	if _, exist := l.indexToSegFile[seg.Index]; !exist {
		l.indexToSegFile[seg.Index] = l.lastFilePath
	}
	return nil
}

type LogIterator struct {
	log           *Log
	curBlockIndex int
	curSegIndex   int
	curSeg        *Segment
	empty         bool
}

func (l *Log) NewLogIterator() (*LogIterator, error) {
	iter := &LogIterator{
		log:           l,
		curBlockIndex: 0,
	}
	if len(l.indexToSegFile) == 0 {
		iter.empty = true
		return iter, nil
	}
	initSeg, err := OpenSegment(l.indexToSegFile[0])
	if err != nil {
		return nil, err
	}
	iter.curSeg = initSeg
	return iter, nil
}

func (it *LogIterator) Next() *Block {
	if it.empty {
		return nil
	}
	if it.curSegIndex >= len(it.log.indexToSegFile) {
		return nil
	}
	if it.curBlockIndex >= len(it.curSeg.Blocks) {
		// next segment
		it.curSegIndex++
		if it.curSegIndex >= len(it.log.indexToSegFile) {
			return nil
		}
		seg, err := OpenSegment(it.log.indexToSegFile[it.curSegIndex])
		if err != nil {
			return nil
		}
		it.curSeg = seg
		it.curBlockIndex = 0
	}
	block := it.curSeg.Blocks[it.curBlockIndex]
	it.curBlockIndex++
	return block
}
