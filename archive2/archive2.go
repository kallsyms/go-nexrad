package archive2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/dsnet/compress/bzip2"
	"github.com/sirupsen/logrus"
)

// LoadedLDMRecord holds both the LDM record information itself, as well as the various
// messages that were in the record
type LoadedLDMRecord struct {
	LDMRecord
	M2   *Message2
	M3   *Message3
	M31s []*Message31
}

// Archive2 wrapper for processed archive 2 data files.
type Archive2 struct {
	// ElevationScans contains all the messages for every elevation scan in the volume
	ElevationScans   map[int][]*Message31
	VolumeHeader     VolumeHeaderRecord
	RadarStatus      *Message2
	RadarPerformance *Message3

	LDMOffsets []int
	LDMRecords []*LoadedLDMRecord

	// Mutex so ElevationScans can be concurrently updated, e.g. in the case of loading
	// chunks in parallel
	mtx sync.Mutex
}

func (ar2 *Archive2) LoadLDMRecord(reader io.Reader) (*LoadedLDMRecord, error) {
	ldm := LDMRecord{}

	// read in control word (size) of LDM record
	err := binary.Read(reader, binary.BigEndian, &ldm.Size)
	if err != nil {
		return nil, err
	}

	// As the control word contains a negative size under some circumstances,
	// the absolute value of the control word must be used for determining
	// the size of the block.
	if ldm.Size < 0 {
		ldm.Size = -ldm.Size
	}

	logrus.Debugf("---------------- LDM Compressed Record (%d bytes)----------------", ldm.Size)

	bzipReader, _ := bzip2.NewReader(io.LimitReader(reader, int64(ldm.Size)), nil)

	numMessages := 0
	loadedRecord := &LoadedLDMRecord{
		LDMRecord: ldm,
	}

	// read until no more messages are available
	for {

		numMessages += 1

		// eat 12 bytes due to legacy compliance of CTM Header, these are all set to nil
		io.ReadFull(bzipReader, make([]byte, 12))

		header := MessageHeader{}
		if err := binary.Read(bzipReader, binary.BigEndian, &header); err != nil {
			if err != io.EOF {
				return loadedRecord, err
			}
			break
		}

		logrus.WithFields(logrus.Fields{
			"type": header.MessageType,
			"seq":  header.IDSequenceNumber,
			"size": header.MessageSize,
		}).Tracef("== Message %d", header.MessageType)

		switch header.MessageType {
		case 2:
			loadedRecord.M2 = &Message2{}
			binary.Read(bzipReader, binary.BigEndian, loadedRecord.M2)
			// skip the rest; 68 is the size of a Message2 record
			io.ReadFull(bzipReader, make([]byte, MessageBodySize-68))
		case 3:
			loadedRecord.M3 = &Message3{}
			binary.Read(bzipReader, binary.BigEndian, loadedRecord.M3)
			io.ReadFull(bzipReader, make([]byte, MessageBodySize-960))
		case 31:
			sz := uint32(header.MessageSize)
			// not sure if this is actually applicable
			if sz == 65535 {
				sz = uint32(header.NumMessageSegments)<<16 | uint32(header.MessageSegmentNum)
			}
			data := make([]byte, sz)
			_, err := io.ReadFull(bzipReader, data)
			if err != nil {
				return loadedRecord, err
			}
			m31, err := NewMessage31(bytes.NewReader(data))
			if err != nil {
				return loadedRecord, err
			}
			loadedRecord.M31s = append(loadedRecord.M31s, m31)
		default:
			io.ReadFull(bzipReader, make([]byte, MessageBodySize))
		}
	}

	return loadedRecord, nil
}

func (ar2 *Archive2) String() string {
	return fmt.Sprintf("%s\n%s", ar2.VolumeHeader, ar2.RadarStatus)
}

func (ar2 *Archive2) AddFromLDMRecord(loadedRecord *LoadedLDMRecord) {
	if loadedRecord.M2 != nil && ar2.RadarStatus == nil {
		// keep a reference around
		ar2.RadarStatus = loadedRecord.M2
	}
	if loadedRecord.M3 != nil && ar2.RadarPerformance == nil {
		ar2.RadarPerformance = loadedRecord.M3
	}
	for _, m31 := range loadedRecord.M31s {
		ar2.ElevationScans[int(m31.Header.ElevationNumber)] = append(ar2.ElevationScans[int(m31.Header.ElevationNumber)], m31)
	}
}

// Extract returns a new Archive2 from the provided reader
func Extract(reader io.Reader) (*Archive2, error) {

	spew.Config.DisableMethods = true

	ar2 := Archive2{
		ElevationScans: make(map[int][]*Message31),
		VolumeHeader:   VolumeHeaderRecord{},
	}

	// -------------------------- Volume Header Record -------------------------
	// At the start of every volume is a 24-byte record describing certain attributes
	// of the radar data. The first 9 bytes is a character constant of which the
	// last 2 characters identify the version. The next 3 bytes is a numeric string
	// field starting with the value 001 and increasing by one for each volume of
	// radar data in the queue to a maximum value of 999. Once the maximum value is
	// reached the value will be rolled over. The combined 12 bytes are called the
	// Archive II filename.

	// read in the volume header record
	binary.Read(reader, binary.BigEndian, &ar2.VolumeHeader)

	logrus.Debug(ar2.VolumeHeader)

	offset := 24

	// read until no more LDM records are available
	for true {
		loadedRecord, err := ar2.LoadLDMRecord(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		ar2.LDMOffsets = append(ar2.LDMOffsets, offset)
		offset += int(loadedRecord.LDMRecord.Size) + 4
		ar2.LDMRecords = append(ar2.LDMRecords, loadedRecord)
		ar2.AddFromLDMRecord(loadedRecord)
	}

	return &ar2, nil
}

func NewArchive2FromFile(filename string) (*Archive2, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return Extract(file)
}
