package archive2

import (
	"compress/bzip2"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

// Archive2 wrapper for processed archive 2 data files.
type Archive2 struct {
	VolumeHeader VolumeHeaderRecord

	mtx            sync.Mutex
	ElevationScans map[int][]*Message31

	// the metadata record will contain a single Message Type 2 which comes in handy
	// in other parts of the decoding for version-specific handling.
	metadataStatusMessage *Message2
}

func (ar2 *Archive2) AddFromLDMRecord(reader io.Reader) (int32, error) {
	ldm := LDMRecord{}

	// read in size of LDM record
	err := binary.Read(reader, binary.BigEndian, &ldm.Size)
	if err != nil {
		return 0, err
	}

	// the size can be negative, but you just interpret it as positive (RDA/RPG 7.3.4)
	if ldm.Size < 0 {
		ldm.Size = -ldm.Size
	}

	logrus.Debugf("LDM Compressed Record (%s bytes)", color.CyanString("%d", ldm.Size))

	bzipReader := bzip2.NewReader(io.LimitReader(reader, int64(ldm.Size)))

	// this uses the alternative bzip2 implementation from github.com/dsnet/compress/bzip2 but the improvement
	// didn't seem substantial enough to add the dependency (~2.2s down to ~2.0s)
	// bzipReader, _ := bzip2.NewReader(bytes.NewReader(compressedRecord), nil)

	// read until no more messages are available
	messageCounts := map[uint8]int{
		2:  0,
		31: 0,
	}

	for true {
		// eat 12 bytes due to legacy compliance of CTM Header, these are all set to nil
		io.ReadFull(bzipReader, make([]byte, 12))

		// read in the rest of the header
		header := MessageHeader{}
		if err := binary.Read(bzipReader, binary.BigEndian, &header); err != nil {
			if err != io.EOF {
				return ldm.Size, err
			}
			break
		}

		logrus.Tracef("  Message Type %d (segments: %d size: %d)", header.MessageType, header.NumMessageSegments, header.MessageSize)

		// anything not called out in the switch falls into the default (and is skipped)
		switch header.MessageType {
		case 2:
			// we'll keep the first one - it should be the metadata record's
			m2 := Message2{}
			binary.Read(bzipReader, binary.BigEndian, &m2)

			if m2.GetBuildNumber() < 18 {
				return ldm.Size, fmt.Errorf("This file is build %.2f. Only build 19.00 is well supported. Try a more recent file.", m2.GetBuildNumber())
			}

			// skip the rest
			io.ReadFull(bzipReader, make([]byte, DefaultMetadataRecordLength-LegacyCTMHeaderLength-16-68))

			// keep a reference around
			if ar2.metadataStatusMessage == nil {
				ar2.metadataStatusMessage = &m2
			}

		case 31:
			m31, err := NewMessage31(bzipReader, ar2.metadataStatusMessage.GetBuildNumber())

			if err != nil {
				return ldm.Size, err
			}

			// instead of having every message dump data out, we'll just look at the 0-1 degree data
			if m31.Header.AzimuthAngle < 1 {
				logrus.Trace(m31)
			}

			ar2.mtx.Lock()
			ar2.ElevationScans[int(m31.Header.ElevationNumber)] = append(ar2.ElevationScans[int(m31.Header.ElevationNumber)], m31)
			ar2.mtx.Unlock()

		default:
			// not handled, skip the rest - which we know is DEFAULT - CTM - header
			io.ReadFull(bzipReader, make([]byte, DefaultMetadataRecordLength-LegacyCTMHeaderLength-16))
		}

		if _, ok := messageCounts[header.MessageType]; ok {
			messageCounts[header.MessageType]++
		} else {
			messageCounts[header.MessageType] = 1
		}
	}

	// helpful for debugging
	totalMessages := 0
	for _, count := range messageCounts {
		totalMessages += count
	}
	logrus.Debugf("  found %s messages in this record", color.CyanString("%d", totalMessages))
	for msgType, count := range messageCounts {
		logrus.Debugf("    type %02d had %d messages", msgType, count)
	}

	return ldm.Size, nil
}

// NewArchive2 returns a new Archive2 from the provided reader
func NewArchive2(reader io.Reader) (*Archive2, error) {
	ar2 := Archive2{
		ElevationScans: make(map[int][]*Message31),
		VolumeHeader:   VolumeHeaderRecord{},
	}

	// the gist of the file format is documented in RDA/RPG 7.3.6
	// but in short:
	//  - read in 24 byte Volume Header
	//  - read in 1 LDM Compressed Record - this is the metadata record
	//  - read in N LDM Compressed Records - these are the data records

	// read in the volume header record
	binary.Read(reader, binary.BigEndian, &ar2.VolumeHeader)
	logrus.Info(ar2.VolumeHeader.Filename())

	// read until no more LDM records are available
	LDMCount := 0
	for true {
		_, err := ar2.AddFromLDMRecord(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		LDMCount++
	}

	return &ar2, nil
}

func NewArchive2FromFile(filename string) (*Archive2, error) {
	file, err := os.Open(filename)
	if err != nil {
		logrus.Fatal(err)
	}
	defer file.Close()
	return NewArchive2(file)
}
