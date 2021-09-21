package archive2

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dsnet/compress/bzip2"
	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

// LoadedLDMRecord holds both the LDM record information itself, as well as the various
// messages that were in the record
type LoadedLDMRecord struct {
	LDMRecord
	M2   *Message2
	M31s []*Message31
}

// Archive2 wrapper for processed archive 2 data files.
type Archive2 struct {
	VolumeHeader VolumeHeaderRecord

	LDMOffsets []int
	LDMRecords []*LoadedLDMRecord

	// Mutex so ElevationScans can be concurrently updated, e.g. in the case of loading
	// chunks in parallel
	mtx            sync.Mutex
	ElevationScans map[int][]*Message31

	// the metadata record will contain a single Message Type 2 which comes in handy
	// in other parts of the decoding for version-specific handling.
	metadataStatusMessage *Message2
}

func (ar2 *Archive2) LoadLDMRecord(reader io.Reader) (*LoadedLDMRecord, error) {
	ldm := LDMRecord{}

	// read in size of LDM record
	err := binary.Read(reader, binary.BigEndian, &ldm.Size)
	if err != nil {
		return nil, err
	}

	// the size can be negative, but you just interpret it as positive (RDA/RPG 7.3.4)
	if ldm.Size < 0 {
		ldm.Size = -ldm.Size
	}

	logrus.Debugf("LDM Compressed Record (%s bytes)", color.CyanString("%d", ldm.Size))

	bzipReader, _ := bzip2.NewReader(io.LimitReader(reader, int64(ldm.Size)), nil)

	totalMessages := 0
	loadedRecord := &LoadedLDMRecord{
		LDMRecord: ldm,
	}

	// read until no more messages are available
	for true {
		// eat 12 bytes due to legacy compliance of CTM Header, these are all set to nil
		io.ReadFull(bzipReader, make([]byte, 12))

		// read in the rest of the header
		header := MessageHeader{}
		if err := binary.Read(bzipReader, binary.BigEndian, &header); err != nil {
			if err != io.EOF {
				return loadedRecord, err
			}
			break
		}

		totalMessages += 1
		logrus.Tracef("  Message Type %d (segments: %d size: %d)", header.MessageType, header.NumMessageSegments, header.MessageSize)

		// anything not called out in the switch falls into the default (and is skipped)
		switch header.MessageType {
		case 2:
			// we'll keep the first one - it should be the metadata record's
			loadedRecord.M2 = &Message2{}
			binary.Read(bzipReader, binary.BigEndian, loadedRecord.M2)

			if loadedRecord.M2.GetBuildNumber() < 18 {
				return loadedRecord, fmt.Errorf("This file is build %.2f. Only build 19.00 is well supported. Try a more recent file.", loadedRecord.M2.GetBuildNumber())
			}

			// skip the rest
			io.ReadFull(bzipReader, make([]byte, DefaultMetadataRecordLength-LegacyCTMHeaderLength-16-68))

		case 31:
			m31, err := NewMessage31(bzipReader, ar2.metadataStatusMessage.GetBuildNumber())

			if err != nil {
				return loadedRecord, err
			}

			// instead of having every message dump data out, we'll just look at the 0-1 degree data
			if m31.Header.AzimuthAngle < 1 {
				logrus.Trace(m31)
			}

			loadedRecord.M31s = append(loadedRecord.M31s, m31)

		default:
			// not handled, skip the rest - which we know is DEFAULT - CTM - header
			io.ReadFull(bzipReader, make([]byte, DefaultMetadataRecordLength-LegacyCTMHeaderLength-16))
		}
	}

	logrus.Debugf("  found %s messages in this record", color.CyanString("%d", totalMessages))
	if loadedRecord.M2 != nil {
		logrus.Debugf("    m2 found in record")
	}
	if len(loadedRecord.M31s) > 0 {
		logrus.Debugf("    %s m31s found in record", color.CyanString("%d", len(loadedRecord.M31s)))
	}

	return loadedRecord, nil
}

func (ar2 *Archive2) AddFromLDMRecord(loadedRecord *LoadedLDMRecord) {
	if loadedRecord.M2 != nil && ar2.metadataStatusMessage == nil {
		// keep a reference around
		ar2.metadataStatusMessage = loadedRecord.M2
	}
	for _, m31 := range loadedRecord.M31s {
		ar2.ElevationScans[int(m31.Header.ElevationNumber)] = append(ar2.ElevationScans[int(m31.Header.ElevationNumber)], m31)
	}
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
		logrus.Fatal(err)
	}
	defer file.Close()
	return NewArchive2(file)
}
