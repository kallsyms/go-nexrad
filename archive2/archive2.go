package archive2

import (
	"bytes"
	"compress/bzip2"
	"encoding/binary"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

// Archive2 wrapper for processed archive 2 data files.
type Archive2 struct {
	VolumeHeader   VolumeHeaderRecord
	ElevationScans map[int][]*Message31
}

// NewArchive2 returns a new Archive2 from the provided file
func NewArchive2(filename string) *Archive2 {

	ar2 := Archive2{
		ElevationScans: make(map[int][]*Message31),
		VolumeHeader:   VolumeHeaderRecord{},
	}

	// try to open the file
	file, err := os.Open(filename)
	if err != nil {
		logrus.Fatal(err)
	}

	// the gist of the file format is documented in RDA/RPG 7.3.6
	// but in short:
	//  - read in 24 byte Volume Header
	//  - read in 1 LDM Compressed Record - this is the metadata record
	//  - read in N LDM Compressed Records - these are the data records

	// read in the volume header record
	binary.Read(file, binary.BigEndian, &ar2.VolumeHeader)
	logrus.Info(ar2.VolumeHeader.Filename())

	// read until no more LDM records are available
	for true {
		ldm := LDMRecord{}

		// read in size of LDM record
		err := binary.Read(file, binary.BigEndian, &ldm.Size)
		if err != nil {
			// all done if EOF
			if err == io.EOF {
				return &ar2
			}

			// un oh, something unexpected
			logrus.Panic(err.Error())
		}

		// the size can be negative, but you just interpret it as positive (RDA/RPG 7.3.4)
		if ldm.Size < 0 {
			ldm.Size = -ldm.Size
		}

		logrus.Debugf("LDM Compressed Record (%d bytes)", ldm.Size)

		var compressedRecord = make([]byte, ldm.Size)
		file.Read(compressedRecord)
		bzipReader := bzip2.NewReader(bytes.NewReader(compressedRecord))

		// this uses the alternative bzip2 implementation from github.com/dsnet/compress/bzip2 but the improvement
		// didn't seem substantial enough to add the dependency (~2.2s down to ~2.0s)
		// bzipReader, _ := bzip2.NewReader(bytes.NewReader(compressedRecord), nil)

		// read until no more messages are available
		for true {

			// eat 12 bytes due to legacy compliance of CTM Header, these are all set to nil
			bzipReader.Read(make([]byte, 12))

			// read in the rest of the header
			header := MessageHeader{}
			if err := binary.Read(bzipReader, binary.BigEndian, &header); err != nil {
				if err != io.EOF {
					logrus.Panic(err.Error())
				}
				break
			}

			logrus.Debugf("  Message Type %d (segments: %d size: %d)", header.MessageType, header.NumMessageSegments, header.MessageSize)

			// anything not called out in the switch falls into the default (and is skipped)
			switch header.MessageType {
			case 2:
				m2 := Message2{}
				binary.Read(bzipReader, binary.BigEndian, &m2)
				logrus.Tracef("    status=%d op-status=%d vcp=%d build=%.2f",
					m2.RDAStatus,
					m2.OperabilityStatus,
					m2.VolumeCoveragePatternNum,
					float32(m2.RDABuild/100),
				)
			case 31:
				m31 := NewMessage31(bzipReader)

				if m31.AzimuthAngle > 140 && m31.AzimuthAngle < 150 {
					logrus.Tracef("    deg=%7.3f elv=%2d tilt=%5f data=(%d gates) %v",
						m31.AzimuthAngle,
						m31.ElevationNumber,
						m31.ElevationAngle,
						m31.ReflectivityData.NumberDataMomentGates,
						m31.ReflectivityData.ScaledData()[0:25],
					)
				}

				ar2.ElevationScans[int(m31.ElevationNumber)] = append(ar2.ElevationScans[int(m31.ElevationNumber)], m31)
			default:
				// not handled, skip the rest - which we know is DEFAULT - CTM - header
				skip := make([]byte, DefaultMetadataRecordLength-LegacyCTMHeaderLength-16)
				bzipReader.Read(skip)
			}
		}
	}
	return &ar2
}
