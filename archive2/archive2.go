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

	// -------------------------- Volume Header Record -------------------------

	// read in the volume header record
	binary.Read(file, binary.BigEndian, &ar2.VolumeHeader)
	logrus.Debug(ar2.VolumeHeader.FileName())

	// ------------------------------ LDM Records ------------------------------

	// The first LDMRecord is the Metadata Record, consisting of 134 messages of
	// Metadata message types 15, 13, 18, 3, 5, and 2

	// Following the first LDM Metadata Record is a variable number of compressed
	// records containing 120 radial messages (type 31) plus 0 or more RDA Status
	// messages (type 2).

	for true {
		ldm := LDMRecord{}

		// read in control word (size) of LDM record
		if err := binary.Read(file, binary.BigEndian, &ldm.Size); err != nil {
			if err != io.EOF {
				logrus.Panic(err.Error())
			}
			return nil
		}

		// As the control word contains a negative size under some circumstances,
		// the absolute value of the control word must be used for determining
		// the size of the block.
		if ldm.Size < 0 {
			ldm.Size = -ldm.Size
		}

		logrus.Debugf("LDM Compressed Record (%d bytes)", ldm.Size)

		var compressedRecord = make([]byte, ldm.Size)
		binary.Read(file, binary.BigEndian, &compressedRecord)
		bzipReader := bzip2.NewReader(bytes.NewReader(compressedRecord))

		for true {

			// eat 12 bytes due to legacy compliance of CTM Header, these are all set to nil
			bzipReader.Read(make([]byte, 12))

			header := MessageHeader{}
			if err := binary.Read(bzipReader, binary.BigEndian, &header); err != nil {
				if err != io.EOF {
					logrus.Panic(err.Error())
				}
				break
			}

			logrus.Debugf("  Message Type %d (segments: %d size: %d)", header.MessageType, header.NumMessageSegments, header.MessageSize)

			// possible metadata record types: 0, 2, 3, 5, 13, 15, 18
			// possible standard record types: 1, 2, 31
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
				m31 := msg31(bzipReader)
				if m31.Header.AzimuthAngle > 359 || m31.Header.AzimuthAngle < 1 {
					logrus.Tracef("    az=%3d ɑ=%7.3f elv=%2d tilt=%5f status=%d",
						m31.Header.AzimuthNumber,
						m31.Header.AzimuthAngle,
						m31.Header.ElevationNumber,
						m31.Header.ElevationAngle,
						m31.Header.RadialStatus,
					)
				}

				ar2.ElevationScans[int(m31.Header.ElevationNumber)] = append(ar2.ElevationScans[int(m31.Header.ElevationNumber)], m31)
			default:
				// skip the rest - which we know is DEFAULT - CTM - header
				skip := make([]byte, DefaultMetadataRecordLength-LegacyCTMHeaderLength-16)
				bzipReader.Read(skip)
			}
		}
	}
	return &ar2
}

func msg31(r io.Reader) *Message31 {
	m31h := Message31Header{}
	binary.Read(r, binary.BigEndian, &m31h)

	m31 := Message31{
		Header: m31h,
	}

	for i := uint16(0); i < m31h.DataBlockCount; i++ {

		d := DataBlock{}
		if err := binary.Read(r, binary.BigEndian, &d); err != nil {
			logrus.Panic(err.Error())
		}

		blockName := string(d.DataName[:])
		switch blockName {
		case "VOL":
			binary.Read(r, binary.BigEndian, &m31.VolumeData)
		case "ELV":
			binary.Read(r, binary.BigEndian, &m31.ElevationData)
		case "RAD":
			binary.Read(r, binary.BigEndian, &m31.RadialData)
		case "REF":
			fallthrough
		case "VEL":
			fallthrough
		case "SW ":
			fallthrough
		case "ZDR":
			fallthrough
		case "PHI":
			fallthrough
		case "RHO":
			fallthrough
		case "CFP":
			m := GenericDataMoment{}
			binary.Read(r, binary.BigEndian, &m)

			// LDM is the amount of space in bytes required for a data moment
			// array and equals ((NG * DWS) / 8) where NG is the number of gates
			// at the gate spacing resolution specified and DWS is the number of
			// bits stored for each gate (DWS is always a multiple of 8).
			ldm := m.NumberDataMomentGates * uint16(m.DataWordSize) / 8
			data := make([]uint8, ldm)
			binary.Read(r, binary.BigEndian, &data)

			d := &DataMoment{
				GenericDataMoment: m,
				Data:              data,
			}

			switch blockName {
			case "REF":
				m31.ReflectivityData = d
			case "VEL":
				m31.VelocityData = d
			case "SW ":
				m31.SwData = d
			case "ZDR":
				m31.ZdrData = d
			case "PHI":
				m31.PhiData = d
			case "RHO":
				m31.RhoData = d
			case "CFP":
				m31.CfpData = d
			}
		default:
			logrus.Panicf("Data Block - unknown type '%s'", blockName)
		}
	}
	return &m31
}
