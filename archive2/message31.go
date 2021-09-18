package archive2

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

var (
	pointersPerBuild = map[float32]int{
		18: 9,
		19: 10,
	}
)

// Message31Header is the non-data portions of Message31 (User 3.2.4.17)
type Message31Header struct {
	RadarIdentifier              [4]byte // ICAO (eg KMPX for Minneapolis)
	CollectionTime               uint32  // CollectionTime Radial data collection time in milliseconds past midnight GMT
	CollectionDate               uint16  // CollectionDate Current Julian date - 2440586.5
	AzimuthNumber                uint16  // AzimuthNumber Radial number within elevation scan
	AzimuthAngle                 float32 // AzimuthAngle Azimuth angle at which radial data was collected
	CompressionIndicator         uint8   // CompressionIndicator Indicates if message type 31 is compressed and what method of compression is used. The Data Header Block is not compressed.
	Spare                        uint8   // unused
	RadialLength                 uint16  // RadialLength Uncompressed length of the radial in bytes including the Data Header block length
	AzimuthResolutionSpacingCode uint8   // AzimuthResolutionSpacing Code for the Azimuthal spacing between adjacent radials. 1 = .5 degrees, 2 = 1degree
	RadialStatus                 uint8   // RadialStatus Radial Status
	ElevationNumber              uint8   // ElevationNumber Elevation number within volume scan
	CutSectorNumber              uint8   // CutSectorNumber Sector Number within cut
	ElevationAngle               float32 // ElevationAngle Elevation angle at which radial radar data was collected
	RadialSpotBlankingStatus     uint8   // RadialSpotBlankingStatus Spot blanking status for current radial, elevation scan and volume scan
	AzimuthIndexingMode          uint8   // AzimuthIndexingMode Azimuth indexing value (Set if azimuth angle is keyed to constant angles)
	DataBlockCount               uint16  // Number of data blocks used
	// normally this would be the data block pointers, but we dont actually use this
}

func (h Message31Header) String() string {
	return fmt.Sprintf("Message 31 - %s @ %v deg=%.2f tilt=%.2f",
		string(h.RadarIdentifier[:]),
		h.Date(),
		h.AzimuthAngle,
		h.ElevationAngle,
	)
}

// Date and time this data is valid for
func (h Message31Header) Date() time.Time {
	return time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(h.CollectionDate) * time.Hour * 24).
		Add(time.Duration(h.CollectionTime) * time.Millisecond)
}

// Message31 - Digital Radar Data Generic Format (User 3.2.4.17)
type Message31 struct {
	Header        Message31Header
	VolumeData    VolumeData
	ElevationData ElevationData
	RadialData    RadialData
	REFData       DataMoment
	VELData       DataMoment
	SWData        DataMoment
	ZDRData       DataMoment
	PHIData       DataMoment
	RHOData       DataMoment
	CFPData       DataMoment
}

// NewMessage31 from the provided io.Reader
func NewMessage31(r io.Reader, build float32) (*Message31, error) {
	header := Message31Header{}
	binary.Read(r, binary.BigEndian, &header)

	// skip over the data block pointers, which is build dependent
	binary.Read(r, binary.BigEndian, make([]uint32, pointersPerBuild[build]))

	m31 := Message31{
		Header: header,
	}

	for i := uint16(0); i < header.DataBlockCount; i++ {
		d := DataBlock{}
		if err := binary.Read(r, binary.BigEndian, &d); err != nil {
			return nil, err
		}

		blockName := string(d.DataName[:])
		switch blockName {
		case "VOL":
			binary.Read(r, binary.BigEndian, &m31.VolumeData)
		case "ELV":
			binary.Read(r, binary.BigEndian, &m31.ElevationData)
		case "RAD":
			binary.Read(r, binary.BigEndian, &m31.RadialData)
		case "REF", "VEL", "SW ", "ZDR", "PHI", "RHO", "CFP":
			m := GenericDataMoment{}
			binary.Read(r, binary.BigEndian, &m)

			// the data moment length is determined with (num gates * word size) / 8.
			dataMomentSize := m.NumberDataMomentGates * uint16(m.DataWordSize) / 8
			data := make([]uint8, dataMomentSize)
			io.ReadFull(r, data)

			moment := DataMoment{
				GenericDataMoment: m,
				Data:              data,
			}

			switch blockName {
			case "REF":
				m31.REFData = moment
			case "VEL":
				m31.VELData = moment
			case "SW ":
				m31.SWData = moment
			case "ZDR":
				m31.ZDRData = moment
			case "PHI":
				m31.PHIData = moment
			case "RHO":
				m31.RHOData = moment
			case "CFP":
				m31.CFPData = moment
			}
		default:
			return nil, fmt.Errorf("Data Block - unknown type '%s'", blockName)
		}
	}
	return &m31, nil
}

// AzimuthResolutionSpacing returns the spacing in degrees
func (h *Message31) AzimuthResolutionSpacing() float32 {
	if h.Header.AzimuthResolutionSpacingCode == 1 {
		return 0.5
	}
	return 1
}

// DataBlock is sort of like the header for the blocks of data (GenericDataMoment, VolumeData, etc). These 4 bytes are
// normally found at the top of tables XVII-[BEFH] (User 3.2.4.17)
type DataBlock struct {
	DataBlockType [1]byte
	DataName      [3]byte
}

// GenericDataMoment is a generic data wrapper for momentary data. ex: REF, VEL, SW data (User 3.2.4.17.2)
type GenericDataMoment struct {
	// data block type and data moment name are retrieved separately
	Reserved                      uint32  //
	NumberDataMomentGates         uint16  // NumberDataMomentGates Number of data moment gates for current radial
	DataMomentRange               uint16  // DataMomentRange Range to center of first range gate
	DataMomentRangeSampleInterval uint16  // DataMomentRangeSampleInterval Size of data moment sample interval
	TOVER                         uint16  // TOVER Threshold parameter which specifies the minimum difference in echo power between two resolution gates for them not to be labeled "overlayed"
	SNRThreshold                  uint16  // SNRThreshold SNR threshold for valid data
	ControlFlags                  uint8   // ControlFlags Indicates special control features
	DataWordSize                  uint8   // DataWordSize Number of bits (DWS) used for storing data for each Data Moment gate
	Scale                         float32 // Scale value used to convert Data Moments from integer to floating point data
	Offset                        float32 // Offset value used to convert Data Moments from integer to floating point data
}

// VolumeData wraps information about the Volume being extracted (User 3.2.4.17.3)
type VolumeData struct {
	// data block type and data moment name are retrieved separately
	LRTUP                          uint16 // LRTUP Size of data block in bytes
	VersionMajor                   uint8
	VersionMinor                   uint8
	Lat                            float32
	Long                           float32
	SiteHeight                     uint16
	FeedhornHeight                 uint16
	CalibrationConstant            float32
	SHVTXPowerHor                  float32
	SHVTXPowerVer                  float32
	SystemDifferentialReflectivity float32
	InitialSystemDifferentialPhase float32
	VolumeCoveragePatternNumber    uint16
	ProcessingStatus               uint16
}

// ElevationData wraps Message 31 elevation data (User 3.2.4.17.4)
type ElevationData struct {
	// data block type and data moment name are retrieved separately
	LRTUP      uint16  // LRTUP Size of data block in bytes
	ATMOS      [2]byte // ATMOS Atmospheric Attenuation Factor
	CalibConst float32 // CalibConst Scaling constant used by the Signal Processor for this elevation to calculate reflectivity
}

// RadialData wraps Message 31 radial data (User 3.2.4.17.5)
type RadialData struct {
	// data block type and data moment name are retrieved separately
	LRTUP              uint16 // LRTUP Size of data block in bytes
	UnambiguousRange   uint16 // UnambiguousRange, Interval Size
	NoiseLevelHorz     float32
	NoiseLevelVert     float32
	NyquistVelocity    uint16
	Spares             [2]byte
	CalibConstHorzChan float32
	CalibConstVertChan float32
}

// DataMoment wraps all Momentary data records. ex: REF, VEL, SW data. Data interpretation provided by User 3.2.4.17.6.
type DataMoment struct {
	GenericDataMoment
	Data []byte
}

const (
	// MomentDataBelowThreshold ...
	MomentDataBelowThreshold = 999

	// MomentDataFolded ...
	MomentDataFolded = 998
)

// ScaledData automatically scales the nexrad moment values to their actual values.
// For all data moment integer values N = 0 indicates received signal is below
// threshold and N = 1 indicates range folded data. Actual data range is N = 2
// through 255, or 1023 for data resolution size 8, and 10 bits respectively.
func (d *DataMoment) ScaledData() []float32 {
	scaledData := make([]float32, len(d.Data))
	for idx, val := range d.Data {
		if val == 0 {
			// below threshold
			scaledData[idx] = MomentDataBelowThreshold
		} else if val == 1 {
			// range folded
			scaledData[idx] = MomentDataFolded
		} else {
			scaledData[idx] = scaleUint(uint16(val), d.GenericDataMoment.Offset, d.GenericDataMoment.Scale)
		}
	}
	return scaledData
}

// scaleUint converts unsigned integer data that can be converted to floating point
// data using the Scale and Offset fields, i.e., F = (N - OFFSET) / SCALE where
// N is the integer data value and F is the resulting floating point value. A
// scale value of 0 indicates floating point moment data for each range gate.
func scaleUint(n uint16, offset, scale float32) float32 {
	val := float32(n)
	if scale == 0 {
		return val
	}
	return (val - offset) / scale
}
