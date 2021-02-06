// Package archive2 provides structs and functions for decoding NEXRAD Archive II files.
//
// The documents used and referenced in this package:
//  • RDA/RPG: https://www.roc.noaa.gov/wsr88d/PublicDocs/ICDs/2620002T.pdf (high level details)
//  • User: https://www.roc.noaa.gov/wsr88d/PublicDocs/ICDs/2620010H.pdf (bulk of the format)
package archive2

import "time"

const (
	radialStatusStartOfElevationScan   = 0
	radialStatusIntermediateRadialData = 1
	radialStatusEndOfElevation         = 2
	radialStatusBeginningOfVolumeScan  = 3
	radialStatusEndOfVolumeScan        = 4
	radialStatusStartNewElevation      = 5

	// LegacyCTMHeaderLength sits in front of every message header
	LegacyCTMHeaderLength = 12

	// DefaultMetadataRecordLength is the size of every record regardless of its contents
	DefaultMetadataRecordLength = 2432
)

// VolumeHeaderRecord for NEXRAD Archive II Data Streams (RDA/RPG 7.3.3)
type VolumeHeaderRecord struct {
	TapeFilename    [9]byte // eg "AR2V0006"
	ExtensionNumber [3]byte // eg "001" (cycles through 0-999)
	ModifiedDate    int32   // data's valid date (julian day since 1970)
	ModifiedTime    int32   // data's valid time (milliseconds past midnight)
	ICAO            [4]byte // radar identifier
}

// Filename for this archive file
func (vh VolumeHeaderRecord) Filename() string {
	return string(vh.TapeFilename[:]) + string(vh.ExtensionNumber[:])
}

// Date and time this data is valid for
func (vh VolumeHeaderRecord) Date() time.Time {
	return time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(vh.ModifiedDate) * time.Hour * 24).
		Add(time.Duration(vh.ModifiedTime) * time.Millisecond)
}

// LDMRecord (Local Data Manager) wraps every radar message in bzip2 compression. (RDA/RPG 7.3.4)
type LDMRecord struct {
	Size           int32
	MetaDataRecord []byte
}

// MessageHeader provides a high level description for a particular message. (User 3.2.4.1)
type MessageHeader struct {
	MessageSize         uint16
	RDARedundantChannel uint8
	MessageType         uint8
	IDSequenceNumber    uint16
	JulianDate          uint16
	MillisOfDay         uint32
	NumMessageSegments  uint16
	MessageSegmentNum   uint16
}

// See the individual messageXX.go files for message specific types.
