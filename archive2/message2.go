package archive2

// Message2 RDA Status Data (User 3.2.4.6)
type Message2 struct {
	RDAStatus                       uint16
	OperabilityStatus               uint16
	ControlStatus                   uint16
	AuxPowerGeneratorState          uint16
	AvgTxPower                      uint16
	HorizRefCalibCorr               uint16
	DataTxEnabled                   uint16
	VolumeCoveragePatternNum        uint16
	RDAControlAuth                  uint16
	RDABuild                        uint16
	OperationalMode                 uint16
	SuperResStatus                  uint16
	ClutterMitigationDecisionStatus uint16
	AvsetStatus                     uint16
	RDAAlarmSummary                 uint16
	CommandAck                      uint16
	ChannelControlStatus            uint16
	SpotBlankingStatus              uint16
	BypassMapGenDate                uint16
	BypassMapGenTime                uint16
	ClutterFilterMapGenDate         uint16
	ClutterFilterMapGenTime         uint16
	VertRefCalibCorr                uint16
	TransitionPwrSourceStatus       uint16
	RMSControlStatus                uint16
	PerformanceCheckStatus          uint16
	AlarmCodes                      uint16
	Spares                          [20]byte
}
