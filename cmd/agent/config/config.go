package config

const (
	DefaultPath                = "/etc/kcover-agent/config.yaml"
	DefaultVendor              = 1
	DefaultInterval            = 5
	DefaultMetaXDay2CheckHour  = 10
	DefaultMetaXGPUNum         = 8
	DefaultMetaXTemperature    = 85
	DefaultMetaXECCMaxCount    = 64
	DefaultMetaXNTPMaxOffsetMS = 10
)

type Agent struct {
	Vendor   int   `yaml:"vendor"`
	Interval int   `yaml:"interval"`
	MetaX    MetaX `yaml:"metaX"`
}

type MetaX struct {
	NodeName           string   `yaml:"-"`
	HCAIDs             []string `yaml:"hcaIDs"`
	GPUNum             int      `yaml:"gpuNum"`
	Temperature        int      `yaml:"temperature"`
	ECCMaxCount        int      `yaml:"eccMaxCount"`
	NTPMaxOffsetMillis int32    `yaml:"ntpMaxOffsetMillis"`
	Day2CheckHour      int      `yaml:"day2CheckHour"`
}

func DefaultAgent() Agent {
	return Agent{
		Vendor:   DefaultVendor,
		Interval: DefaultInterval,
		MetaX: MetaX{
			GPUNum:             DefaultMetaXGPUNum,
			Temperature:        DefaultMetaXTemperature,
			ECCMaxCount:        DefaultMetaXECCMaxCount,
			NTPMaxOffsetMillis: DefaultMetaXNTPMaxOffsetMS,
			Day2CheckHour:      DefaultMetaXDay2CheckHour,
		},
	}
}

func (cfg *Agent) ApplyDefaults() {
	if cfg.Vendor <= 0 {
		cfg.Vendor = DefaultVendor
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.MetaX.Day2CheckHour < 0 || cfg.MetaX.Day2CheckHour > 23 {
		cfg.MetaX.Day2CheckHour = DefaultMetaXDay2CheckHour
	}
	if cfg.MetaX.NTPMaxOffsetMillis <= 0 {
		cfg.MetaX.NTPMaxOffsetMillis = DefaultMetaXNTPMaxOffsetMS
	}
	if cfg.MetaX.GPUNum < 0 {
		cfg.MetaX.GPUNum = DefaultMetaXGPUNum
	}
	if cfg.MetaX.Temperature <= 0 {
		cfg.MetaX.Temperature = DefaultMetaXTemperature
	}
	if cfg.MetaX.ECCMaxCount < 0 {
		cfg.MetaX.ECCMaxCount = DefaultMetaXECCMaxCount
	}
}
