package main

import (
	"os"
	"runtime/pprof"

	"github.com/fatih/color"
	"github.com/jddeal/go-nexrad/archive2"
	"github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"
)

var cli struct {
	Args struct {
		Filename string
	} `positional-args:"yes" required:"yes"`
	LogLevel         string `short:"l" long:"log-level" description:"logging level" choice:"error" choice:"info" choice:"debug" choice:"trace" default:"info"`
	ShowVolumeHeader bool   `long:"show-volume-header" description:"dumps out the contents of the Volume Header"`
}

func main() {

	// parse the input args
	_, err := flags.Parse(&cli)
	if err != nil {
		os.Exit(1)
	}

	// set the logging level
	errorLevels := map[string]logrus.Level{
		"error": logrus.ErrorLevel,
		"info":  logrus.InfoLevel,
		"debug": logrus.DebugLevel,
		"trace": logrus.TraceLevel,
	}
	logrus.SetLevel(errorLevels[cli.LogLevel])

	// uncomment below to enable profiling, then run `go tool pprof out.prof` and `top10` in the pprof prompt
	f, _ := os.Create("out.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// decode it
	logrus.Info(color.CyanString("decoding ", cli.Args.Filename))
	archive2.NewArchive2(cli.Args.Filename)
}
