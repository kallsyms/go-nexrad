package main

import (
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/llgcode/draw2d"

	"golang.org/x/image/colornames"
	"golang.org/x/image/font"
	"golang.org/x/image/math/fixed"

	"github.com/cheggaaa/pb/v3"
	"github.com/kallsyms/go-nexrad/archive2"
	"github.com/llgcode/draw2d/draw2dimg"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/image/font/inconsolata"
)

var cmd = &cobra.Command{
	Use:   "nexrad-render",
	Short: "nexrad-render generates products from NEXRAD Level 2 (archive 2) data files.",
	Run:   run,
}

var inputFile string
var outputFile string
var colorScheme string
var logLevel string
var directory string
var renderLabel bool
var product string
var imageSize int32
var runners int
var products []string

var colorSchemes map[string]map[string]func(float32) color.Color

func init() {
	cmd.PersistentFlags().StringVarP(&inputFile, "file", "f", "", "archive 2 file to process")
	cmd.PersistentFlags().StringVarP(&outputFile, "output", "o", "", "output radar image")
	cmd.PersistentFlags().StringVarP(&product, "product", "p", "ref", "product to produce. ex: ref, vel, sw, rho")
	cmd.PersistentFlags().StringVarP(&colorScheme, "color-scheme", "c", "noaa", "color scheme to use. noaa, radarscope, pink")
	cmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "warn", "log level, debug, info, warn, error")
	cmd.PersistentFlags().Int32VarP(&imageSize, "size", "s", 1024, "size in pixel of the output image")
	cmd.PersistentFlags().IntVarP(&runners, "threads", "t", runtime.NumCPU(), "threads")
	cmd.PersistentFlags().StringVarP(&directory, "directory", "d", "", "directory of L2 files to process")
	cmd.PersistentFlags().BoolVarP(&renderLabel, "label", "L", false, "label the image with station and date")

	products = []string{"ref", "vel", "sw", "rho"}

	colorSchemes = make(map[string]map[string]func(float32) color.Color)
	colorSchemes["ref"] = map[string]func(float32) color.Color{
		"noaa":          dbzColorNOAA,
		"radarscope":    dbzColorScope,
		"scope-classic": dbzColorScopeClassic,
		"pink":          dbzColor,
		"clean-air":     dbzColorCleanAirMode,
	}
	colorSchemes["vel"] = map[string]func(float32) color.Color{
		"noaa":       velColorRadarscope, // placeholder for default product value
		"radarscope": velColorRadarscope,
	}
	colorSchemes["sw"] = map[string]func(float32) color.Color{
		"noaa": swColor,
	}
	colorSchemes["rho"] = map[string]func(float32) color.Color{
		"noaa": rhoColor,
	}
}

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) {

	if _, ok := colorSchemes[product][colorScheme]; !ok {
		logrus.Fatal(fmt.Sprintf("unsupported %s colorscheme %s", product, colorScheme))
	}

	lvl, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.Fatalf("failed to parse level: %s", err)
	}
	logrus.SetLevel(lvl)

	if inputFile != "" {
		out := "radar.png"
		if outputFile != "" {
			out = outputFile
		}
		single(inputFile, out, product)
	} else if directory != "" {
		out := "out"
		if outputFile != "" {
			out = outputFile
		}
		animate(directory, out, product)
	}
}

func animate(dir, outdir, prod string) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logrus.Fatal(err)
	}

	// create the output dir
	if _, err := os.Stat(outdir); os.IsNotExist(err) {
		os.Mkdir(outdir, os.ModePerm)
	}

	bar := pb.StartNew(len(files))

	source := make(chan string, runners)
	wg := sync.WaitGroup{}
	wg.Add(runners)
	for i := 0; i < runners; i++ {
		go func(i int) {
			for l2f := range source {
				outf := fmt.Sprintf("%s/%s.png", outdir, l2f)
				// fmt.Printf("Generating %s from %s -> %s\n", prod, l2f, outf)
				f, err := os.Open(dir + "/" + l2f)
				if err != nil {
					logrus.Error(err)
					return
				}
				ar2, err := archive2.Extract(f)
				if err != nil {
					logrus.Panic(err)
				}
				f.Close()
				elv := 1
				if prod == "vel" {
					elv = 2
				}
				render(outf, ar2.ElevationScans[elv], fmt.Sprintf("%s - %s", ar2.VolumeHeader.ICAO, ar2.VolumeHeader.Date()))
				bar.Increment()
			}
			wg.Done()
		}(i)
	}

	for _, fn := range files {
		if strings.HasSuffix(fn.Name(), ".ar2v") {
			source <- fn.Name()
		} else {
			bar.Increment()
		}
	}
	close(source)
	wg.Wait()
	bar.Finish()
}

func single(in, out, product string) {
	fmt.Printf("Generating %s from %s -> %s\n", strings.ToUpper(product), in, out)

	f, err := os.Open(in)
	defer f.Close()
	if err != nil {
		logrus.Error(err)
		return
	}

	ar2, err := archive2.Extract(f)
	if err != nil {
		logrus.Panic(err)
	}
	fmt.Println(ar2)
	elv := 1
	// if product != "ref" {
	// elv = 2 // uhhh, why did i do this again?
	// }
	label := fmt.Sprintf("%s %f %s VCP:%d %s %s", ar2.VolumeHeader.ICAO, ar2.ElevationScans[2][0].Header.ElevationAngle, strings.ToUpper(product), ar2.RadarStatus.VolumeCoveragePatternNum, ar2.VolumeHeader.FileName(), ar2.VolumeHeader.Date().Format(time.RFC3339))
	render(out, ar2.ElevationScans[elv], label)
}

func render(out string, radials []*archive2.Message31, label string) {

	width := float64(imageSize)
	height := float64(imageSize)

	canvas := image.NewRGBA(image.Rect(0, 0, int(width), int(height)))
	draw.Draw(canvas, canvas.Bounds(), image.Black, image.ZP, draw.Src)

	gc := draw2dimg.NewGraphicContext(canvas)

	xc := width / 2
	yc := height / 2
	pxPerKm := width / 2 / 460
	// spew.Dump(radials)
	firstGatePx := float64(radials[0].ReflectivityData.DataMomentRange) / 1000 * pxPerKm
	gateIntervalKm := float64(radials[0].ReflectivityData.DataMomentRangeSampleInterval) / 1000
	gateWidthPx := gateIntervalKm * pxPerKm

	log.Println("rendering radials")
	// valueDist := map[float32]int{}

	for _, radial := range radials {
		// round to the nearest rounded azimuth for the given resolution.
		// ex: for radial 20.5432, round to 20.5
		azimuthAngle := float64(radial.Header.AzimuthAngle) - 90
		if azimuthAngle < 0 {
			azimuthAngle = 360.0 + azimuthAngle
		}
		azimuthSpacing := radial.Header.AzimuthResolutionSpacing()
		azimuth := math.Floor(azimuthAngle)
		if math.Floor(azimuthAngle+azimuthSpacing) > azimuth {
			azimuth += azimuthSpacing
		}
		startAngle := azimuth * (math.Pi / 180.0)      /* angles are specified */
		endAngle := azimuthSpacing * (math.Pi / 180.0) /* clockwise in radians           */

		// start drawing gates from the start of the first gate
		distanceX, distanceY := firstGatePx, firstGatePx
		gc.SetLineWidth(gateWidthPx + 1)
		gc.SetLineCap(draw2d.ButtCap)

		var gates []float32
		switch product {
		case "vel":
			gates = radial.VelocityData.ScaledData()
		case "sw":
			gates = radial.SwData.ScaledData()
		case "rho":
			gates = radial.RhoData.ScaledData()
		default:
			gates = radial.ReflectivityData.ScaledData()
		}

		numGates := len(gates)
		for i, v := range gates {
			if v != archive2.MomentDataBelowThreshold {

				// valueDist[v] += 1

				gc.MoveTo(xc+math.Cos(startAngle)*distanceX, yc+math.Sin(startAngle)*distanceY)

				// make the gates connect visually by extending arcs so there is no space between adjacent gates.
				if i == 0 {
					gc.ArcTo(xc, yc, distanceX, distanceY, startAngle-.001, endAngle+.001)
				} else if i == numGates-1 {
					gc.ArcTo(xc, yc, distanceX, distanceY, startAngle, endAngle)
				} else {
					gc.ArcTo(xc, yc, distanceX, distanceY, startAngle, endAngle+.001)
				}

				gc.SetStrokeColor(colorSchemes[product][colorScheme](v))
				gc.Stroke()
			}

			distanceX += gateWidthPx
			distanceY += gateWidthPx
			azimuth += radial.Header.AzimuthResolutionSpacing()
		}
	}

	// fmt.Println(valueDist)

	if renderLabel {
		addLabel(canvas, int(width-495.0), int(height-10.0), label)
	}

	// Save to file
	draw2dimg.SaveToPngFile(out, canvas)
}

func addLabel(img *image.RGBA, x, y int, label string) {
	point := fixed.Point26_6{fixed.Int26_6(x * 64), fixed.Int26_6(y * 64)}

	d := &font.Drawer{
		Dst:  img,
		Src:  image.NewUniform(colornames.Gray),
		Face: inconsolata.Bold8x16,
		Dot:  point,
	}
	d.DrawString(label)
}

// scaleInt scales a number form one range to another range
func scaleInt(value, oldMax, oldMin, newMax, newMin int32) int32 {
	oldRange := (oldMax - oldMin)
	newRange := (newMax - newMin)
	return (((value - oldMin) * newRange) / oldRange) + newMin
}
