package main

import (
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/llgcode/draw2d"
	"github.com/lukeroth/gdal"
	"github.com/sirupsen/logrus"

	"golang.org/x/image/colornames"

	"github.com/jddeal/go-nexrad/archive2"
	"github.com/llgcode/draw2d/draw2dimg"

	"github.com/gorilla/mux"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var colorSchemes map[string]map[string]func(float32) color.Color

func init() {
	colorSchemes = make(map[string]map[string]func(float32) color.Color)
	colorSchemes["REF"] = map[string]func(float32) color.Color{
		"noaa":          dbzColorNOAA,
		"radarscope":    dbzColorScope,
		"scope-classic": dbzColorScopeClassic,
		"pink":          dbzColor,
		"clean-air":     dbzColorCleanAirMode,
	}
	colorSchemes["VEL"] = map[string]func(float32) color.Color{
		"noaa":       velColorRadarscope, // placeholder for default product value
		"radarscope": velColorRadarscope,
	}
}

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	// l3 format is l3/{site}/{product}/{filename}/render
	r := mux.NewRouter()
	r.HandleFunc("/l2/{fn}/{elv}/{product}/render", renderHandler)
	r.HandleFunc("/l2/{fn}.json", metaHandler)
	r.HandleFunc("/l2/realtime/{site}/{volume}/{elv}/{product}/render", realtimeRenderHandler)
	r.HandleFunc("/l2/realtime/{site}/{volume}.json", realtimeMetaHandler)

	srv := &http.Server{
		Addr: "0.0.0.0:8081",
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r, // Pass our instance of gorilla/mux in.
	}

	if err := srv.ListenAndServe(); err != nil {
		fmt.Println(err)
	}
}

func loadArchive2Realtime(site string, volume int) (*archive2.Archive2, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Region:      aws.String("us-east-1"),
	})
	svc := s3.New(sess)
	bucket := aws.String("unidata-nexrad-level2-chunks")

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: aws.String(fmt.Sprintf("%s/%d/", site, volume)),
	})
	if err != nil {
		return nil, err
	}

	headerFile, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: bucket,
		Key:    resp.Contents[0].Key,
	})
	if err != nil {
		return nil, err
	}

	ar2, err := archive2.NewArchive2(headerFile.Body)
	headerFile.Body.Close()
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	for _, chunkObjectInfo := range resp.Contents[1:] {
		wg.Add(1)
		go func(chunkObjectInfo *s3.Object) {
			defer wg.Done()

			chunk, err := svc.GetObject(&s3.GetObjectInput{
				Bucket: bucket,
				Key:    chunkObjectInfo.Key,
			})
			if err != nil {
				return
			}

			ar2.AddFromLDMRecord(chunk.Body)
			chunk.Body.Close()
		}(chunkObjectInfo)
	}
	wg.Wait()

	return ar2, nil
}

func loadArchive2(fn string) (*archive2.Archive2, error) {
	// fn is like KOKX20210902_000428_V06
	site := fn[:4]
	date, err := time.Parse("20060102_150405", fn[4:19])
	if err != nil {
		return nil, err
	}

	radResp, err := http.Get("https://noaa-nexrad-level2.s3.amazonaws.com/" + date.Format("2006/01/02/") + site + "/" + fn)
	if err != nil {
		return nil, err
	}

	defer radResp.Body.Close()

	if radResp.StatusCode != 200 {
		return nil, fmt.Errorf("Bad status code fetching file: %d", radResp.StatusCode)
	}

	return archive2.NewArchive2(radResp.Body)
}

func writeMeta(w http.ResponseWriter, ar2 *archive2.Archive2) {
	headers := make([]*archive2.Message31Header, len(ar2.ElevationScans))
	for elv, m31s := range ar2.ElevationScans {
		headers[elv-1] = &m31s[0].Header
	}
	j, _ := json.Marshal(headers)
	w.Write(j)
}

func metaHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	fn := vars["fn"]

	ar2, err := loadArchive2(fn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeMeta(w, ar2)
}

func realtimeMetaHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	site := vars["site"]
	volume, err := strconv.Atoi(vars["volume"])
	if err != nil {
		http.Error(w, "Invalid volume number", http.StatusBadRequest)
		return
	}

	ar2, err := loadArchive2Realtime(site, volume)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeMeta(w, ar2)
}

func renderHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	fn := vars["fn"]
	elv, err := strconv.Atoi(vars["elv"])
	if err != nil {
		http.Error(w, "Invalid elv", http.StatusBadRequest)
		return
	}

	product := vars["product"]

	ar2, err := loadArchive2(fn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	warpedDS := renderAndReproject(ar2.ElevationScans[elv], product, 6000, 2600)
	png.Encode(w, warpedDS.Image)
	warpedDS.DS.Close()
}

func realtimeRenderHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	site := vars["site"]
	volume, err := strconv.Atoi(vars["volume"])
	if err != nil {
		http.Error(w, "Invalid volume number", http.StatusBadRequest)
		return
	}

	elv, err := strconv.Atoi(vars["elv"])
	if err != nil {
		http.Error(w, "Invalid elv", http.StatusBadRequest)
		return
	}

	product := vars["product"]

	ar2, err := loadArchive2Realtime(site, volume)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	warpedDS := renderAndReproject(ar2.ElevationScans[elv], product, 6000, 2600)
	png.Encode(w, warpedDS.Image)
	warpedDS.DS.Close()
}

func renderAndReproject(radials []*archive2.Message31, product string, width, height int) imageDS {
	intermediateSize := 1000 // width and height of initial rendered image before projection
	renderDS := getRenderDS(radials, product, intermediateSize, "noaa")
	defer renderDS.DS.Close()

	warpedImg := image.NewRGBA(image.Rect(0, 0, width, height))
	warpedDS := makeImageDS(warpedImg)

	spatialRef := gdal.CreateSpatialReference("")
	spatialRef.FromEPSG(3857)
	srString, _ := spatialRef.ToWKT()
	warpedDS.DS.SetProjection(srString)

	// warpedDS.SetGeoTransform([6]float64{
	// 	-125,  // upper-left x coord
	// 	(125.0-65.0)/float64(warpedImg.Rect.Dx()),  // x pixel res
	// 	0, // row rotation
	// 	50,  // upper-left y coord
	// 	0, // col rotation
	// 	(50.0-25.0)/float64(warpedImg.Rect.Dy()),  // y pixel res
	// })

	// gdaltransform -s_srs epsg:4326 -t_srs epsg:3857
	// -125 50
	// -13914936.3491592 6446275.84101716 0
	upperLeftX := -13914936.3491592
	upperLeftY := 6446275.84101716
	// -65 25
	// -7235766.90156278 2875744.62435224 0
	lowerRightX := -7235766.90156278
	lowerRightY := 2875744.62435224
	warpedDS.DS.SetGeoTransform([6]float64{
		upperLeftX,
		(lowerRightX - upperLeftX) / float64(warpedDS.Image.Rect.Dx()),
		0,
		upperLeftY,
		0,
		(lowerRightY - upperLeftY) / float64(warpedDS.Image.Rect.Dy()),
	})

	gdal.Warp("", &warpedDS.DS, []gdal.Dataset{renderDS.DS}, []string{
		"-srcalpha",
		"-dstalpha",
	})

	return warpedDS
}

// little helper to keep both a GDAL dataset and the DS's backing Image together
// both for convenience but also to make sure the DS's backing memory doesn't get GCd
type imageDS struct {
	DS    gdal.Dataset
	Image *image.RGBA
}

func getRenderDS(radials []*archive2.Message31, product string, imageSize int, colorScheme string) imageDS {
	renderImg := render(radials, product, imageSize, "noaa")
	renderDS := makeImageDS(renderImg)

	// from pyart's projection: https://github.com/ARM-DOE/pyart/blob/master/pyart/io/output_to_geotiff.py#L119
	renderDS.DS.SetProjection(fmt.Sprintf(
		`PROJCS[
			"unnamed",
			GEOGCS[
				"WGS 84",
				DATUM[
					"unknown",
					SPHEROID["WGS84",6378137,298.257223563]
				],
				PRIMEM["Greenwich",0],
				UNIT["degree",0.0174532925199433]
			],
			PROJECTION["Azimuthal_Equidistant"],
			PARAMETER["latitude_of_center",%f],
			PARAMETER["longitude_of_center",%f],
			PARAMETER["false_easting",0],
			PARAMETER["false_northing",0],
			UNIT["metre",1,AUTHORITY["EPSG","9001"]]
		]`,
		radials[0].VolumeData.Lat,
		radials[0].VolumeData.Long,
	))

	distM := float64(460 * 1000) // what the min/max x/y bounds are, in meters
	pixStepM := distM * 2.0 / float64(renderImg.Rect.Dx())
	renderDS.DS.SetGeoTransform([6]float64{-distM, pixStepM, 0, distM, 0, -pixStepM})

	// sanity check renderDS was loaded properly
	// gdal.Translate("/tmp/o.png", renderDS, []string{})

	return renderDS
}

func makeImageDS(warpedImg *image.RGBA) imageDS {
	warpedDSName := fmt.Sprintf(
		"MEM:::DATAPOINTER=%p,PIXELS=%d,LINES=%d,BANDS=4,DATATYPE=Byte,PIXELOFFSET=4,BANDOFFSET=1",
		&warpedImg.Pix[0],
		warpedImg.Rect.Dx(),
		warpedImg.Rect.Dy(),
	)

	warpedDS, _ := gdal.Open(warpedDSName, gdal.Update)

	return imageDS{
		warpedDS,
		warpedImg,
	}
}

func render(radials []*archive2.Message31, product string, imageSize int, colorScheme string) *image.RGBA {
	width := float64(imageSize)
	height := float64(imageSize)

	canvas := image.NewRGBA(image.Rect(0, 0, imageSize, imageSize))
	draw.Draw(canvas, canvas.Bounds(), image.Transparent, image.ZP, draw.Src)

	gc := draw2dimg.NewGraphicContext(canvas)

	xc := width / 2
	yc := height / 2
	pxPerKm := width / 2 / 460 // NEXRAD goes to 460km
	firstGatePx := float64(radials[0].REFData.DataMomentRange) / 1000 * pxPerKm
	gateIntervalKm := float64(radials[0].REFData.DataMomentRangeSampleInterval) / 1000
	gateWidthPx := gateIntervalKm * pxPerKm

	for _, radial := range radials {
		// round to the nearest rounded azimuth for the given resolution.
		// ex: for radial 20.5432, round to 20.5
		azimuthAngle := float64(radial.Header.AzimuthAngle) - 90
		if azimuthAngle < 0 {
			azimuthAngle = 360.0 + azimuthAngle
		}
		azimuthSpacing := radial.AzimuthResolutionSpacing()
		azimuth := math.Floor(azimuthAngle)
		if math.Floor(azimuthAngle+float64(azimuthSpacing)) > azimuth {
			azimuth += float64(azimuthSpacing)
		}
		startAngle := float64(azimuth * (math.Pi / 180.0))      /* angles are specified */
		endAngle := float64(azimuthSpacing * (math.Pi / 180.0)) /* clockwise in radians           */

		// start drawing gates from the start of the first gate
		distanceX, distanceY := firstGatePx, firstGatePx
		gc.SetLineWidth(gateWidthPx + 1)
		gc.SetLineCap(draw2d.ButtCap)

		var gates []float32
		switch product {
		case "REF":
			gates = radial.REFData.ScaledData()
		case "VEL":
			gates = radial.VELData.ScaledData()

		}

		numGates := len(gates)
		for i, v := range gates {
			if v != archive2.MomentDataBelowThreshold {
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
			azimuth += float64(radial.AzimuthResolutionSpacing())
		}
	}

	return canvas
}

func dbzColor(dbz float32) color.Color {
	if dbz < 5.0 {
		return colornames.Black
	} else if dbz >= 5.0 && dbz < 10.0 {
		return color.NRGBA{0x9C, 0x9C, 0x9C, 0xFF}
	} else if dbz >= 10.0 && dbz < 15.0 {
		return color.NRGBA{0x76, 0x76, 0x76, 0xFF}
	} else if dbz >= 15.0 && dbz < 20.0 {
		return color.NRGBA{0xFF, 0xAA, 0xAA, 0xFF}
	} else if dbz >= 20.0 && dbz < 25.0 {
		return color.NRGBA{0xEE, 0x8C, 0x8C, 0xFF}
	} else if dbz >= 25.0 && dbz < 30.0 {
		return color.NRGBA{0xC9, 0x70, 0x70, 0xFF}
	} else if dbz >= 30.0 && dbz < 35.0 {
		return color.NRGBA{0x00, 0xFB, 0x90, 0xFF}
	} else if dbz >= 35.0 && dbz < 40.0 {
		return color.NRGBA{0x00, 0xBB, 0x00, 0xFF}
	} else if dbz >= 40.0 && dbz < 45.0 {
		return color.NRGBA{0xFF, 0xFF, 0x70, 0xFF}
	} else if dbz >= 45.0 && dbz < 50.0 {
		return color.NRGBA{0xD0, 0xD0, 0x60, 0xFF}
	} else if dbz >= 50.0 && dbz < 55.0 {
		return color.NRGBA{0xFF, 0x60, 0x60, 0xFF}
	} else if dbz >= 55.0 && dbz < 60.0 {
		return color.NRGBA{0xDA, 0x00, 0x00, 0xFF}
	} else if dbz >= 60.0 && dbz < 65.0 {
		return color.NRGBA{0xAE, 0x00, 0x00, 0xFF}
	} else if dbz >= 65.0 && dbz < 70.0 {
		return color.NRGBA{0x00, 0x00, 0xFF, 0xFF}
	} else if dbz >= 70.0 && dbz < 75.0 {
		return color.NRGBA{0xFF, 0xFF, 0xFF, 0xFF}
	}
	return color.NRGBA{0xE7, 0x00, 0xFF, 0xFF}
}

func dbzColorCleanAirMode(dbz float32) color.Color {
	if dbz < -28.0 {
		return colornames.Black
	} else if dbz >= -28.0 && dbz < -24.0 {
		return color.NRGBA{0x9C, 0x9C, 0x9C, 0xFF}
	} else if dbz >= -24.0 && dbz < -20.0 {
		return color.NRGBA{0x76, 0x76, 0x76, 0xFF}
	} else if dbz >= -20.0 && dbz < -16.0 {
		return color.NRGBA{0xFF, 0xAA, 0xAA, 0xFF}
	} else if dbz >= -16.0 && dbz < -12.0 {
		return color.NRGBA{0xEE, 0x8C, 0x8C, 0xFF}
	} else if dbz >= -12.0 && dbz < -8.0 {
		return color.NRGBA{0xC9, 0x70, 0x70, 0xFF}
	} else if dbz >= -8.0 && dbz < -4.0 {
		return color.NRGBA{0x00, 0xFB, 0x90, 0xFF}
	} else if dbz >= -4.0 && dbz < 0.0 {
		return color.NRGBA{0x00, 0xBB, 0x00, 0xFF}
	} else if dbz >= 0.0 && dbz < 4.0 {
		return color.NRGBA{0xFF, 0xFF, 0x70, 0xFF}
	} else if dbz >= 4.0 && dbz < 8.0 {
		return color.NRGBA{0xD0, 0xD0, 0x60, 0xFF}
	} else if dbz >= 8.0 && dbz < 12.0 {
		return color.NRGBA{0xFF, 0x60, 0x60, 0xFF}
	} else if dbz >= 12.0 && dbz < 16.0 {
		return color.NRGBA{0xDA, 0x00, 0x00, 0xFF}
	} else if dbz >= 16.0 && dbz < 20.0 {
		return color.NRGBA{0xAE, 0x00, 0x00, 0xFF}
	} else if dbz >= 20.0 && dbz < 24.0 {
		return color.NRGBA{0x00, 0x00, 0xFF, 0xFF}
	} else if dbz >= 24.0 && dbz < 28.0 {
		return color.NRGBA{0xFF, 0xFF, 0xFF, 0xFF}
	}
	return color.NRGBA{0xE7, 0x00, 0xFF, 0xFF}
}

func dbzColorNOAA(dbz float32) color.Color {
	if dbz < 5.0 || dbz == archive2.MomentDataFolded {
		return color.NRGBA{0x00, 0x00, 0x00, 0x00}
	} else if dbz >= 5.0 && dbz < 10.0 {
		return color.NRGBA{0x40, 0xe8, 0xe3, 0xFF}
	} else if dbz >= 10.0 && dbz < 15.0 {
		// 26A4FA
		return color.NRGBA{0x26, 0xa4, 0xfa, 0xFF}
	} else if dbz >= 15.0 && dbz < 20.0 {
		// 0030ED
		return color.NRGBA{0x00, 0x30, 0xed, 0xFF}
	} else if dbz >= 20.0 && dbz < 25.0 {
		// 49FB3E
		return color.NRGBA{0x49, 0xfb, 0x3e, 0xFF}
	} else if dbz >= 25.0 && dbz < 30.0 {
		// 36C22E
		return color.NRGBA{0x36, 0xc2, 0x2e, 0xFF}
	} else if dbz >= 30.0 && dbz < 35.0 {
		// 278C1E
		return color.NRGBA{0x27, 0x8c, 0x1e, 0xFF}
	} else if dbz >= 35.0 && dbz < 40.0 {
		// FEF543
		return color.NRGBA{0xfe, 0xf5, 0x43, 0xFF}
	} else if dbz >= 40.0 && dbz < 45.0 {
		// EBB433
		return color.NRGBA{0xeb, 0xb4, 0x33, 0xFF}
	} else if dbz >= 45.0 && dbz < 50.0 {
		// F6952E
		return color.NRGBA{0xf6, 0x95, 0x2e, 0xFF}
	} else if dbz >= 50.0 && dbz < 55.0 {
		// F80A26
		return color.NRGBA{0xf8, 0x0a, 0x26, 0xFF}
	} else if dbz >= 55.0 && dbz < 60.0 {
		// CB0516
		return color.NRGBA{0xcb, 0x05, 0x16, 0xFF}
	} else if dbz >= 60.0 && dbz < 65.0 {
		// A90813
		return color.NRGBA{0xa9, 0x08, 0x13, 0xFF}
	} else if dbz >= 65.0 && dbz < 70.0 {
		// EE34FA
		return color.NRGBA{0xee, 0x34, 0xfa, 0xFF}
	} else if dbz >= 70.0 && dbz < 75.0 {
		return color.NRGBA{0x91, 0x61, 0xc4, 0xFF}
	}
	return color.NRGBA{0xff, 0xff, 0xFF, 0xFF}
}

func dbzColorScopeClassic(dbz float32) color.Color {
	if dbz < 5.0 {
		return colornames.Black
	} else if dbz >= 5.0 && dbz < 10.0 {
		return color.NRGBA{0x02, 0x0d, 0x02, 0xFF}
	} else if dbz >= 10.0 && dbz < 15.0 {
		return color.NRGBA{0x04, 0x23, 0x03, 0xFF}
	} else if dbz >= 15.0 && dbz < 20.0 {
		return color.NRGBA{0x11, 0x52, 0x0d, 0xFF}
	} else if dbz >= 20.0 && dbz < 25.0 {
		return color.NRGBA{0x33, 0xba, 0x2b, 0xFF}
	} else if dbz >= 25.0 && dbz < 30.0 {
		return color.NRGBA{0x43, 0xeb, 0x39, 0xFF}
	} else if dbz >= 30.0 && dbz < 35.0 {
		return color.NRGBA{0xff, 0xFB, 0x45, 0xFF}
	} else if dbz >= 35.0 && dbz < 40.0 {
		return color.NRGBA{0xf5, 0xcb, 0x39, 0xFF}
	} else if dbz >= 40.0 && dbz < 45.0 {
		return color.NRGBA{0xFb, 0xab, 0x32, 0xFF}
	} else if dbz >= 45.0 && dbz < 50.0 {
		return color.NRGBA{0xfa, 0x83, 0x2a, 0xFF}
	} else if dbz >= 50.0 && dbz < 55.0 {
		return color.NRGBA{0xbb, 0x03, 0x13, 0xFF}
	} else if dbz >= 55.0 && dbz < 60.0 {
		return color.NRGBA{0xf7, 0x06, 0x1d, 0xFF}
	} else if dbz >= 60.0 && dbz < 65.0 {
		return color.NRGBA{0xf9, 0x64, 0x69, 0xFF}
	} else if dbz >= 65.0 && dbz < 70.0 {
		return color.NRGBA{0xfa, 0x97, 0xcc, 0xFF}
	} else if dbz >= 70.0 && dbz < 75.0 {
		return color.NRGBA{0xf7, 0x34, 0xf9, 0xFF}
	}
	return color.NRGBA{0xff, 0xff, 0xFF, 0xFF}
}

func velColorRadarscope(vel float32) color.Color {
	if vel == archive2.MomentDataFolded {
		return color.NRGBA{0x69, 0x1A, 0xC1, 0xff}
	}

	colors := []color.Color{
		color.NRGBA{0xF9, 0x14, 0x73, 0xff}, // 140
		color.NRGBA{0xAA, 0x10, 0x79, 0xff}, // 130
		color.NRGBA{0x6E, 0x0E, 0x80, 0xff}, // 120
		color.NRGBA{0x2E, 0x0E, 0x84, 0xff}, // 110
		color.NRGBA{0x15, 0x1F, 0x93, 0xff}, // 100
		color.NRGBA{0x23, 0x6F, 0xB3, 0xff}, // 90
		color.NRGBA{0x41, 0xDA, 0xDB, 0xff}, // 80
		color.NRGBA{0x66, 0xE1, 0xE2, 0xff}, // 70
		color.NRGBA{0x9E, 0xE8, 0xEA, 0xff}, // 60
		color.NRGBA{0x57, 0xFA, 0x63, 0xff}, // 50
		color.NRGBA{0x31, 0xE3, 0x2B, 0xff}, // 40
		// color.NRGBA{0x21, 0xBE, 0x0A, 0xff}, // 35
		color.NRGBA{0x24, 0xAA, 0x1F, 0xff}, // 30
		color.NRGBA{0x19, 0x76, 0x13, 0xff}, // 20
		color.NRGBA{0x45, 0x67, 0x42, 0xff}, // -10
		color.NRGBA{0x63, 0x4F, 0x50, 0xff}, // 0
		color.NRGBA{0x6e, 0x2e, 0x39, 0xff}, // 10
		color.NRGBA{0x7F, 0x03, 0x0C, 0xff}, // 20
		color.NRGBA{0xB6, 0x07, 0x16, 0xff}, // 30
		// color.NRGBA{0xC5, 0x00, 0x0D, 0xff}, // 35
		color.NRGBA{0xF3, 0x22, 0x45, 0xff}, // 40
		color.NRGBA{0xF6, 0x50, 0x8A, 0xff}, // 50
		color.NRGBA{0xFB, 0x8B, 0xBF, 0xff}, // 60
		color.NRGBA{0xFD, 0xDE, 0x93, 0xff}, // 70
		color.NRGBA{0xFC, 0xB4, 0x70, 0xff}, // 80
		color.NRGBA{0xFA, 0x81, 0x4B, 0xff}, // 90
		color.NRGBA{0xDD, 0x60, 0x3C, 0xff}, // 100
		color.NRGBA{0xB7, 0x45, 0x2D, 0xff}, // 110
		color.NRGBA{0x93, 0x2C, 0x20, 0xff}, // 120
		color.NRGBA{0x71, 0x16, 0x14, 0xff}, // 130
		color.NRGBA{0x52, 0x01, 0x06, 0xff}, // 140
	}

	// if vel < -140 {
	// 	return color.NRGBA{0x69, 0x1A, 0xC1, 0xff} // -140+
	// } else if vel > 140 {
	// 	return color.NRGBA{0xff, 0xff, 0xff, 0xff} // 140+
	// }

	i := scaleInt(int32(vel), 140, -140, int32(len(colors))-1, 0)
	return colors[i]
}

func dbzColorScope(dbz float32) color.Color {
	colors := []color.Color{
		color.NRGBA{0x03, 0x03, 0x03, 0xff}, // 0
		color.NRGBA{0x09, 0x0A, 0x0A, 0xff},
		color.NRGBA{0x0F, 0x11, 0x14, 0xff},
		color.NRGBA{0x12, 0x15, 0x1A, 0xff},
		color.NRGBA{0x14, 0x19, 0x20, 0xff},
		color.NRGBA{0x16, 0x1B, 0x26, 0xff},
		color.NRGBA{0x16, 0x1D, 0x2C, 0xff},
		color.NRGBA{0x16, 0x1E, 0x31, 0xff},
		color.NRGBA{0x17, 0x21, 0x3A, 0xff},
		color.NRGBA{0x19, 0x25, 0x3F, 0xff},
		color.NRGBA{0x17, 0x21, 0x3A, 0xff}, // 10
		color.NRGBA{0x1D, 0x2D, 0x47, 0xff},
		color.NRGBA{0x23, 0x37, 0x52, 0xff},
		color.NRGBA{0x28, 0x41, 0x5C, 0xff},
		color.NRGBA{0x2E, 0x4C, 0x67, 0xff},
		color.NRGBA{0x34, 0x58, 0x72, 0xff},
		color.NRGBA{0x37, 0x5E, 0x77, 0xff},
		color.NRGBA{0x42, 0x73, 0x8A, 0xff},
		color.NRGBA{0x46, 0x7B, 0x90, 0xff},
		color.NRGBA{0x4E, 0x8C, 0x9D, 0xff},
		color.NRGBA{0x39, 0x9F, 0x5D, 0xff}, //20
		color.NRGBA{0x2F, 0xA2, 0x3E, 0xff},
		color.NRGBA{0x2C, 0x9B, 0x3A, 0xff},
		color.NRGBA{0x25, 0x86, 0x2D, 0xff},
		color.NRGBA{0x20, 0x78, 0x25, 0xff},
		color.NRGBA{0x1E, 0x72, 0x21, 0xff},
		color.NRGBA{0x16, 0x59, 0x13, 0xff},
		color.NRGBA{0x14, 0x53, 0x11, 0xff},
		color.NRGBA{0x32, 0x71, 0x15, 0xff},
		color.NRGBA{0x5C, 0x92, 0x1C, 0xff},
		color.NRGBA{0xA6, 0xC7, 0x2A, 0xff}, // 30
		color.NRGBA{0xC1, 0xD9, 0x2F, 0xff},
		color.NRGBA{0xF6, 0xF9, 0x38, 0xff},
		color.NRGBA{0xF1, 0xF3, 0x37, 0xff},
		color.NRGBA{0xED, 0xEC, 0x35, 0xff},
		color.NRGBA{0xE0, 0xDA, 0x31, 0xff},
		color.NRGBA{0xD6, 0xCD, 0x2E, 0xff},
		color.NRGBA{0xC8, 0xBB, 0x2A, 0xff},
		color.NRGBA{0xC8, 0xBB, 0x2A, 0xff},
		color.NRGBA{0xBB, 0xAA, 0x26, 0xff},
		color.NRGBA{0xF4, 0x81, 0x25, 0xff}, // 40
		color.NRGBA{0xEA, 0x79, 0x24, 0xff},
		color.NRGBA{0xE1, 0x73, 0x22, 0xff},
		color.NRGBA{0xD8, 0x6D, 0x20, 0xff},
		color.NRGBA{0xCF, 0x67, 0x1F, 0xff},
		color.NRGBA{0xC6, 0x60, 0x1E, 0xff},
		color.NRGBA{0xC2, 0x5D, 0x1D, 0xff},
		color.NRGBA{0xB4, 0x54, 0x1B, 0xff},
		color.NRGBA{0xB0, 0x51, 0x1A, 0xff},
		color.NRGBA{0xA3, 0x48, 0x19, 0xff},
		color.NRGBA{0xF1, 0x0C, 0x20, 0xff}, // 50
		color.NRGBA{0xE1, 0x0D, 0x1E, 0xff},
		color.NRGBA{0xDA, 0x10, 0x1D, 0xff},
		color.NRGBA{0xC4, 0x13, 0x1C, 0xff},
		color.NRGBA{0xBD, 0x14, 0x1B, 0xff},
		color.NRGBA{0xA8, 0x16, 0x1B, 0xff},
		color.NRGBA{0xA1, 0x17, 0x1A, 0xff},
		color.NRGBA{0x8C, 0x19, 0x1A, 0xff},
		color.NRGBA{0x86, 0x19, 0x1A, 0xff},
		color.NRGBA{0x72, 0x1B, 0x1A, 0xff},
		color.NRGBA{0xBC, 0x86, 0xA4, 0xff}, // 60
		color.NRGBA{0xBA, 0x76, 0x9D, 0xff},
		color.NRGBA{0xB9, 0x68, 0x95, 0xff},
		color.NRGBA{0xB7, 0x5B, 0x8D, 0xff},
		color.NRGBA{0xB6, 0x4E, 0x86, 0xff},
		color.NRGBA{0xB4, 0x41, 0x7E, 0xff},
		color.NRGBA{0xB4, 0x3B, 0x7A, 0xff},
		color.NRGBA{0xB3, 0x28, 0x70, 0xff},
		color.NRGBA{0xB2, 0x1D, 0x69, 0xff},
		color.NRGBA{0xB0, 0x0C, 0x5F, 0xff},
		color.NRGBA{0x85, 0x1E, 0xD5, 0xff}, // 70
		color.NRGBA{0x7B, 0x1C, 0xCA, 0xff},
		color.NRGBA{0x75, 0x1B, 0xC4, 0xff},
		color.NRGBA{0x66, 0x18, 0xB5, 0xff},
		color.NRGBA{0x5E, 0x16, 0xAB, 0xff},
		color.NRGBA{0x54, 0x14, 0xA1, 0xff},
		color.NRGBA{0x4F, 0x13, 0x9C, 0xff},
		color.NRGBA{0x43, 0x10, 0x8E, 0xff},
		color.NRGBA{0x3A, 0x0E, 0x85, 0xff},
		color.NRGBA{0x2E, 0x0B, 0x77, 0xff},
	}

	if int(dbz) >= 0 && int(dbz) < len(colors) {
		return colors[int(dbz)]
	}
	return colornames.Black
}

// scaleInt scales a number form one range to another range
func scaleInt(value, oldMax, oldMin, newMax, newMin int32) int32 {
	oldRange := (oldMax - oldMin)
	newRange := (newMax - newMin)
	return (((value - oldMin) * newRange) / oldRange) + newMin
}
