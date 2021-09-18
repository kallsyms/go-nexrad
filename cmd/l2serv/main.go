package main

import (
	"encoding/json"
	"fmt"
	"image/png"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"

	"github.com/jddeal/go-nexrad/archive2"

	"github.com/gorilla/mux"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	r := mux.NewRouter()
	r.HandleFunc("/l2", siteListHandler)
	r.HandleFunc("/l2/{site}", listFilesHandler)
	r.HandleFunc("/l2/{site}/{fn}", metaHandler)
	r.HandleFunc("/l2/{site}/{fn}/{elv}/{product}/render", renderHandler)

	r.HandleFunc("/l2-realtime/{site}/{volume}.json", realtimeMetaHandler)
	r.HandleFunc("/l2-realtime/{site}/{volume}/{elv}/{product}/render", realtimeRenderHandler)

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

func siteListHandler(w http.ResponseWriter, req *http.Request) {
	sess, _ := session.NewSession(&aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Region:      aws.String("us-east-1"),
	})
	svc := s3.New(sess)
	bucket := aws.String("noaa-nexrad-level2")

	// check yesterday to get a list of all radars
	t := time.Now().UTC().AddDate(0, 0, -1)
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    bucket,
		Prefix:    aws.String(t.Format("2006/01/02/")),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sites := make([]string, 0, len(resp.CommonPrefixes))
	for _, d := range resp.CommonPrefixes {
		sites = append(sites, filepath.Base(*d.Prefix))
	}

	j, _ := json.Marshal(sites)
	w.Write(j)
}

func listFilesHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	site := vars["site"]

	sess, _ := session.NewSession(&aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Region:      aws.String("us-east-1"),
	})
	svc := s3.New(sess)
	bucket := aws.String("noaa-nexrad-level2")

	now := time.Now().UTC()
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: aws.String(now.Format("2006/01/02/") + site),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	files := make([]string, 0, len(resp.Contents))
	for _, d := range resp.Contents {
		files = append(files, filepath.Base(*d.Key))
	}

	if len(files) < 30 {
		now = now.AddDate(0, 0, -1)
		resp, err = svc.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: bucket,
			Prefix: aws.String(now.Format("2006/01/02/") + site),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pastFiles := make([]string, 0, len(resp.Contents))
		for _, d := range resp.Contents {
			pastFiles = append(pastFiles, filepath.Base(*d.Key))
		}
		files = append(pastFiles, files...)
	}

	files = files[len(files)-30:]

	j, _ := json.Marshal(files)
	w.Write(j)
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

	f, _ := os.Create("/tmp/pprof")
	pprof.StartCPUProfile(f)
	ar2, err := archive2.NewArchive2(radResp.Body)
	pprof.StopCPUProfile()
	f.Close()
	return ar2, err
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
