package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jddeal/go-nexrad/archive2"
	"github.com/sirupsen/logrus"

	"github.com/gorilla/mux"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	r := mux.NewRouter()
	r.HandleFunc("/l2", siteListHandler)
	r.HandleFunc("/l2/{site}", listFilesHandler)
	r.HandleFunc("/l2/{site}/{fn}", metaHandler)
	r.HandleFunc("/l2/{site}/{fn}/{elv}/{product}/render", renderHandler)

	r.HandleFunc("/l2-realtime/{site}/{volume}", realtimeMetaHandler)
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

	meta, _, err := ChunkCache.GetMeta(fn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	j, _ := json.Marshal(meta)
	w.Write(j)
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

	// f, _ := os.Create("/tmp/pprof")
	// runtime.SetCPUProfileRate(1000)
	// pprof.StartCPUProfile(f)

	ar2, err := ChunkCache.GetFileWithElevation(fn, elv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pngFile := renderAndReproject(ar2.ElevationScans[elv], product, 6000, 2600)
	png, _ := ioutil.ReadAll(pngFile)
	pngFile.Close()
	w.Write(png)

	// pprof.StopCPUProfile()
	// f.Close()
}
