package main

import (
	"errors"
	"fmt"
	"image/png"
	"net/http"
	"strconv"
	"sync"

	"github.com/jddeal/go-nexrad/archive2"

	"github.com/gorilla/mux"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func loadArchive2Realtime(site string, volume int) (*archive2.Archive2, error) {
	sess, _ := session.NewSession(&aws.Config{
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

	if len(resp.Contents) == 0 {
		return nil, errors.New("No such volume number")
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

	mtx := sync.Mutex{}
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

			record, err := ar2.LoadLDMRecord(chunk.Body)
			chunk.Body.Close()
			if err != nil {
				return
			}
			mtx.Lock()
			ar2.AddFromLDMRecord(record)
			mtx.Unlock()
		}(chunkObjectInfo)
	}
	wg.Wait()

	return ar2, nil
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
