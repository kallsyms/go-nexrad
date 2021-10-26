package main

import (
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const L3_BUCKET = "gcp-public-data-nexrad-l3-realtime"

func listGCS(ctx context.Context, bucket *storage.BucketHandle, prefix string) ([]string, []string) {
	blobs := []string{}
	dirs := []string{}

	it := bucket.Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: "/",
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logrus.Errorf("Bucket.Objects: %v", err)
			break
		}
		if attrs.Prefix != "" {
			dirs = append(dirs, filepath.Base(attrs.Prefix))
		} else {
			blobs = append(blobs, filepath.Base(attrs.Name))
		}
	}

	return blobs, dirs
}

func l3ListSitesHandler(c *gin.Context) {
	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile("service_account.json"))
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	defer client.Close()
	_, sites := listGCS(context.Background(), client.Bucket(L3_BUCKET), "NIDS/")

	c.JSON(200, sites)
}

func l3ListProductsHandler(c *gin.Context) {
	site := c.Param("site")

	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile("service_account.json"))
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	defer client.Close()
	_, products := listGCS(context.Background(), client.Bucket(L3_BUCKET), "NIDS/"+site+"/")

	c.JSON(200, products)
}

func l3ListFilesHandler(c *gin.Context) {
	site := c.Param("site")
	product := c.Param("product")

	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile("service_account.json"))
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	defer client.Close()
	files, _ := listGCS(context.Background(), client.Bucket(L3_BUCKET), "NIDS/"+site+"/"+product+"/")

	c.JSON(200, files)
}

func l3RenderHandler(c *gin.Context) {
	site := c.Param("site")
	product := c.Param("product")
	fn := c.Param("fn")

	date, err := time.Parse("20060102_1504", strings.Split(fn, "_")[1])
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	radDir, err := ioutil.TempDir("", "")
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	defer os.RemoveAll(radDir)

	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile("service_account.json"))
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	defer client.Close()

	radFileReader, err := client.Bucket(L3_BUCKET).Object("NIDS/" + site + "/" + product + "/" + fn).NewReader(context.Background())
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	radFile, _ := ioutil.ReadAll(radFileReader)
	radFileReader.Close()

	err = os.MkdirAll(filepath.Join(radDir, "NIDS", site, product), 0777)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	err = os.WriteFile(filepath.Join(radDir, "NIDS", site, product, fn), radFile, 0666)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	outFn := filepath.Join(radDir, "out.gif")
	cmd := exec.Command("./l3_conv.sh", radDir, outFn, product, date.Format("060102/1504"))
	err = cmd.Run()
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	gif, err := ioutil.ReadFile(outFn)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	c.Data(http.StatusOK, "image/gif", gif)
}
