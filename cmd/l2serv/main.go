package main

import (
	"time"

	"github.com/gin-contrib/cache"
	"github.com/gin-contrib/cache/persistence"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	r := gin.Default()
	store := persistence.NewInMemoryStore(time.Minute)

	r.GET("/l2", cache.CachePage(store, 24*time.Hour, l2ListSitesHandler))
	r.GET("/l2/:site", cache.CachePage(store, 1*time.Minute, l2ListFilesHandler))
	r.GET("/l2/:site/:fn", cache.CachePage(store, 1*time.Hour, l2MetaHandler))
	r.GET("/l2/:site/:fn/:elv/:product/render", l2RenderHandler)

	r.GET("/l2-realtime/:site/:volume", realtimeMetaHandler)
	r.GET("/l2-realtime/:site/:volume/:elv/:product/render", realtimeRenderHandler)

	r.GET("/l3", cache.CachePage(store, 24*time.Hour, l3ListSitesHandler))
	r.GET("/l3/:site", cache.CachePage(store, 24*time.Hour, l3ListProductsHandler))
	r.GET("/l3/:site/:product", cache.CachePage(store, 1*time.Minute, l3ListFilesHandler))
	r.GET("/l3/:site/:product/:fn/render", l3RenderHandler)

	r.Run(":8081")
}
