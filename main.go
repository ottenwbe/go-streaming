package main

import (
	"github.com/gin-gonic/gin"
	"go-stream-processing/api"
	"go.uber.org/zap"
)

func main() {
	p, _ := zap.NewProduction() //TODO - handle error
	zap.ReplaceGlobals(p)
	router := gin.Default()

	zap.S().Info("Started Server")

	api.CreateRestAPI(router)

	if err := router.Run("localhost:8080"); err != nil {
		zap.S().Error("", zap.String("module", "main"))
	}

}
