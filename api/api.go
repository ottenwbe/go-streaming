package api

import (
	"github.com/gin-gonic/gin"
	"go-stream-processing/events"
	"go-stream-processing/streams"
	"go.uber.org/zap"
	"io"
	"net/http"
)

func CreateRestAPI(router *gin.Engine) {

	router.GET("/steams/:stream", func(c *gin.Context) {
		result := make(map[string]interface{})

		name := c.Param("stream")

		v, err := streams.PubSubSystem.GetDescriptionN(name)
		if err != nil {
			zap.S().Error("Stream could not be found")
			c.String(http.StatusNotFound, "could not find stream")
		}

		result["meta"] = v

		c.JSON(http.StatusOK, result)
	})

	router.POST("/steams", func(c *gin.Context) {

		data, err := io.ReadAll(c.Request.Body)
		if err != nil {
			zap.S().Error("Could not read body from POST /stream", zap.Error(err))
			c.String(http.StatusBadRequest, "no payload provided")
			return
		}

		d, err := streams.StreamDescriptionFromJSON(data)
		if err != nil {
			zap.S().Error("could not read stream description", zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusBadRequest, "json not properly formatted: %v", err.Error())
			return
		}
		err = streams.InstantiateStream(d)
		if err != nil {
			zap.S().Error("could not instantiate stream")
			c.String(http.StatusNotFound, "error when instantiating stream: %v", err.Error())
			return
		}

		c.Status(http.StatusCreated)
	})

	router.POST("/streams/:stream/events", func(c *gin.Context) {

		name := c.Param("stream")

		data, err := io.ReadAll(c.Request.Body)
		if err != nil {
			zap.S().Error("Could not read body from POST /events", zap.Error(err))
			c.String(http.StatusBadRequest, "no payload provided")
			return
		}

		e := events.NewSimpleEvent(data)

		err = streams.PubSubSystem.PublishN(name, e)
		if err != nil {
			zap.S().Error("cannot publish event", zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusBadRequest, "cannot publish request")
		}

		c.Status(http.StatusCreated)
	})
}
