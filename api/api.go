package api

import (
	"github.com/gin-gonic/gin"
	"go-stream-processing/internal/events"
	streams2 "go-stream-processing/internal/streams"
	"go.uber.org/zap"
	"io"
	"net/http"
)

func CreateRestAPI(router *gin.Engine) {

	router.GET("/steams/:stream", func(c *gin.Context) {

		var (
			description streams2.StreamDescription
			result      = make(map[string]interface{})
			err         error
			name        = c.Param("stream")
		)

		description, err = streams2.GetDescriptionN[any](name)
		if err != nil {
			zap.S().Error("stream could not be found", zap.String("method", "GET"), zap.String("path", "/streams/:stream"), zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusNotFound, "stream not found")
			return
		}

		result["meta"] = description

		c.JSON(http.StatusOK, result)
	})

	router.POST("/steams", func(c *gin.Context) {

		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			zap.S().Error("could not read body", zap.String("method", "POST"), zap.String("path", "/streams"), zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusBadRequest, "no payload provided")
			return
		}

		d, err := streams2.StreamDescriptionFromJSON(body)
		if err != nil {
			zap.S().Error("could not read stream description", zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusBadRequest, "invalid json body")
			return
		}
		err = streams2.NewOrReplaceStreamD[any](d)
		if err != nil {
			zap.S().Error("could not instantiate stream", zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusNotFound, "error when instantiating stream")
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

		e, err := events.NewEventFromJSON(data)
		if err != nil {
			zap.S().Error("cannot unmarshall event", zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusBadRequest, "event not valid")
		}

		err = streams2.PublishN(name, e)
		if err != nil {
			zap.S().Error("cannot publish event", zap.String("module", "api"), zap.Error(err))
			c.String(http.StatusBadRequest, "stream not found or inactive: cannot publish request")
		}

		c.Status(http.StatusCreated)
	})
}
