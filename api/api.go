package api

// Legacy code

//
//import (
//	"io"
//	"net/http"
//
//	"github.com/ottenwbe/go-streaming/pkg/events"
//	"github.com/ottenwbe/go-streaming/pkg/pubsub"
//
//	"github.com/gin-gonic/gin"
//	"go.uber.org/zap"
//)
//
//func CreateRestAPI(router *gin.Engine) {
//
//	router.GET("/steams/:stream", func(c *gin.Context) {
//
//		var (
//			description pubsub.StreamDescription
//			result      = make(map[string]interface{})
//			err         error
//			name        = c.Param("stream")
//		)
//
//		description, err = pubsub.GetDescription(pubsub.MakeStreamID[map[string]interface{}](name))
//		if err != nil {
//			zap.S().Error("stream could not be found", zap.String("method", "GET"), zap.String("path", "/pubsub/:stream"), zap.String("module", "api"), zap.Error(err))
//			c.String(http.StatusNotFound, "stream not found")
//			return
//		}
//
//		result["meta"] = description
//
//		c.JSON(http.StatusOK, result)
//	})
//
//	router.POST("/steams", func(c *gin.Context) {
//
//		body, err := io.ReadAll(c.Request.Body)
//		if err != nil {
//			zap.S().Error("could not read body", zap.String("method", "POST"), zap.String("path", "/pubsub"), zap.String("module", "api"), zap.Error(err))
//			c.String(http.StatusBadRequest, "no payload provided")
//			return
//		}
//
//		d, err := pubsub.StreamDescriptionFromJSON(body)
//		if err != nil {
//			zap.S().Error("could not read stream description", zap.String("module", "api"), zap.Error(err))
//			c.String(http.StatusBadRequest, "invalid json body")
//			return
//		}
//		_, err = pubsub.AddOrReplaceStreamFromDescription[any](d)
//		if err != nil {
//			zap.S().Error("could not instantiate stream", zap.String("module", "api"), zap.Error(err))
//			c.String(http.StatusNotFound, "error when instantiating stream")
//			return
//		}
//
//		c.Status(http.StatusCreated)
//	})
//
//	router.POST("/pubsub/:stream/events", func(c *gin.Context) {
//
//		name := c.Param("stream")
//
//		data, err := io.ReadAll(c.Request.Body)
//		if err != nil {
//			zap.S().Error("Could not read body from POST /events", zap.Error(err))
//			c.String(http.StatusBadRequest, "no payload provided")
//			return
//		}
//
//		e, err := events.NewEventFromJSON(data)
//		if err != nil {
//			zap.S().Error("cannot unmarshall event", zap.String("module", "api"), zap.Error(err))
//			c.String(http.StatusBadRequest, "event not valid")
//		}
//
//		publisher, _ := pubsub.RegisterPublisher[map[string]interface{}](pubsub.MakeStreamID[map[string]interface{}](name))
//		defer pubsub.UnRegisterPublisher(publisher)
//		publisher.Publish(e)
//
//		c.Status(http.StatusCreated)
//	})
//}
