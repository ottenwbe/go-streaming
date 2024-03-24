package api

import "github.com/gin-gonic/gin"

func main() {
	router := gin.Default()

	// This handler will match /user/john but will not match /user/ or /user
	router.POST("/steam", func(c *gin.Context) {

	})

	router.Run(":8080")
}
