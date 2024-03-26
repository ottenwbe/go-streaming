package main

import "fmt"

func main() {
	e1 := []int{1, 2, 3, 4}
	e2 := e1[1:3]
	fmt.Println(e2)

	fmt.Println(e1)
}

/*func main() {
	p, _ := zap.NewProduction() //TODO - handle error
	zap.ReplaceGlobals(p)
	router := gin.Default()

	zap.S().Info("Started Server")

	api.CreateRestAPI(router)

	if err := router.Run("localhost:8080"); err != nil {
		zap.S().Error("", zap.String("module", "main"))
	}

}
*/
