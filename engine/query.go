package engine

func Stream[T any](name string) {

}

func Query[TIN, TOUT any](InStreams TIN, operation func(TIN) TOUT, OutStreams TOUT) {

}
