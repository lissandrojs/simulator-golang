package main

import (
	"fmt"
	"log"

	"github.com/joho/godotenv"
	"github.com/lissandrojs/simulator-golang/application/route"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("error loading .env file")
	}
}

func main() {

	route := route.Route{
		ID:       "1",
		ClientID: "1",
	}

	route.LoadPositions()
	stringjson, _ := route.ExportJsonPositions()
	fmt.Println(stringjson[0])
}
