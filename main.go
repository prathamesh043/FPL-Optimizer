package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/", homeHandleFunc)
	http.HandleFunc("/about", aboutHandleFunc)
	http.ListenAndServe(":8080", nil)
}

func homeHandleFunc(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "FPL squad optimizer")
}

func aboutHandleFunc(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Prathamesh M")
}
