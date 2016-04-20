package main

import (
	"fmt"

	"../go-whisk-cli/commands"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			fmt.Println("Application exited unexpectedly")
		}
	}()

	if err := commands.Execute(); err != nil {
		fmt.Println(err)
		return
	}
}
