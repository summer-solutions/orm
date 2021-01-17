package main

import "github.com/summer-solutions/orm"

func main() {
	registry := &orm.Registry{}
	validatedRegistry, _ := registry.Validate()
	engine := validatedRegistry.CreateEngine()
	go func() {
		engine.GetMysql()
	}()
	go func() {
		engine.GetMysql()
	}()
}
