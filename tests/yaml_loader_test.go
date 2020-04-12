package tests

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"gopkg.in/yaml.v2"
)

func TestYamlLoader(t *testing.T) {
	yamlData, err := ioutil.ReadFile("config.yaml")
	assert.Nil(t, err)
	data := make(map[string]interface{})
	err = yaml.Unmarshal(yamlData, &data)
	assert.Nil(t, err)
	registry, err := orm.InitByYaml(data)
	assert.Nil(t, err)
	assert.NotNil(t, registry)
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	assert.NotNil(t, config)
}
