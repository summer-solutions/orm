package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
)

type Entity interface {
	getORM() *ORM
	GetID() uint64
	SetField(field string, value interface{}) error
}

type entityAttributes struct {
	onDuplicateKeyUpdate *Where
	loaded               bool
	delete               bool
	value                reflect.Value
	elem                 reflect.Value
	idElem               reflect.Value
	logMeta              map[string]interface{}
}

type ORM struct {
	dBData      map[string]interface{}
	tableSchema *tableSchema
	engine      *Engine
	attributes  *entityAttributes
}

func (orm *ORM) getORM() *ORM {
	return orm
}

func (orm *ORM) GetID() uint64 {
	if orm.attributes == nil {
		return 0
	}
	return orm.attributes.idElem.Uint()
}

func (orm *ORM) SetField(field string, value interface{}) error {
	asString, isString := value.(string)
	if isString {
		asString = strings.ToLower(asString)
		if asString == "nil" || asString == "null" {
			value = nil
		}
	}
	if orm.attributes == nil {
		return errors.NotValidf("entity is not loaded")
	}
	f := orm.attributes.elem.FieldByName(field)
	if !f.IsValid() {
		return errors.NotFoundf("field %s", field)
	}
	if !f.CanSet() {
		return errors.NotAssignedf("field %s", field)
	}
	typeName := f.Type().String()
	switch typeName {
	case "uint",
		"uint8",
		"uint16",
		"uint32",
		"uint64":
		val := uint64(0)
		if value != nil {
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return errors.NotValidf("%s value %v", field, value)
			}
			val = parsed
		}
		f.SetUint(val)
	case "*uint",
		"*uint8",
		"*uint16",
		"*uint32",
		"*uint64":
		if value != nil {
			val := uint64(0)
			parsed, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return errors.NotValidf("%s value %v", field, value)
			}
			val = parsed
			f.SetUint(val)
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "int",
		"int8",
		"int16",
		"int32",
		"int64":
		val := int64(0)
		if value != nil {
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return errors.NotValidf("%s value %v", field, value)
			}
			val = parsed
		}
		f.SetInt(val)
	case "*int",
		"*int8",
		"*int16",
		"*int32",
		"*int64":
		if value != nil {
			val := int64(0)
			parsed, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
			if err != nil {
				return errors.NotValidf("%s value %v", field, value)
			}
			val = parsed
			f.SetInt(val)
		} else {
			f.Set(reflect.Zero(f.Type()))
		}
	case "string":
		if value == nil {
			f.SetString("")
		} else {
			f.SetString(fmt.Sprintf("%v", value))
		}
	case "*string":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			f.SetString(fmt.Sprintf("%v", value))
		}
	case "[]string":
		_, ok := value.([]string)
		if !ok {
			return errors.NotValidf("%s value %v", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "[]uint8":
		_, ok := value.([]uint8)
		if !ok {
			return errors.NotValidf("%s value %v", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "bool":
		val := false
		asString := strings.ToLower(fmt.Sprintf("%v", value))
		if asString == "true" || asString == "1" {
			val = true
		}
		f.SetBool(val)
	case "*bool":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := false
			asString := strings.ToLower(fmt.Sprintf("%v", value))
			if asString == "true" || asString == "1" {
				val = true
			}
			f.SetBool(val)
		}
	case "float32",
		"float64":
		val := float64(0)
		if value != nil {
			valueString := fmt.Sprintf("%v", value)
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return errors.NotValidf("%s value %v", field, value)
			}
			val = parsed
		}
		f.SetFloat(val)
	case "*float32",
		"*float64":
		if value == nil {
			f.Set(reflect.Zero(f.Type()))
		} else {
			val := float64(0)
			valueString := fmt.Sprintf("%v", value)
			valueString = strings.ReplaceAll(valueString, ",", ".")
			parsed, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return errors.NotValidf("%s value %v", field, value)
			}
			val = parsed
			f.SetFloat(val)
		}
	case "*time.Time":
		_, ok := value.(*time.Time)
		if !ok {
			return errors.NotValidf("%s value %v", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "time.Time":
		_, ok := value.(time.Time)
		if !ok {
			return errors.NotValidf("%s value %v", field, value)
		}
		f.Set(reflect.ValueOf(value))
	case "interface {}":
		f.Set(reflect.ValueOf(value))
	default:
		k := f.Type().Kind().String()
		if k == "struct" {
			return errors.NotSupportedf("%s", field)
		} else if k == "ptr" {
			modelType := reflect.TypeOf((*Entity)(nil)).Elem()
			if f.Type().Implements(modelType) {
				if value == nil || (isString && (value == "" || value == "0")) {
					f.Set(reflect.Zero(f.Type()))
				} else {
					asEntity, ok := value.(Entity)
					if ok {
						f.Set(reflect.ValueOf(asEntity))
					} else {
						id, err := strconv.ParseUint(fmt.Sprintf("%v", value), 10, 64)
						if err != nil {
							return errors.NotValidf("%s", field)
						}
						if id == 0 {
							f.Set(reflect.ValueOf(asEntity))
						} else {
							val := reflect.New(f.Type().Elem())
							val.Elem().FieldByName("ID").SetUint(id)
							f.Set(val)
						}
					}
				}
			}
		} else {
			return errors.NotSupportedf("%s", field)
		}
	}
	return nil
}
