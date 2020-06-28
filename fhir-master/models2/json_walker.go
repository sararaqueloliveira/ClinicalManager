package models2

import (
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
)


type FhirVisitor interface {
	String(pos positionInfo, value string) error
	Reference(pos positionInfo, value string) error
	Date(pos positionInfo, value string) error
	Instant(pos positionInfo, value string) error
	Decimal(pos positionInfo, value string) error
	Number(pos positionInfo, value string) error
	Bool(pos positionInfo, value bool) error
	Null(pos positionInfo) error

	Extension(pos positionInfo, url string) error
}

func WalkFHIRjson(jsonBytes []byte, visitor FhirVisitor) (err error) {

	debug("=== WalkFHIRjson ===")

	resourceType, err := jsonparser.GetString(jsonBytes, "resourceType")
	if err != nil {
		err = errors.Wrapf(err, "WalkFHIRjson: failed to get resourceType")
	}

	if err == nil {
		pos := positionInfo{pathHere: resourceType, element: resourceType}
		err = jsonparser.ObjectEach(jsonBytes, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			err4 := walkObjectKV(visitor, pos, key, value, dataType, offset)
			return err4
		})
	}

	return err
}

func walkObjectKV(visitor FhirVisitor, pos positionInfo, key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
	strKey := string(key)
	nextPos := pos.downTo(strKey, value)

	err := walkValue(visitor, nextPos, value, dataType)
	if err != nil {
		return errors.Wrapf(err, "object walkValue failed at %s", nextPos.pathHere)
	}

	if pos.atReference() {
		err = visitor.Reference(nextPos, string(value))
		if err != nil {
			return errors.Wrapf(err, "visitor.Reference failed")
		}
	}

	return nil
}

func walkArray(visitor FhirVisitor, pos positionInfo, value []byte, dataType jsonparser.ValueType, offset int) error {

	err := walkValue(visitor, pos.intoArray(value), value, dataType)
	if err != nil {
		return errors.Wrapf(err, "array walkValue failed at %s", pos.pathHere)
	}
	return nil
}

func walkValue(visitor FhirVisitor, pos positionInfo, value []byte, dataType jsonparser.ValueType) (err error) {

	switch dataType {
	case jsonparser.Object:

		err = jsonparser.ObjectEach(value, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			err2 := walkObjectKV(visitor, pos, key, value, dataType, offset)
			// fmt.Printf("Key: '%s'\n Value: '%s'\n Type: %s\n", string(key), string(value), dataType)
			return err2
		})
		if err != nil {
			return errors.Wrapf(err, "walkValue ObjectEach failed at %s", pos.pathHere)
		}

		return nil

	case jsonparser.Array:

		if pos.atExtension() {
			err = walkExtensionArray(visitor, value, pos)
			if err != nil {
				err = errors.Wrap(err, "walkExtensionArray failed")
			}
			return err
		}

		var err5 error
		_, err := jsonparser.ArrayEach(value, func(value []byte, dataType jsonparser.ValueType, offset int, err3 error) {
			if err3 == nil && err5 == nil {
				err5 = walkArray(visitor, pos, value, dataType, offset)
			}
		})
		if err != nil {
			return errors.Wrapf(err, "ArrayEach failed at %s", pos.pathHere)
		}
		if err5 != nil {
			return errors.Wrapf(err5, "ArrayEach.addToBSONarray failed at %s", pos.pathHere)
		}

		return nil

	case jsonparser.String:

		if pos.atDate() {
			err = visitor.Date(pos, string(value))
			if err != nil {
				err = errors.Wrap(err, "visitor.Date failed")
			}
			return
		} else if pos.atInstant() {
			err = visitor.Instant(pos, string(value))
			if err != nil {
				err = errors.Wrap(err, "visitor.Instant failed")
			}
			return
		} else {
			unescaped, err := jsonparser.Unescape(value, nil)
			if err != nil {
				return errors.Wrap(err, "jsonparser.Unescape failed")
			}

			err = visitor.String(pos, string(unescaped))
			if err != nil {
				return errors.Wrap(err, "visitor.String failed")
			}
			return err
		}

	case jsonparser.Null:
		err = visitor.Null(pos)
		if err != nil {
			err = errors.Wrap(err, "visitor.Null failed")
		}
		return err

	case jsonparser.Boolean:
		boo, err := jsonparser.GetBoolean(value)
		if err != nil {
			return errors.Wrap(err, "GetBoolean failed")
		}
		err = visitor.Bool(pos, boo)
		if err != nil {
			err = errors.Wrap(err, "visitor.Bool failed")
		}
		return err

	case jsonparser.Number:

		if pos.atDecimal() {
			err = visitor.Decimal(pos, string(value))
			if err != nil {
				return errors.Wrap(err, "visitor.Decimal failed")
			}
		} else {
			err = visitor.Number(pos, string(value))
			if err != nil {
				return errors.Wrap(err, "visitor.Number failed")
			}
		}
		return nil

	default:
		panic(fmt.Errorf("unhandled json datatype: %d", dataType))
	}

}

func walkExtensionArray(visitor FhirVisitor, jsonBytes []byte, pos positionInfo) (err error) {
	debug("walkExtensionArray started")
	var funcErr error
	_, err = jsonparser.ArrayEach(jsonBytes, func(origExtensonBytes []byte, dataType jsonparser.ValueType, offset int, err3 error) {
		if err3 == nil && funcErr == nil {

			if dataType != jsonparser.Object {
				funcErr = fmt.Errorf("getExtensionArray: element is not an object at %s (%d)", pos.pathHere, dataType)
				return
			}

			// promote url to a key to enable searching in Mongodb
			var url string
			url, funcErr = jsonparser.GetString(origExtensonBytes, "url")
			if funcErr != nil {
				funcErr = errors.Wrap(funcErr, "failed to get url")
				debug("walkExtensionArray: failed to get url: %v", funcErr)
				return
			}

			funcErr = visitor.Extension(pos, url)
			if funcErr != nil {
				funcErr = errors.Wrap(funcErr, "visitor.Extension failed")
				debug("walkExtensionArray: failed to call visitorExtensino for url: %v", funcErr)
				return
			}

			funcErr = jsonparser.ObjectEach(origExtensonBytes, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
				strKey := string(key)
				if strKey == "url" {
					debug("walkExtensionArray: child object: %s (skipped)", strKey)
					return nil
				} else {
					debug("walkExtensionArray: child object: %s", strKey)
				}
				err4 := walkObjectKV(visitor, pos, key, value, dataType, offset)
				return err4
			})
			if funcErr != nil {
				return
			}
		}
		// fmt.Printf("Key: '%s'\n Value: '%s'\n Type: %s\n", string(key), string(value), dataType)
	})

	debug("walkExtensionArray finished: errors %v %v", funcErr, err)

	if funcErr != nil {
		return funcErr
	}
	if err != nil {
		return err
	}
	return nil
}
