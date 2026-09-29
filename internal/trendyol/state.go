package trendyol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type pageState struct {
	Root    map[string]any
	Product map[string]any
	Doc     *goquery.Document
	JSONLD  map[string]any
}

func extractPageState(body []byte) (*pageState, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	state := &pageState{Doc: doc}
	doc.Find("script").EachWithBreak(func(_ int, selection *goquery.Selection) bool {
		raw := scriptJSON(strings.TrimSpace(selection.Text()))
		if raw == "" {
			return true
		}
		value, decodeErr := decodeJSON([]byte(raw))
		if decodeErr != nil {
			return true
		}
		if typ, _ := selection.Attr("type"); strings.EqualFold(typ, "application/ld+json") {
			if object := firstObject(value); object != nil {
				state.JSONLD = object
			}
		}
		if root, product := findProductState(value); product != nil {
			state.Root, state.Product = root, product
			return false
		}
		return true
	})
	if state.Product == nil && state.JSONLD == nil {
		return state, fmt.Errorf("product state not found")
	}
	return state, nil
}

// Puzzle fragments now expose server-rendered state as assignments such as
// window["__envoy__SHARED_PROPS"]={...}; instead of a bare JSON script.
// Restrict assignment extraction to named PROPS blobs, then let the JSON
// decoder consume only the first value and ignore the trailing semicolon.
func scriptJSON(raw string) string {
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[") {
		return raw
	}
	upper := strings.ToUpper(raw)
	if !strings.Contains(upper, "__PROPS") && !strings.Contains(upper, "__SHARED_PROPS") {
		return ""
	}
	equal := strings.IndexByte(raw, '=')
	if equal < 0 {
		return ""
	}
	value := strings.TrimSpace(raw[equal+1:])
	if !strings.HasPrefix(value, "{") && !strings.HasPrefix(value, "[") {
		return ""
	}
	return value
}

func decodeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil && err != io.EOF {
		return nil, err
	}
	return value, nil
}

func firstObject(value any) map[string]any {
	switch value := value.(type) {
	case map[string]any:
		return value
	case []any:
		for _, child := range value {
			if object := firstObject(child); object != nil {
				return object
			}
		}
	}
	return nil
}

func findProductState(value any) (map[string]any, map[string]any) {
	switch value := value.(type) {
	case map[string]any:
		if product := object(value["product"]); isProductObject(product) {
			return value, product
		}
		if isProductObject(value) {
			return value, value
		}
		for _, child := range value {
			if root, product := findProductState(child); product != nil {
				return root, product
			}
		}
	case []any:
		for _, child := range value {
			if root, product := findProductState(child); product != nil {
				return root, product
			}
		}
	}
	return nil, nil
}

func isProductObject(value map[string]any) bool {
	if value == nil || stringValue(value["id"]) == "" {
		return false
	}
	return len(array(value["images"])) > 0 && len(array(value["categoryTree"])) > 0 && object(value["merchantListing"]) != nil
}

func object(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func array(value any) []any {
	result, _ := value.([]any)
	return result
}

func stringValue(value any) string {
	switch value := value.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return value.String()
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.6f", value), "0"), ".")
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	}
	return ""
}

func numberValue(value any) (float64, bool) {
	switch value := value.(type) {
	case json.Number:
		result, err := value.Float64()
		return result, err == nil
	case float64:
		return value, true
	case int:
		return float64(value), true
	case string:
		var number json.Number = json.Number(strings.TrimSpace(value))
		result, err := number.Float64()
		return result, err == nil
	}
	return 0, false
}
