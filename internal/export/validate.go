package export

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"trendyol/internal/model"
)

var timestampPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`)

func ValidateRow(row []string, minPrice float64) error {
	if len(row) != len(model.CSVFields) {
		return fmt.Errorf("columns=%d want=%d", len(row), len(model.CSVFields))
	}
	value := func(name string) string {
		for index, field := range model.CSVFields {
			if field == name {
				return row[index]
			}
		}
		return ""
	}
	for _, field := range []string{"product_id", "item_number", "url", "title", "brand", "main_image", "description", "category_path"} {
		if value(field) == "" {
			return fmt.Errorf("%s empty", field)
		}
	}
	price, err := strconv.ParseFloat(value("selling_price"), 64)
	if err != nil || price <= minPrice {
		return fmt.Errorf("selling_price must be > %.2f", minPrice)
	}
	if value("currency") != "USD" {
		return fmt.Errorf("currency is not USD")
	}
	var specs map[string]any
	if err := json.Unmarshal([]byte(value("specs")), &specs); err != nil {
		return fmt.Errorf("invalid specs JSON: %w", err)
	}
	var reviews []model.Review
	if err := json.Unmarshal([]byte(value("reviews")), &reviews); err != nil || len(reviews) > 10 {
		return fmt.Errorf("invalid reviews JSON")
	}
	for _, review := range reviews {
		if review.Stars < 1 || review.Stars > 5 || (strings.TrimSpace(review.Title) == "" && strings.TrimSpace(review.Body) == "") {
			return fmt.Errorf("invalid review")
		}
	}
	theme, variation := value("variation_theme"), value("variation_value")
	if (theme == "") != (variation == "") {
		return fmt.Errorf("variation pair incomplete")
	}
	if theme != "" && len(strings.Split(theme, "|")) != len(strings.Split(variation, "|")) {
		return fmt.Errorf("variation dimensions mismatch")
	}
	if theme == "" && value("product_group_id") != value("product_id") {
		return fmt.Errorf("standalone parent mismatch")
	}
	path := strings.Split(value("category_path"), "|")
	if len(path) > 0 {
		if value("category_level_1") != path[0] || value("category_leaf") != path[len(path)-1] {
			return fmt.Errorf("category levels mismatch")
		}
	}
	if value("category_source") != "trendyol_category_tree" {
		return fmt.Errorf("invalid category source")
	}
	if value("category_complete") != "true" || value("status") != "ok" || value("error") != "" {
		return fmt.Errorf("invalid success status")
	}
	if !timestampPattern.MatchString(value("scraped_at")) {
		return fmt.Errorf("invalid scraped_at")
	}
	return nil
}
