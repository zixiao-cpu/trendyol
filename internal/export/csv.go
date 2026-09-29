package export

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"trendyol/internal/model"
	"trendyol/internal/store"
)

type Manifest struct {
	Rows          int64  `json:"rows"`
	Bytes         int64  `json:"bytes"`
	SHA256        string `json:"sha256"`
	ReviewQueries int64  `json:"review_queries"`
	CreatedAt     string `json:"created_at"`
}

func CSV(ctx context.Context, db *store.DB, output string, minPrice float64) (Manifest, error) {
	var manifest Manifest
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil && filepath.Dir(output) != "." {
		return manifest, err
	}
	temp, err := os.CreateTemp(filepath.Dir(output), ".trendyol-export-*.csv")
	if err != nil {
		return manifest, err
	}
	tempPath := temp.Name()
	published := false
	defer func() {
		_ = temp.Close()
		if !published {
			_ = os.Remove(tempPath)
		}
	}()
	hasher := sha256.New()
	counting := &countWriter{writer: io.MultiWriter(temp, hasher)}
	buffer := bufio.NewWriterSize(counting, 256*1024)
	writer := csv.NewWriter(buffer)
	if err := writer.Write(model.CSVFields); err != nil {
		return manifest, err
	}
	rows, err := db.SQL().QueryContext(ctx, `SELECT p.product_id,v.item_number,p.url,p.title,p.brand,v.color,v.capacity,v.material,v.selling_price,v.currency,p.main_image,p.images,p.description,p.features,p.specs,v.product_group_id,v.variation_theme,v.variation_value,v.scraped_at,p.category_path,p.category_level_1,p.category_level_2,p.category_level_3,p.category_leaf,p.category_node_ids,p.category_source,p.category_complete,v.variation_status FROM products p JOIN variants v ON v.product_id=p.product_id ORDER BY p.product_id,v.item_number`)
	if err != nil {
		return manifest, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	lastProduct, reviewsJSON := "", "[]"
	for rows.Next() {
		fields := make([]string, 28)
		pointers := make([]any, len(fields))
		for i := range fields {
			pointers[i] = &fields[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return manifest, err
		}
		productID, itemNumber := fields[0], fields[1]
		if productID != lastProduct {
			reviewsJSON, err = reviewsForProduct(ctx, db, productID)
			if err != nil {
				return manifest, err
			}
			manifest.ReviewQueries++
			lastProduct = productID
		}
		row := []string{fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11], fields[12], fields[13], fields[14], reviewsJSON, fields[15], fields[16], fields[17], fields[18], "ok", "", fields[19], fields[20], fields[21], fields[22], fields[23], fields[24], fields[25], fields[26]}
		if seen[itemNumber] {
			return manifest, fmt.Errorf("duplicate item_number %s", itemNumber)
		}
		seen[itemNumber] = true
		if err := ValidateRow(row, minPrice); err != nil {
			return manifest, fmt.Errorf("SKU %s: %w", itemNumber, err)
		}
		if err := writer.Write(row); err != nil {
			return manifest, err
		}
		manifest.Rows++
	}
	if err := rows.Err(); err != nil {
		return manifest, err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return manifest, err
	}
	if err := buffer.Flush(); err != nil {
		return manifest, err
	}
	if err := temp.Sync(); err != nil {
		return manifest, err
	}
	if err := temp.Close(); err != nil {
		return manifest, err
	}
	manifest.Bytes = counting.bytes
	manifest.SHA256 = hex.EncodeToString(hasher.Sum(nil))
	manifest.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := os.Rename(tempPath, output); err != nil {
		return manifest, err
	}
	published = true
	manifestRaw, _ := json.MarshalIndent(manifest, "", "  ")
	manifestTemp := output + ".manifest.tmp"
	if err := os.WriteFile(manifestTemp, append(manifestRaw, '\n'), 0o644); err != nil {
		return manifest, err
	}
	if file, err := os.OpenFile(manifestTemp, os.O_RDWR, 0); err == nil {
		_ = file.Sync()
		_ = file.Close()
	}
	if err := os.Rename(manifestTemp, output+".manifest.json"); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func reviewsForProduct(ctx context.Context, db *store.DB, productID string) (string, error) {
	rows, err := db.SQL().QueryContext(ctx, `SELECT stars,title,body,review_date,author,images FROM reviews WHERE product_id=? ORDER BY position,review_key LIMIT 10`, productID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var reviews []model.Review
	for rows.Next() {
		var review model.Review
		var images string
		if err := rows.Scan(&review.Stars, &review.Title, &review.Body, &review.Date, &review.Author, &images); err != nil {
			return "", err
		}
		if err := json.Unmarshal([]byte(images), &review.Images); err != nil {
			return "", err
		}
		reviews = append(reviews, review)
	}
	if reviews == nil {
		reviews = []model.Review{}
	}
	raw, err := json.Marshal(reviews)
	return string(raw), err
}

func ValidateFile(path string, minPrice float64) (Manifest, error) {
	var manifest Manifest
	file, err := os.Open(path)
	if err != nil {
		return manifest, err
	}
	defer file.Close()
	hasher := sha256.New()
	reader := csv.NewReader(io.TeeReader(file, hasher))
	header, err := reader.Read()
	if err != nil {
		return manifest, err
	}
	if len(header) != len(model.CSVFields) {
		return manifest, fmt.Errorf("header columns=%d", len(header))
	}
	for i := range header {
		if header[i] != model.CSVFields[i] {
			return manifest, fmt.Errorf("header field %d=%s", i, header[i])
		}
	}
	seen := map[string]bool{}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return manifest, err
		}
		if err := ValidateRow(row, minPrice); err != nil {
			return manifest, fmt.Errorf("row %d: %w", manifest.Rows+2, err)
		}
		if seen[row[1]] {
			return manifest, fmt.Errorf("duplicate item_number %s", row[1])
		}
		seen[row[1]] = true
		manifest.Rows++
	}
	info, err := file.Stat()
	if err != nil {
		return manifest, err
	}
	manifest.Bytes = info.Size()
	manifest.SHA256 = hex.EncodeToString(hasher.Sum(nil))
	return manifest, nil
}

type countWriter struct {
	writer io.Writer
	bytes  int64
	hash   hash.Hash
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.bytes += int64(n)
	return n, err
}
