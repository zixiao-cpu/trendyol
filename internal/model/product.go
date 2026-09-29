package model

import "time"

var CSVFields = []string{
	"product_id", "item_number", "url", "title", "brand",
	"color", "capacity", "material", "selling_price", "currency",
	"main_image", "images", "description", "features", "specs", "reviews",
	"product_group_id", "variation_theme", "variation_value", "scraped_at",
	"status", "error", "category_path", "category_level_1",
	"category_level_2", "category_level_3", "category_leaf",
	"category_node_ids", "category_source", "category_complete",
}

type Job struct {
	ID              int64
	ProductID       string
	Path            string
	URL             string
	State           string
	QueueName       string
	Attempts        int
	QualityAttempts int
	BypassAttempts  int
	AvailableAt     time.Time
	LeaseUntil      time.Time
	WorkerID        string
}

type Product struct {
	ProductID        string
	URL              string
	Title            string
	Brand            string
	MainImage        string
	Images           string
	Description      string
	Features         string
	Specs            string
	CategoryPath     string
	CategoryLevel1   string
	CategoryLevel2   string
	CategoryLevel3   string
	CategoryLeaf     string
	CategoryNodeIDs  string
	CategorySource   string
	CategoryComplete string
	ScrapedAt        string
}

type Variant struct {
	ItemNumber      string
	ProductID       string
	ProductGroupID  string
	Color           string
	Capacity        string
	Material        string
	SellingPrice    string
	Currency        string
	VariationTheme  string
	VariationValue  string
	VariationStatus string
	CategoryKey     string
	ScrapedAt       string
}

type Seller struct {
	SellerID     string
	Name         string
	OfficialName string
}

type Offer struct {
	ListingID    string
	ItemNumber   string
	SellerID     string
	SellingPrice string
	Currency     string
}

type Review struct {
	Stars    float64  `json:"stars"`
	Title    string   `json:"title"`
	Body     string   `json:"body"`
	Date     string   `json:"date"`
	Author   string   `json:"author"`
	Images   []string `json:"images"`
	Position int      `json:"-"`
}

type ProductBundle struct {
	Product             Product
	Variants            []Variant
	Seller              *Seller
	Offers              []Offer
	Reviews             []Review
	VariationIncomplete bool
	MissingPriceSKUs    []string
}

type ResultKind string

const (
	ResultDone          ResultKind = "done"
	ResultRetry         ResultKind = "retry"
	ResultSkip          ResultKind = "skip"
	ResultChallenge     ResultKind = "challenge"
	ResultQualityRetry  ResultKind = "quality_retry"
	ResultParserFailure ResultKind = "parser_failure"
)

type ScrapeResult struct {
	Job             Job
	Kind            ResultKind
	Bundle          *ProductBundle
	Reason          string
	HTTPStatus      int
	ResponseBytes   int64
	Duration        time.Duration
	ProxyKey        string
	Fixture         []byte
	BypassAttempted bool
	BypassCost      float64
}

func (b *ProductBundle) SKURecords(reviewsJSON string) [][]string {
	rows := make([][]string, 0, len(b.Variants))
	for _, v := range b.Variants {
		p := b.Product
		rows = append(rows, []string{
			p.ProductID, v.ItemNumber, p.URL, p.Title, p.Brand,
			v.Color, v.Capacity, v.Material, v.SellingPrice, v.Currency,
			p.MainImage, p.Images, p.Description, p.Features, p.Specs, reviewsJSON,
			v.ProductGroupID, v.VariationTheme, v.VariationValue, v.ScrapedAt,
			"ok", "", p.CategoryPath, p.CategoryLevel1, p.CategoryLevel2,
			p.CategoryLevel3, p.CategoryLeaf, p.CategoryNodeIDs,
			p.CategorySource, p.CategoryComplete,
		})
	}
	return rows
}
