package trendyol

import "strings"

// DiscoveryPriority estimates price potential from information available
// before a product detail request. It only affects fetch order; no candidate
// is discarded, so coverage is preserved for the one-million-product goal.
func DiscoveryPriority(values ...string) int {
	value := strings.ToLower(strings.Join(values, " "))
	value = strings.NewReplacer(
		"-", " ", "_", " ", "/", " ",
		"ı", "i", "ş", "s", "ğ", "g", "ü", "u", "ö", "o", "ç", "c",
	).Replace(value)
	high := []string{
		"cep telefonu", "akilli telefon", "smartphone", "iphone", "galaxy",
		"laptop", "notebook", "bilgisayar", "tablet", "televizyon", "monitor",
		"kamera", "fotograf makinesi", "oyun konsolu", "playstation", "xbox", "drone",
		"buzdolabi", "camasir makinesi", "bulasik makinesi", "kurutma makinesi",
		"klima", "ankastre", "firin", "robot supurge", "kahve makinesi",
		"mobilya", "koltuk", "yatak", "gardırop", "gardrop", "yemek masasi",
		"bisiklet", "kosu bandi", "akilli saat", "projeksiyon", "jenerator",
	}
	medium := []string{
		"ayakkabi", "sneaker", "bot", "mont", "ceket", "elbise", "canta",
		"parfum", "kozmetik", "oyuncu", "mutfak robotu", "elektrikli supurge",
		"matkap", "takim cantasi", "bebek arabasi", "oto lastik",
	}
	low := []string{
		"kulot", "corap", "toka", "anahtarlik", "sticker", "pecete",
		"temizlik bezi", "sunger", "kalem", "telefon kilifi", "kablo koruyucu",
	}
	if containsAny(value, high) {
		return 100
	}
	if containsAny(value, medium) {
		return 40
	}
	if containsAny(value, low) {
		return -40
	}
	return 0
}

func containsAny(value string, terms []string) bool {
	for _, term := range terms {
		if strings.Contains(value, term) {
			return true
		}
	}
	return false
}

func maxPriority(a, b int) int {
	if a > b {
		return a
	}
	return b
}
