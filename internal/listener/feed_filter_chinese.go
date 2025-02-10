package listener

var expectedChineseJapaneseRatio = 1.5

// https://github.com/pemistahl/lingua-go/issues/38
func hasChinese(text string) bool {
	zhCount := 0
	jaCount := 0
	for _, char := range text {
		if isJapanese(char) {
			jaCount++
		}
		if isChinese(char) {
			zhCount++
		}
	}
	ratio := float64(zhCount) / float64(jaCount)
	return ratio > expectedChineseJapaneseRatio
}

func isChinese(c rune) bool {
	// Chinese Unicode range
	if (c >= '\u3400' && c <= '\u4db5') || // CJK Unified Ideographs Extension A
		(c >= '\u4e00' && c <= '\u9fed') || // CJK Unified Ideographs
		(c >= '\uf900' && c <= '\ufaff') { // CJK Compatibility Ideographs
		return true
	}

	return false
}

func isJapanese(c rune) bool {
	// Japanese Unicode range
	if (c >= '\u3021' && c <= '\u3029') || // Japanese Hanzi
		(c >= '\u3040' && c <= '\u309f') || // Hiragana
		(c >= '\u30a0' && c <= '\u30ff') || // Katakana
		(c >= '\u31f0' && c <= '\u31ff') || // Katakana Phonetic Extension
		(c >= '\uf900' && c <= '\ufaff') { // CJK Compatibility Ideographs
		return true
	}

	return false
}
