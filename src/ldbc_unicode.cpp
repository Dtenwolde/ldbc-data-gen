#include "ldbc_unicode.hpp"

#include "duckdb/common/exception.hpp"

#include "unicode/normalizer2.h"
#include "unicode/unistr.h"
#include "unicode/utf16.h"

namespace duckdb {

string LdbcJavaNormalizeNfdStripDiacritics(const string &value) {
	UErrorCode status = U_ZERO_ERROR;
	auto normalizer = icu::Normalizer2::getNFDInstance(status);
	if (U_FAILURE(status)) {
		throw InvalidInputException("Could not initialize ICU NFD normalizer");
	}

	auto input = icu::UnicodeString::fromUTF8(value);
	auto normalized = normalizer->normalize(input, status);
	if (U_FAILURE(status)) {
		throw InvalidInputException("Could not normalize string with ICU NFD");
	}

	icu::UnicodeString stripped;
	for (int32_t offset = 0; offset < normalized.length();) {
		auto codepoint = normalized.char32At(offset);
		offset += U16_LENGTH(codepoint);

		// Java's "\\p{InCombiningDiacriticalMarks}" targets the U+0300..U+036F block.
		if (codepoint >= 0x0300 && codepoint <= 0x036F) {
			continue;
		}
		stripped.append(codepoint);
	}

	string result;
	stripped.toUTF8String(result);
	return result;
}

string LdbcEmailBaseFromFirstName(const string &first_name) {
	auto result = LdbcJavaNormalizeNfdStripDiacritics(first_name);
	for (auto &character : result) {
		if (character == ' ') {
			character = '.';
		}
	}

	string collapsed;
	bool previous_dot = false;
	for (auto character : result) {
		if (character == '.') {
			if (previous_dot) {
				continue;
			}
			previous_dot = true;
		} else {
			previous_dot = false;
		}
		collapsed += character;
	}
	return collapsed;
}

int32_t LdbcJavaStringLength(const string &value) {
	return icu::UnicodeString::fromUTF8(value).length();
}

string LdbcJavaSubstring(const string &value, int32_t offset, int32_t length) {
	auto input = icu::UnicodeString::fromUTF8(value);
	auto substring = input.tempSubString(offset, length);
	string result;
	substring.toUTF8String(result);
	return result;
}

} // namespace duckdb
