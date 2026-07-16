#include "ldbc_resources.hpp"

#include "ldbc_datagen_config.hpp"

#include "duckdb/common/exception.hpp"

#include <fstream>
#include <sstream>

namespace duckdb {

extern bool LdbcGetEmbeddedResource(const string &relative_path, const char *&data, idx_t &size);

class LdbcMemoryBuffer : public std::streambuf {
public:
	LdbcMemoryBuffer(const char *data, idx_t size) {
		auto begin = const_cast<char *>(data);
		setg(begin, begin, begin + size);
	}
};

class LdbcMemoryInputStream : public std::istream {
public:
	LdbcMemoryInputStream(const char *data, idx_t size) : std::istream(nullptr), buffer(data, size) {
		rdbuf(&buffer);
	}

private:
	LdbcMemoryBuffer buffer;
};

bool LdbcUsesEmbeddedResources(const string &resource_dir) {
	return resource_dir == LdbcDatagenConfig::EMBEDDED_RESOURCE_DIR;
}

string LdbcResourcePath(const string &base, const string &path) {
	if (base.empty()) {
		throw InvalidInputException("LDBC resource path base must not be empty");
	}
	if (base.back() == '/') {
		return base + path;
	}
	return base + "/" + path;
}

static string EmbeddedRelativePath(const string &path) {
	const string prefix = string(LdbcDatagenConfig::EMBEDDED_RESOURCE_DIR) + "/";
	if (path == LdbcDatagenConfig::EMBEDDED_RESOURCE_DIR) {
		return "";
	}
	if (path.size() > prefix.size() && path.substr(0, prefix.size()) == prefix) {
		return path.substr(prefix.size());
	}
	return "";
}

LdbcResource LdbcOpenResourcePath(const string &path) {
	auto relative_path = EmbeddedRelativePath(path);
	if (!relative_path.empty()) {
		const char *data = nullptr;
		idx_t size = 0;
		if (!LdbcGetEmbeddedResource(relative_path, data, size)) {
			throw IOException("Could not open embedded LDBC resource '%s'", relative_path);
		}
		LdbcResource result;
		result.path = path;
		result.stream = std::unique_ptr<std::istream>(new LdbcMemoryInputStream(data, size));
		return result;
	}

	std::unique_ptr<std::ifstream> file(new std::ifstream(path));
	if (!file->is_open()) {
		throw IOException("Could not open LDBC resource file '%s'", path);
	}
	LdbcResource result;
	result.path = path;
	result.stream = std::move(file);
	return result;
}

LdbcResource LdbcOpenResource(const string &resource_dir, const string &relative_path) {
	return LdbcOpenResourcePath(LdbcResourcePath(resource_dir, relative_path));
}

LdbcResource LdbcOpenResource(const string &base, const string &path, const string &file_name) {
	return LdbcOpenResourcePath(LdbcResourcePath(LdbcResourcePath(base, path), file_name));
}

string LdbcReadResourceText(const string &resource_dir, const string &relative_path) {
	auto resource = LdbcOpenResource(resource_dir, relative_path);
	std::stringstream buffer;
	buffer << resource.stream->rdbuf();
	return buffer.str();
}

} // namespace duckdb
