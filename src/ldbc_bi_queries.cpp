#include "ldbc_bi_queries.hpp"
#include "ldbc_bi_queries_generated.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// The query bodies are adapted from the official LDBC SNB BI Umbra SQL
// implementation, licensed under Apache-2.0:
// https://github.com/ldbc/ldbc_snb_bi/tree/main/umbra/queries

struct LdbcBiQuerySpec {
	const char *name;
	const char *parameters;
};

static constexpr LdbcBiQuerySpec LDBC_BI_QUERY_SPECS[] = {
    {"Posting summary", "datetime"},
    {"Tag evolution", "date, tag_class"},
    {"Popular topics in a country", "tag_class, country"},
    {"Top message creators by country", "date"},
    {"Most active posters in a given topic", "tag"},
    {"Most authoritative users on a given topic", "tag"},
    {"Related topics", "tag"},
    {"Central person for a tag", "tag, start_date, end_date"},
    {"Top thread initiators", "start_date, end_date"},
    {"Experts in social circle", "person_id, country, tag_class"},
    {"Friend triangles", "country, start_date, end_date"},
    {"How many persons have a given number of messages", "start_date, length_threshold, languages"},
    {"Zombies in a country", "country, end_date"},
    {"International dialog", "country1, country2"},
    {"Trusted connection paths through forums", "person1_id, person2_id, start_date, end_date"},
    {"Fake news detection", "tag_a, date_a, tag_b, date_b, max_knows_limit"},
    {"Information propagation analysis", "tag, delta"},
    {"Friend recommendation", "tag"},
    {"Interaction path between cities", "city1_id, city2_id"},
    {"Recruitment", "person2_id, company"},
};

struct LdbcBiParameter {
	const char *name;
	const char *placeholder;
};

static vector<LdbcBiParameter> LdbcBiParameters(idx_t query_number) {
	switch (query_number) {
	case 1:
		return {{"datetime", ":datetime"}};
	case 2:
		return {{"tag_class", ":tagClass"}, {"date", ":date"}};
	case 3:
		return {{"tag_class", ":tagClass"}, {"country", ":country"}};
	case 4:
		return {{"date", ":date"}};
	case 5:
	case 6:
	case 7:
	case 18:
		return {{"tag", ":tag"}};
	case 8:
		return {{"start_date", ":startDate"}, {"end_date", ":endDate"}, {"tag", ":tag"}};
	case 9:
		return {{"start_date", ":startDate"}, {"end_date", ":endDate"}};
	case 10:
		return {{"person_id", ":personId"}, {"tag_class", ":tagClass"}, {"country", ":country"}};
	case 11:
		return {{"start_date", ":startDate"}, {"end_date", ":endDate"}, {"country", ":country"}};
	case 12:
		return {{"length_threshold", ":lengthThreshold"}, {"start_date", ":startDate"}, {"languages", ":languages"}};
	case 13:
		return {{"end_date", ":endDate"}, {"country", ":country"}};
	case 14:
		return {{"country1", ":country1"}, {"country2", ":country2"}};
	case 15:
		return {{"person1_id", ":person1Id"},
		        {"person2_id", ":person2Id"},
		        {"start_date", ":startDate"},
		        {"end_date", ":endDate"}};
	case 16:
		return {{"max_knows_limit", ":maxKnowsLimit"},
		        {"date_a", ":dateA"},
		        {"date_b", ":dateB"},
		        {"tag_a", ":tagA"},
		        {"tag_b", ":tagB"}};
	case 17:
		return {{"delta", ":delta"}, {"tag", ":tag"}};
	case 19:
		return {{"city1_id", ":city1Id"}, {"city2_id", ":city2Id"}};
	case 20:
		return {{"person2_id", ":person2Id"}, {"company", ":company"}};
	default:
		throw InternalException("Unknown LDBC BI query number");
	}
}

static const char *LDBC_BI_COMPATIBILITY_CTES = R"LDBCBI(
Person AS (SELECT * FROM @DATASET@.Person),
Place AS (SELECT * FROM @DATASET@.Place),
Organisation AS (SELECT * FROM @DATASET@.Organisation),
Tag AS (SELECT * FROM @DATASET@.Tag),
TagClass AS (SELECT * FROM @DATASET@.TagClass),
Forum AS (SELECT * FROM @DATASET@.Forum),
Forum_hasMember_Person AS (SELECT * FROM @DATASET@.Forum_hasMember_Person),
Person_hasInterest_Tag AS (SELECT * FROM @DATASET@.Person_hasInterest_Tag),
Person_knows_Source AS (SELECT * FROM @DATASET@.Person_knows_Person),
Person_knows_Person AS (
    SELECT creationDate, Person1Id, Person2Id FROM Person_knows_Source
    UNION ALL
    SELECT creationDate, Person2Id, Person1Id FROM Person_knows_Source
),
Person_studyAt_University AS (SELECT * FROM @DATASET@.Person_studyAt_University),
Person_workAt_Company AS (SELECT * FROM @DATASET@.Person_workAt_Company),
PostSource AS (SELECT * FROM @DATASET@.Post),
CommentSource AS (SELECT * FROM @DATASET@.Comment),
Post_hasTag_Tag AS (SELECT * FROM @DATASET@.Post_hasTag_Tag),
Comment_hasTag_Tag AS (SELECT * FROM @DATASET@.Comment_hasTag_Tag),
Person_likes_Post AS (SELECT * FROM @DATASET@.Person_likes_Post),
Person_likes_Comment AS (SELECT * FROM @DATASET@.Person_likes_Comment),
City AS (
    SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
    FROM Place WHERE type = 'City'
),
Country AS (
    SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
    FROM Place WHERE type = 'Country'
),
Company AS (
    SELECT id, name, url, LocationPlaceId
    FROM Organisation WHERE type = 'Company'
),
University AS (
    SELECT id, name, url, LocationPlaceId
    FROM Organisation WHERE type = 'University'
),
MessageTree(creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile,
            locationIP, browserUsed, length, CreatorPersonId, ContainerForumId,
            LocationCountryId, ParentMessageId) AS (
    SELECT creationDate, id, id, language, content, imageFile, locationIP, browserUsed,
           length, CreatorPersonId, ContainerForumId, LocationCountryId, NULL::BIGINT
    FROM PostSource
    UNION ALL
    SELECT c.creationDate, c.id, parent.RootPostId, parent.RootPostLanguage, c.content,
           NULL::VARCHAR, c.locationIP, c.browserUsed, c.length, c.CreatorPersonId,
           parent.ContainerForumId, c.LocationCountryId,
           coalesce(c.ParentPostId, c.ParentCommentId)
    FROM CommentSource c
    JOIN MessageTree parent
      ON parent.MessageId = coalesce(c.ParentPostId, c.ParentCommentId)
),
Message AS (SELECT * FROM MessageTree),
Person_likes_Message AS (
    SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
    UNION ALL
    SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment
),
Message_hasTag_Tag AS (
    SELECT creationDate, PostId AS MessageId, TagId FROM Post_hasTag_Tag
    UNION ALL
    SELECT creationDate, CommentId AS MessageId, TagId FROM Comment_hasTag_Tag
),
Comment_View AS (
    SELECT creationDate, MessageId AS id, locationIP, browserUsed, content, length,
           CreatorPersonId, LocationCountryId, ParentMessageId
    FROM Message WHERE ParentMessageId IS NOT NULL
),
Post_View AS (
    SELECT creationDate, MessageId AS id, imageFile, locationIP, browserUsed,
           RootPostLanguage, content, length, CreatorPersonId, ContainerForumId,
           LocationCountryId
    FROM Message WHERE ParentMessageId IS NULL
),
Top100PopularForumsQ04 AS (
    SELECT membership.id, Forum.creationDate, membership.maxNumberOfMembers
    FROM (
        SELECT ForumId AS id, max(numberOfMembers) AS maxNumberOfMembers
        FROM (
            SELECT fm.ForumId, count(Person.id) AS numberOfMembers,
                   City.PartOfCountryId AS CountryId
            FROM Forum_hasMember_Person fm
            JOIN Person ON Person.id = fm.PersonId
            JOIN City ON City.id = Person.LocationCityId
            GROUP BY City.PartOfCountryId, fm.ForumId
        ) ForumMembershipPerCountry
        GROUP BY ForumId
    ) membership
    JOIN Forum ON membership.id = Forum.id
),
PopularityScoreQ06 AS (
    SELECT message.CreatorPersonId AS person2id, count(*) AS popularityScore
    FROM Message message
    JOIN Person_likes_Message likes ON likes.MessageId = message.MessageId
    GROUP BY message.CreatorPersonId
),
PathQ19 AS (
    WITH weights(src, dst, w) AS (
        SELECT pp.Person1Id, pp.Person2Id,
               greatest(round(40 - sqrt(count(*)))::BIGINT, 1) AS w
        FROM Person_knows_Person pp
        JOIN Message m1 ON true
        JOIN Message m2
          ON pp.Person1Id = least(m1.CreatorPersonId, m2.CreatorPersonId)
         AND pp.Person2Id = greatest(m1.CreatorPersonId, m2.CreatorPersonId)
         AND m1.ParentMessageId = m2.MessageId
         AND m1.CreatorPersonId <> m2.CreatorPersonId
        WHERE pp.Person1Id < pp.Person2Id
        GROUP BY pp.Person1Id, pp.Person2Id
    )
    SELECT src, dst, w FROM weights
    UNION ALL
    SELECT dst, src, w FROM weights
),
PathQ20 AS (
    SELECT p1.PersonId AS src, p2.PersonId AS dst,
           min(abs(p1.classYear - p2.classYear)) + 1 AS w
    FROM Person_knows_Person pp
    JOIN Person_studyAt_University p1 ON pp.Person1Id = p1.PersonId
    JOIN Person_studyAt_University p2
      ON pp.Person2Id = p2.PersonId AND p1.UniversityId = p2.UniversityId
    GROUP BY p1.PersonId, p2.PersonId
)
)LDBCBI";

static string LdbcBiQueryBody(idx_t query_number) {
	string query = LDBC_BI_QUERY_TEXT[query_number - 1];
	auto comment_end = query.find("*/");
	if (comment_end != string::npos) {
		query = query.substr(comment_end + 2);
	}
	StringUtil::Trim(query);
	if (!query.empty() && query.back() == ';') {
		query.pop_back();
	}
	StringUtil::Trim(query);
	return query;
}

static string AddCompatibilityCtes(string query, const string &dataset) {
	string compatibility = StringUtil::Replace(LDBC_BI_COMPATIBILITY_CTES, "@DATASET@", dataset);
	if (StringUtil::CIStartsWith(query, "WITH RECURSIVE")) {
		return "WITH RECURSIVE\n" + compatibility + ",\n" + query.substr(14);
	}
	if (StringUtil::CIStartsWith(query, "WITH")) {
		return "WITH RECURSIVE\n" + compatibility + ",\n" + query.substr(4);
	}
	return "WITH RECURSIVE\n" + compatibility + "\n" + query;
}

static string PragmaLdbcBiQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto query_number = parameters.values[0].GetValue<int64_t>();
	if (query_number < 1 || query_number > 20) {
		throw InvalidInputException("LDBC BI query number must be between 1 and 20");
	}

	const auto &default_entry = ClientData::Get(context).catalog_search_path->GetDefault();
	string catalog = default_entry.GetCatalog().GetIdentifierName();
	string schema = default_entry.GetSchema().GetIdentifierName();
	if (catalog.empty()) {
		catalog = DatabaseManager::GetDefaultDatabase(context).GetIdentifierName();
	}
	auto catalog_entry = parameters.named_parameters.find("catalog");
	if (catalog_entry != parameters.named_parameters.end()) {
		if (catalog_entry->second.IsNull()) {
			throw InvalidInputException("LDBC BI catalog cannot be NULL");
		}
		catalog = StringValue::Get(catalog_entry->second);
	}
	auto schema_entry = parameters.named_parameters.find("schema");
	if (schema_entry != parameters.named_parameters.end()) {
		if (schema_entry->second.IsNull()) {
			throw InvalidInputException("LDBC BI schema cannot be NULL");
		}
		schema = StringValue::Get(schema_entry->second);
	}
	if (catalog.empty() || schema.empty()) {
		throw InvalidInputException("LDBC BI catalog and schema cannot be empty");
	}

	auto query = LdbcBiQueryBody(NumericCast<idx_t>(query_number));
	if (query_number == 12) {
		query = StringUtil::Replace(query, "IN :languages", "IN (SELECT unnest(:languages))");
	}
	for (const auto &parameter : LdbcBiParameters(NumericCast<idx_t>(query_number))) {
		auto entry = parameters.named_parameters.find(parameter.name);
		if (entry == parameters.named_parameters.end()) {
			throw InvalidInputException("LDBC BI query %d requires parameter '%s'", query_number, parameter.name);
		}
		if (entry->second.IsNull()) {
			throw InvalidInputException("LDBC BI query parameter '%s' cannot be NULL", parameter.name);
		}
		query = StringUtil::Replace(std::move(query), parameter.placeholder, entry->second.ToSQLString());
	}

	const auto dataset = SQLIdentifier::ToString(catalog) + "." + SQLIdentifier::ToString(schema);
	return AddCompatibilityCtes(std::move(query), dataset);
}

struct LdbcBiQueriesState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

static unique_ptr<FunctionData> LdbcBiQueriesBind(ClientContext &, TableFunctionBindInput &,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names = {"query_nr", "name", "parameters", "query"};
	return_types = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> LdbcBiQueriesInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<LdbcBiQueriesState>();
}

static void LdbcBiQueriesFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<LdbcBiQueriesState>();
	idx_t count = 0;
	while (state.offset < 20 && count < STANDARD_VECTOR_SIZE) {
		output.data[0].SetValue(count, Value::INTEGER(NumericCast<int32_t>(state.offset + 1)));
		output.data[1].SetValue(count, Value(LDBC_BI_QUERY_SPECS[state.offset].name));
		output.data[2].SetValue(count, Value(LDBC_BI_QUERY_SPECS[state.offset].parameters));
		output.data[3].SetValue(count, Value(LdbcBiQueryBody(state.offset + 1)));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void RegisterLdbcBiQueries(ExtensionLoader &loader) {
	auto pragma = PragmaFunction::PragmaCall("ldbc_bi", PragmaLdbcBiQuery, {LogicalType::BIGINT});
	pragma.named_parameters["catalog"] = LogicalType::VARCHAR;
	pragma.named_parameters["schema"] = LogicalType::VARCHAR;
	pragma.named_parameters["datetime"] = LogicalType::TIMESTAMP;
	pragma.named_parameters["date"] = LogicalType::TIMESTAMP;
	pragma.named_parameters["tag_class"] = LogicalType::VARCHAR;
	pragma.named_parameters["country"] = LogicalType::VARCHAR;
	pragma.named_parameters["tag"] = LogicalType::VARCHAR;
	pragma.named_parameters["start_date"] = LogicalType::TIMESTAMP;
	pragma.named_parameters["end_date"] = LogicalType::TIMESTAMP;
	pragma.named_parameters["person_id"] = LogicalType::BIGINT;
	pragma.named_parameters["languages"] = LogicalType::LIST(LogicalType::VARCHAR);
	pragma.named_parameters["length_threshold"] = LogicalType::BIGINT;
	pragma.named_parameters["country1"] = LogicalType::VARCHAR;
	pragma.named_parameters["country2"] = LogicalType::VARCHAR;
	pragma.named_parameters["person1_id"] = LogicalType::BIGINT;
	pragma.named_parameters["person2_id"] = LogicalType::BIGINT;
	pragma.named_parameters["tag_a"] = LogicalType::VARCHAR;
	pragma.named_parameters["date_a"] = LogicalType::DATE;
	pragma.named_parameters["tag_b"] = LogicalType::VARCHAR;
	pragma.named_parameters["date_b"] = LogicalType::DATE;
	pragma.named_parameters["max_knows_limit"] = LogicalType::BIGINT;
	pragma.named_parameters["delta"] = LogicalType::BIGINT;
	pragma.named_parameters["city1_id"] = LogicalType::BIGINT;
	pragma.named_parameters["city2_id"] = LogicalType::BIGINT;
	pragma.named_parameters["company"] = LogicalType::VARCHAR;
	loader.RegisterFunction(pragma);

	TableFunction queries("ldbc_bi_queries", {}, LdbcBiQueriesFunction, LdbcBiQueriesBind, LdbcBiQueriesInit);
	loader.RegisterFunction(queries);
}

} // namespace duckdb
