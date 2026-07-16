#pragma once

#include "duckdb/common/common.hpp"

#include <unordered_map>

namespace duckdb {

struct LdbcDatagenConfig {
	string scale_factor_name;
	string resource_dir;
	unordered_map<string, string> properties;

	int64_t num_persons;
	int32_t block_size;
	int32_t start_year;
	int32_t num_years;
	int32_t delta;
	string degree_distribution;
	string knows_generator;
	string person_similarity;
	int32_t max_num_friends;
	int32_t min_num_tags_per_person;
	int32_t max_num_tags_per_person;
	int32_t max_emails;
	int32_t max_companies;
	double base_prob_correlated;
	double limit_prob_correlated;
	double prob_knows_deleted;
	double prob_english;
	double prob_second_lang;
	double missing_ratio;
	double prob_another_browser;
	double prob_uncorrelated_company;
	double prob_uncorrelated_organisation;
	double prob_top_univ;
	double tag_country_corr_prob;
	int32_t max_num_post_per_month;
	int32_t max_num_comments;
	int32_t max_num_flashmob_post_per_month;
	int32_t max_num_group_created_per_person;
	int32_t max_num_group_flashmob_post_per_month;
	int32_t max_num_group_post_per_month;
	int32_t max_num_like;
	int32_t max_group_size;
	int32_t max_num_photo_albums_per_month;
	int32_t max_num_photo_per_albums;
	int32_t max_num_popular_places;
	int32_t max_num_tag_per_flashmob_post;
	int32_t flashmob_tags_per_month;
	int32_t min_text_size;
	int32_t max_text_size;
	int32_t min_comment_size;
	int32_t max_comment_size;
	int32_t min_large_post_size;
	int32_t max_large_post_size;
	int32_t min_large_comment_size;
	int32_t max_large_comment_size;
	double group_moderator_prob;
	double prob_diff_ip_travel_season;
	double prob_diff_ip_not_travel_season;
	double ratio_reduce_text;
	double ratio_large_post;
	double ratio_large_comment;
	double prob_interest_flashmob_tag;
	double prob_random_per_level;
	double flashmob_tag_min_level;
	double flashmob_tag_max_level;
	double flashmob_tag_dist_exp;
	double prob_forum_deleted;
	double prob_memb_deleted;
	double prob_photo_deleted;
	double prob_comment_deleted;
	double prob_like_deleted;
	double bulkload_portion = 0.97;

	static constexpr const char *EMBEDDED_RESOURCE_DIR = "embedded";
	static constexpr const char *DEFAULT_RESOURCE_DIR = EMBEDDED_RESOURCE_DIR;
	static constexpr double ALPHA = 0.4;

	static LdbcDatagenConfig Load(double scale_factor, const string &resource_dir = DEFAULT_RESOURCE_DIR);
};

} // namespace duckdb
