#include "String.h"
#include "../object.h"

String::String() {}
String::String(string value) {
	this->value = value;
}
String::String(const char value[])
{
	this->value = value;
}

int String::count(string target)
{
	size_t target_len = target.length();
	int counter = 0;

	size_t end = this->len() - target_len + 1;
	for (size_t i = 0; i < end; i++)
	{
		if (value.substr(i, target_len) == target)
			++counter;
	}

	return counter;
}

String String::stripl()
{
	auto start_index = value.find_first_not_of(' ');
	if (start_index == std::string::npos) return *this;
	return String(value.substr((int)start_index, value.length()));
}
String String::stripr()
{
	auto end_index = value.find_last_not_of(' ');
	if (end_index == std::string::npos) return *this;
	return String(value.substr(0, (int)end_index + 1));
}
String String::strip() {
	return stripl().stripr();
}

String String::substr(int start, int end)
{
	if (end == -1) {
		end = len();
	}
	return String(value.substr(start, end));
}

size_t String::find(string target, int start)
{
	size_t index = value.find(target, start);
	if (index == string::npos) return -1;
	return index;
}

bool String::contains(string target)
{
	return this->find(target) != -1;
}

vector<size_t> String::get_min_find(
	string value,
	vector<string> from,
	size_t start_pos) {

	size_t index = string::npos;
	size_t from_index = 0;

	for (int i = 0; i < from.size(); i++)
	{
		auto curr_index = value.find(from[i], start_pos);
		if (curr_index < index) {
			index = curr_index;
			from_index = i;
		}
	}

	return { index, from_index };
}

String String::replace(string from, string to)
{
	string str = value;

	size_t to_len = to.length();

	vector<Object> froms_ = String(from).split("|");

	vector<string> froms(froms_.size());
	vector<size_t> froms_len(froms_.size());

	vector<size_t> start_pos_and_froms_index = { 0, 0 };

	for (size_t i = 0; i < froms_.size(); i++)
	{
		froms[i] = froms_[i].get();
		froms_len[i] = froms_[i].len();
	}


	while (1) {

		start_pos_and_froms_index = get_min_find(
			str,
			froms,
			start_pos_and_froms_index[0]
		);

		if (start_pos_and_froms_index[0] == string::npos) {
			break;
		}

		str = str.replace(
			start_pos_and_froms_index[0],
			froms_len[start_pos_and_froms_index[1]],
			to);

		start_pos_and_froms_index[0] += (int)to_len;
	}

	return String(str);
}

vector<Object> String::split(string sep, bool parsing_numbers)
{

	string value_str = value;

	if (sep == "") {
		size_t size = value.length();
		vector<Object> res_vec(size);
		for (size_t i = 0; i < size; i++)
		{
			res_vec[i] = parsing_numbers ?
				Object(string(1, value_str[i])) :
				Object(string(1, value_str[i]), Dtype::STRING);
		}
		return res_vec;
	}

	int vec_size = this->count(sep);
	vector<Object> res_vec(vec_size + 1);

	size_t start = 0;
	size_t end = 0;

	int sep_len = (int)sep.length();
	int idx = 0;
	string temp_str;

	while ((end = value_str.find(sep, start)) != std::string::npos) {

		temp_str = value_str.substr(start, end - start);

		if (temp_str.empty() == false) {

			res_vec[idx] = parsing_numbers ?
				Object(temp_str) :
				Object(temp_str, Dtype::STRING);
		}

		++idx;
		start = end + sep_len;
	}

	temp_str = value_str.substr(start);

	if (temp_str.empty() == false) {
		res_vec[idx] = temp_str;
	}

	return res_vec;
}




String String::up()
{
	std::string result = value;
	for (char& c : result) {
		c = std::toupper(c);
	}
	return String(result);
}
String String::lw()
{
	std::string result = value;
	for (char& c : result) {
		c = std::tolower(c);
	}
	return String(result);
}
String String::title()
{
	std::string result = value;
	size_t size = this->len();

	if (std::isalpha(result[0])) {
		result[0] = std::toupper(result[0]);
	}

	for (size_t i = 1; i < size; i++)
	{
		if (std::isalpha(result[i]) && std::isspace(result[i - 1])) {
			result[i] = std::toupper(result[i]);
		}
		else {
			result[i] = std::tolower(result[i]);
		}
	}
	return String(result);
}

Object String::extract_pattern(const std::string& pattern)
{

	std::regex regex_pattern(pattern);
	std::smatch match; // For storing the matched result

	// If a match is found, extract the first occurrence
	if (std::regex_search(value, match, regex_pattern) && !match.empty()) {
		return Object(match.str(0));
	}
	return Object();
}

vector<Object> String::extract_pattern(
	const vector<Object>& values, const string& pattern)
{
	// Create a regex object from the given pattern
	std::regex regex_pattern(pattern);

	// Vector to store the results
	vector<Object> extracted_values;
	extracted_values.reserve(values.size());

	// Iterate through each string in the input vector
	for (const auto& str_value : values) {
		if (str_value.type != Dtype::STRING) {
			extracted_values.emplace_back(Object());
		}

		std::smatch match; // For storing the matched result

		// If a match is found, extract the first occurrence
		const string curr_str = str_value.get_val<String>().get();

		if (std::regex_search(curr_str, match, regex_pattern) && !match.empty()) {

			extracted_values.emplace_back(
				Object(match.str(0))
			); // Store the matched string
		}
		else {
			extracted_values.emplace_back(Object());
		}
	}

	return extracted_values;
}


String String::join(string sep, vector<Object> arr)
{

	size_t arr_size = arr.size() - 1;
	string results = "";
	int i = 0;

	while (i < arr_size)
	{
		results += arr[i].get();
		results += sep;
		++i;
	}
	results += arr[i].get();

	return String(results);
}

String String::operator + (const String& other) const {
	return String(value + other.value);
}

String String::operator - (const String& other) {

	int value_len = (int)value.length(), other_value_len = other.len();

	if (value_len < other_value_len) {

		cout << "\nOperation: " << value << " - " << other.value << endl;
		cout << "Unsupported operation: can't make subtract big str from small one." << endl;
		assert(false);
	}

	return this->replace(other.value, " ");
}

String String::operator * (const float& other) const {

	if (other == 1) return *this;

	string temp_value = this->value;
	if (other < 0) {
		std::reverse(temp_value.begin(), temp_value.end());
	}

	string temp_str = "";
	int s_int = (int)other;
	float s_float = other - (int)other;
	int part_of = (int)(s_float * (int)value.length());

	for (int i = 0; i < s_int; i++)
	{
		temp_str += value;
	}
	for (int i = 0; i < part_of; i++)
	{
		temp_str += value[i];
	}
	return String(temp_str);
}

String String::operator + (const float& other) const
{
	assert(0 && "Invalid Operation: you trying to add number to a String.");
	return String();
}

String String::operator - (const float& other) const
{
	assert(0 && "Invalid Operation: you trying to subtract number to a String.");

	return String();
}

String String::operator / (const float& other) const
{
	assert(0 && "Invalid Operation: you trying to divide the String by number.");

	return String();
}

String String::operator / (const String& other) const
{
	assert(0 && "Invalid Operation: you trying to divide the String by another String.");
	return String();
}

String String::operator * (const String& other) const
{
	assert(0 && "Invalid Operation: you trying to multiply the String by String.");
	return String();
}

void String::operator += (const String& other)
{
	*this = *this + other;
}
void String::operator -= (const String& other)
{
	*this = *this - other;
}
void String::operator *= (const float&  other) {

	*this = *this * other;
}


bool String::operator < (const String& other) const {
	return value < other.value;
}
bool String::operator > (const String& other) const
{
	return value > other.value;
}
bool String::operator <=(const String& other) const
{
	return value <= other.value;
}
bool String::operator >=(const String& other) const
{
	return value >= other.value;
}
bool String::operator ==(const String& other) const
{
	return value == other.value;
}
bool String::operator !=(const String& other) const
{
	return value != other.value;
}