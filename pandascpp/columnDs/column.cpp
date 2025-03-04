#include "../columnDs/column.h"
#include "../tableclass/Table.h"


// Getter/Setter Attributes

void column::set_name(string name)
{
	this->name = name;
	int namelen = (int)name.length();
	maxLenStr = maxLenStr < namelen ? namelen : maxLenStr;
}

string column::type_str() const
{
	return Object::type_str(type);
}

int column::len()
{
	return (int)values.size();
}

////////////////

// Append Functions

void column::append(Object val)
{
	Dtype tempType = val.type;

	if (size == 0) {

		values[size] = val;
		type = tempType;
		secType = val.secType;
	}
	else {

		if (this->mixed_type) {

			type    = std::max(tempType, this->type);
			secType = std::max(val.secType, secType);

			values[size] = val;
		}
		else {

			if (tempType != type && type == Dtype::NA || 
				tempType != type && Object::get_sec_type(tempType) == Dtype::NUMBER) {

				type    = tempType;
				secType = val.secType;

				values[size] = val;
			}
			else if (type == tempType) {
				values[size] = val;
			}
			else {

				cout << "val: "
					<< val
					<< " invalid data type, this column data type is: "
					<< type
					<< "\n in column: "
					<< name
					<< "\n";

				assert(false);
			}
		}
	}

	++size;
}
void column::append(const vector<Object>& vals)
{
	for (const Object& val : vals)  append(val);
}

void column::appendPushBack(Object val)
{
	Dtype tempType = val.type;

	if (size == 0) {
		values.push_back(val);
		type = tempType;
		secType = val.secType;
	}
	else {
		if (tempType == Dtype::NA) {
			values.push_back(val);
		}
		else if (mixed_type) {
			type = std::max(tempType, type);
			secType = Object::get_sec_type(type);

			values.push_back(val);
		}
		else if (
			(tempType != type && type == Dtype::NA) ||
			(tempType != type && Object::get_sec_type(tempType) == Dtype::NUMBER)
			) {

			values.push_back(val);

			if (tempType > type) {
				type = tempType;
				secType = val.secType;
			}
		}
		else if (tempType == type) {
			values.push_back(val);
		}
		else {
			cout << "val: " << val << " invalid data type, this column data type is: "
				 << type    << "\n in column: " << name << "\n";
			assert(false);
		}
	}

	++size;
}
void column::appendPushBack(const vector<Object>& vals)
{
	for (const Object& val : vals) appendPushBack(val);
}

////////////////

// Print Function

void column::printAt(int idx) {

	cout << setfill(' ');
	if (idx == -1) {
		cout << " "
			<< name
			<< setw(maxLenStr - name.length() + 2)
			<< " +";
	}
	else cout << " "
		<< ((values[idx].type == Dtype::INT8) ? (int)values[idx].get_val<int8_t>() : values[idx])
		<< setw(maxLenStr - values[idx].len() + 2)
		<< " |";
}

void column::p(int from, int to) {

	if (from == -1 && to == -1) { from = 0; to = size; }
	else if (from != -1 && to == -1) { to = from + 1; }


	vector<Object> index_vec = Tools::gen_seq_obj(from, to);

	int newsize = to - from;
	vector<Object> ranged_value_vec(newsize);

	for (int i = 0; i < newsize; i++)
	{
		ranged_value_vec[i] = values[from++];
	}

	column index_col("Index", index_vec);
	column ranged_value_col(name, ranged_value_vec);

	Table table({ index_col, ranged_value_col });
	table.print();
}

void column::print_naidxs() {

	column naIndxCol("NA Index", naidxs());
	Table tabel({ naIndxCol });
	tabel.print();
}

void column::phead(int limit)
{
	this->range(0, limit).p();
}

void column::ptail(int limit)
{
	this->range(this->size - limit, this->size).p();
}

////////////////

// Get Column Values

column column::range(int  from, int  to)
{
	if (from >= to) assert(false && "Put Valid Range (\"to\" must be greater than \"from\".)");

	int newsize = to - from;
	vector<Object> tempVec(newsize);

	for (int i = 0; i < newsize; i++) {
		tempVec[i] = values[from++];
	}

	return column(column::name, tempVec);
}

column column::head(int limit) {
	return this->range(0, limit);
}
column column::tail(int limit) {
	return this->range(this->size - limit, this->size);
}


template<typename Container>
inline column column::get_indexes(
	const Container& indexes, typename std::enable_if_t<is_iterable_container<Container>::value>*)
{
	int new_size = (int)indexes.size();

	column new_col = *this;
	new_col.size = new_size;
	new_col.values = std::move(vector<Object>(new_col.values.begin(), new_col.values.begin() + new_size));

	int new_i = 0;
	for (const int& in_i : indexes) {
		new_col.values[new_i++] = values[in_i];
	}

	return new_col;
}


column column::get_indexes(std::initializer_list<int> indexes)
{
	return get_indexes(vector<int>(indexes.begin(), indexes.end()));
}

column column::copy() { return *this; }

////////////////

// Handling Missing Data

set<int> column::naidxs()
{
	set<int> idxs;

	for (int i = 0; i < size; i++) if (values[i].type == Dtype::NA) idxs.insert(i);
	
	return idxs;
}
set<int> column::not_naidxs()
{
	set<int> idxs;

	for (int i = 0; i < size; i++) if (values[i].type != Dtype::NA) idxs.insert(i);

	return idxs;
}

column column::fillna(Object val, bool inplace)
{
	assert(val.type != Dtype::NA && "Value must not be NA too!");

	type = type == Dtype::NA ? val.type : type;

	if (!mixed_type) {
		assert(val.type == type && "must value be same type.");
	}

	set<int> currnaindxs = naidxs();

	if (inplace) {

		for (const int& idx : currnaindxs) {
			values[idx] = val;
		}
		return column();
	}
	vector<Object> new_values = values;

	for (const int& idx : currnaindxs) {
		new_values[idx] = val;
	}
	return column(name, std::move(new_values));
}
column column::fillna(bool inplace)
{
	Object val = secType == Dtype::NUMBER ? Object(mean()) : mode();

	return fillna(val, inplace);
}

////////////////

// Statistical Operations

double column::min()
{
	assert(Object::get_sec_type(type) == Dtype::NUMBER && "Type Must be number to get min");


	double min_val = values[0].value_num();

#pragma omp parallel for reduction(min:min_val)
	for (int i = 1; i < size; ++i) {

		if (values[i].type == Dtype::NA) continue;

		auto temp = values[i].value_num();
		if (temp < min_val) {
			min_val = temp;
		}
	}

	return min_val;
}
double column::max()
{

	assert(Object::get_sec_type(type) == Dtype::NUMBER && "Type Must be number to get max");

	double max_val = values[0].value_num();

#pragma omp parallel for reduction(max:max_val)
	for (int i = 1; i < size; ++i) {

		if (values[i].type == Dtype::NA) continue;

		auto temp = values[i].value_num();
		if (temp > max_val) {
			max_val = temp;
		}
	}

	return max_val;
}

double column::sum ()
{
	double sumValue = 0;
	for (int i = 0; i < size; i++)
	{
		if (values[i].secType == Dtype::NUMBER) {
			sumValue += values[i].value_num();
		}
	}

	return sumValue;
}
double column::mean()
{
	assert(secType == Dtype::NUMBER && "Type Must be number to get mean");
	return this->sum() / values.size();
}
double column::std  (bool haveMean, double meanValue)

{
	assert(secType == Dtype::NUMBER && "Type Must be number to get std");

	double meanv = haveMean ? meanValue : mean();
	double sum = 0.0;

	for (Object value : values) {
		if (value.type == Dtype::NA) continue;

		sum += std::pow((value - meanv).value_num(), 2);
	}

	return std::pow(sum / values.size(), .5);
}
double column::std_2(bool haveMean, double meanValue)
{
	assert(secType == Dtype::NUMBER && "Type Must be number to get std");

	double meanv = haveMean ? meanValue : mean();
	double sum = 0.0;

#pragma omp parallel for reduction(+:sum)
	for (int i = 0; i < size; i++)
	{
		if (values[i].secType == Dtype::NUMBER) {
			sum += std::pow(values[i].value_num() - meanv, 2);
		}

	}

	return std::pow(sum / size, .5);
}
double column::median(bool sorted, vector<double> optionalValues)
{
	assert(secType == Dtype::NUMBER && "Type Must be number to get median");

	int const newSize = (int)(values.size() - naidxs().size());

	vector < double > tempvs(newSize);


	if (sorted == 0) {
		int idx = 0;

		for (int i = 0; i < size; i++)
		{
			if (values[i].secType == Dtype::NUMBER) {
				tempvs[idx++] = values[i].value_num();
			}
		}

		std::sort(tempvs.begin(), tempvs.end());
	}
	else {
		tempvs = optionalValues;
	}


	if (newSize % 2 == 0) {
		return (tempvs[newSize / 2 - 1] + tempvs[newSize / 2]) / 2;
	}
	else {
		return tempvs[newSize / 2];
	}

	return 0;
}
Object column::mode  (bool with_valu_count, unordered_map<Object, int> value_counts_)
{

	if (!with_valu_count) {
		value_counts_ = value_count();
	}
	unordered_map<Object, int> curr_value_count = value_counts_;

	Object maxval = curr_value_count.begin()->first;
	int maxvalnumber = curr_value_count[maxval];

	for (pair<Object, int> val : curr_value_count) {

		if (val.first.type == Dtype::NA) continue;
		if (val.second > maxvalnumber) {

			maxvalnumber = val.second;
			maxval = val.first;
		}
	}

	return maxval;
}

double column::corr(column& other,
	bool with_means, double mean_x, double mean_y,
	bool with_stds, double std_x, double std_y)
{
	CHECK_NUMBER_METHOUDS((*this));
	CHECK_NUMBER_METHOUDS(other);

	size_t n = size;

	if (with_means == false) {

#pragma omp parallel for reduction(+:mean_x, mean_y)
		for (int i = 0; i < n; i++)
		{
			if (values[i].type != Dtype::NA && other[i].type != Dtype::NA) {
				mean_x += values[i].value_num();
				mean_y += other[i].value_num();
			}
		}
		mean_x /= n;
		mean_y /= n;
	}


	double numerator = 0.0;
#pragma omp parallel for reduction(+:numerator)
	for (int i = 0; i < n; ++i) {

		if (values[i].secType == Dtype::NUMBER && other[i].secType == Dtype::NUMBER) {
			numerator += (values[i].value_num() - mean_x) * (other[i].value_num() - mean_y);
		}
	}

	if (with_stds == false) {

		double std_x_sum = 0.0;
		double std_y_sum = 0.0;

#pragma omp parallel for reduction(+:std_x_sum, std_y_sum)
		for (int i = 0; i < size; i++)
		{
			if (values[i].secType == Dtype::NUMBER && other[i].secType == Dtype::NUMBER) {

				double temp = values[i].value_num() - mean_x;
				std_x_sum += temp * temp;

				temp = other[i].value_num() - mean_y;
				std_y_sum += temp * temp;
			}

		}

		std_x = std::sqrt(std_x_sum / n);
		std_y = std::sqrt(std_y_sum / n);
	}

	double correlation = numerator / (n * std_x * std_y);

	return correlation;
}

double column::quantile  (float q, bool withInSortedVec,  vector < double> sortedValuesIn)
{
	CHECK_NUMBER_METHOUDS((*this));

	vector<double> sortedValus;

	if (withInSortedVec) {
		sortedValus = sortedValuesIn;
	}
	else {
		int const newSize = (int)(values.size() - naidxs().size());

		vector < double > tempvs(newSize);

		int idx = 0;

		for (Object val : values) {
			if (val.type == Dtype::NA) continue;
			tempvs[idx++] = val.value_num();
		}

		sortedValus = tempvs;
		std::sort(sortedValus.begin(), sortedValus.end());
	}
	double index = q * (sortedValus.size() - 1);

	double percentile;

	if (index == (int)index) {
		percentile = sortedValus[(int)index];
	}

	else {
		int lower_index = (int)index;
		int upper_index = lower_index + 1;

		double lower_value = sortedValus[lower_index];
		double upper_value = sortedValus[upper_index];

		percentile = (lower_value + upper_value) / 2;
	}
	return percentile;
}
double column::quantile_2(float q, vector < double>& sortedValuesIn, bool withInSortedVec)
{
	CHECK_NUMBER_METHOUDS((*this));

	vector<double> sortedValus;

	if (withInSortedVec) {
		sortedValus = sortedValuesIn;
	}
	else {
		int const newSize = (int)(values.size() - naidxs().size());

		vector < double > tempvs(newSize);

		int idx = 0;

		for (Object val : values) {
			if (val.type == Dtype::NA) continue;
			tempvs[idx++] = val.value_num();
		}

		sortedValus = tempvs;
		std::sort(sortedValus.begin(), sortedValus.end());
	}
	double index = q * (sortedValus.size() - 1);

	double percentile;

	if (index == (int)index) {
		percentile = sortedValus[(int)index];
	}

	else {
		int lower_index = (int)index;
		int upper_index = lower_index + 1;

		double lower_value = sortedValus[lower_index];
		double upper_value = sortedValus[upper_index];

		percentile = (lower_value + upper_value) / 2;
	}
	return percentile;
}


column column::rolling(int window, StatFun func)
{
	CHECK_NUMBER_METHOUDS((*this));

	column new_col;
	new_col.set_name(name + " Rolling_" + statfun_str.at(func));
	new_col.size = size;
	new_col.values = vector<Object>();
	new_col.values.reserve(size);
	new_col.type = std::max(Dtype::FLOAT, type);
	new_col.secType = Dtype::NUMBER;

	for (size_t e = 0; e < (window - 1); e++) {
		new_col.values.emplace_back(Object());
	}

	size_t end_idx = (size_t)(size - window);

	int gen_i = window - 1;
	vector<Object> temp_vec = values;
	for (size_t i = 0; i < end_idx; i++)
	{
		switch (func)
		{
		case MEAN:
			new_col.values.emplace_back(column::mean(temp_vec, i, i + window));
			break;
		case MEDIAN:
			new_col.values.emplace_back(column::median(temp_vec, i, i + window));

			break;
		case STD:
			new_col.values.emplace_back(column::std(temp_vec, i, i + window));

			break;
		case MODE:
			new_col.values.emplace_back(column::mode(temp_vec, i, i + window));

			break;
		case SUM:
			new_col.values.emplace_back(column::sum(temp_vec, i, i + window));
			break;

		default:
			break;
		}
	}

	return new_col;
}

column column::for_each_interval(int interval_size, StatFun func)
{

	int new_size = std::ceil(size / (double)interval_size);

	column new_col;
	new_col.set_name(name + " - fei " + statfun_str.at(func));
	new_col.size = new_size;
	new_col.values = vector<Object>();
	new_col.values.reserve(new_size);
	new_col.type = std::max(Dtype::FLOAT, type);
	new_col.secType = Dtype::NUMBER;

	int gen_i = 0;
	vector<Object> temp_vec = values;
	int temp_end;
	for (int i = 0; i < size; i += interval_size)
	{
		temp_end = i + interval_size > size ? size : i + interval_size;

		switch (func)
		{
		case MEAN:
			new_col.values.emplace_back(column::mean(temp_vec, i, temp_end));
			break;
		case MEDIAN:
			new_col.values.emplace_back(column::median(temp_vec, i, temp_end));

			break;
		case STD:
			new_col.values.emplace_back(column::std(temp_vec, i, temp_end));

			break;
		case MODE:
			new_col.values.emplace_back(column::mode(temp_vec, i, temp_end));

			break;
		case SUM:
			new_col.values.emplace_back(column::sum(temp_vec, i, temp_end));
			break;

		default:
			break;
		}
	}
	return new_col;
}

column column::diff(int periods, string change_func)
{
	CHECK_NUMBER_METHOUDS((*this));

	/*
		change_func: can be
			1- "normal" -> get normal diff between the periods.
			2- "pct"	-> get the percentage of difference.
	*/

	column new_col;
	new_col.set_name(name + " - " + change_func + " diff");
	new_col.size = size;
	new_col.values = vector<Object>();
	new_col.values.reserve(size);
	new_col.type = std::max(Dtype::FLOAT, type);
	new_col.secType = Dtype::NUMBER;


	for (size_t e = 0; e < (periods); e++) {
		new_col.values.emplace_back(Object());
	}

	double temp_res{};
	double temp_val_new, temp_val_old;

	for (size_t i = periods; i < size; ++i)
	{
		if (values[i].secType != Dtype::NUMBER ||
			values[i - periods].secType != Dtype::NUMBER) {

			new_col.values.emplace_back(Object());
			continue;
		};

		temp_val_new = values[i].value_num();
		temp_val_old = values[i - periods].value_num();

		if (change_func == "normal") {
			temp_res = temp_val_new - temp_val_old;
		}
		else if (change_func == "pct") {
			if (temp_val_old == 0) {
				if (temp_val_new > 0)
					temp_res = 1.0;
				else {
					temp_res = 0;
				}
			}
			else {
				temp_res = (temp_val_new - temp_val_old) / (temp_val_old);
			}

		}

		new_col.values.emplace_back(Object(temp_res));
	}

	return new_col;
}

////////////////

// Outliers Detection

column column::remove_outliers(double m, bool inplace)
{
	CHECK_NUMBER_METHOUDS((*this));

	double mean_ = mean();
	double std_dev = std_2(true, mean_);
	double threshold = m * std_dev;

	if (inplace) {

		values.erase(std::remove_if(
			std::execution::par,
			values.begin(), values.end(),
			[&mean_, &threshold](Object& val) {
				return val.type != Dtype::NA && std::abs((val.value_num() - mean_)) > threshold;
			}),
			values.end());

		this->size = (int)values.size();
		return column();
	}

	vector<Object> new_values;
	new_values.reserve(values.size());
	new_values = values;

	new_values.erase(
		std::remove_if(
			std::execution::par,
			new_values.begin(), new_values.end(),
			[&mean_, &threshold](Object& val) {
				return val.type != Dtype::NA && std::abs((val.value_num() - mean_)) > threshold;
			}),
		new_values.end());

	return column(name, new_values);
}

set<int>& column::remove_get_outliers_idxs(double m)
{
	CHECK_NUMBER_METHOUDS((*this));

	double mean_ = mean();
	double std_dev = std_2(true, mean_);

	vector<int> removed_idxs;
	removed_idxs.reserve(size);

	vector<Object> new_values;
	new_values.reserve(size);

	double threshold = m * std_dev;

	for (size_t i = 0; i < size; i++)
	{
		if (values[i].type == Dtype::NA) continue;

		if (std::abs((values[i].value_num() - mean_)) > threshold) {
			removed_idxs.emplace_back(i);
		}
		else {
			new_values.emplace_back(std::move(values[i]));
		}
	}

	values = std::move(new_values);
	this->size = (int)values.size();

	set<int> final_removed_idxs(removed_idxs.begin(), removed_idxs.end());
	return final_removed_idxs;
}

set<int> column::get_outliers_idxs(double m)
{
	CHECK_NUMBER_METHOUDS((*this));

	double mean_ = mean();
	double std_dev = std_2(true, mean_);

	vector<int> removed_idxs;
	removed_idxs.reserve(size);

	double threshold = m * std_dev;

	for (int i = 0; i < size; i++)
	{
		if (values[i].type == Dtype::NA) continue;
		if (std::abs((values[i].value_num() - mean_)) > threshold) {
			removed_idxs.push_back(i);
		}
	}

	set<int> final_removed_idxs;
	for (int& idx : removed_idxs) final_removed_idxs.insert(idx);

	return final_removed_idxs;
}

////////////////

// String Operations

column column::to_lw()
{
	STRING_FUNCTION((*this), lw);
}
column column::to_up()
{
	STRING_FUNCTION((*this), up);
}
column column::to_title()
{
	STRING_FUNCTION((*this), title);
}
column column::strip()
{
	STRING_FUNCTION((*this), strip);
}

set<int> column::contains(string target)
{
	CHECK_STRING_METHOUDS((*this));

	set<int> idxs;
	for (int i = 0; i < size; i++) 
		if (values[i].contains(target)) idxs.insert(i);
	
	return idxs;
}

column column::str_extract_pattern(const string& pattern)
{
	CHECK_STRING_METHOUDS((*this));

	column new_column = *this;
	new_column.values = String::extract_pattern(new_column.values, pattern);
	return new_column;
}

////////////////

// Conversion

column column::to_num(bool inplace)
{
	if (inplace) {
		if (this->secType == Dtype::NUMBER) return column();

		this->type = Dtype::DOUBLE;
		this->secType = Dtype::NUMBER;

		for (auto& val : values) {
			val = val.to_type(Dtype::DOUBLE);
		}
	}
	else if (this->secType == Dtype::NUMBER) return *this;
	else {		
		column new_col = *this;
		new_col.secType = Dtype::NUMBER;
		new_col.type = Dtype::DOUBLE;

		for (auto& val : new_col.values) {
			val = val.to_type(Dtype::DOUBLE);
		}
		return new_col;
	}

	return column();
}

column column::to_str(bool inplace)
{
	

	if (inplace) {
		if (type == Dtype::STRING) return column();

		this->type = Dtype::STRING;
		this->secType = Dtype::STRING;

		for (auto& val : values) {

			val = val.to_type(Dtype::STRING);
		}
	}
	else if (this->type == Dtype::STRING) return *this;

	else {

		column new_col = *this;
		new_col.type = Dtype::STRING;

		for (auto& val : new_col.values) {

			val = val.to_type(Dtype::STRING);
		}
		return new_col;
	}

	return column();
}

column column::to_date(bool inplace, DateFormat dateformat)
{
	if (this->type != Dtype::STRING) {
		assert(false && "Error from to_date(), we Can Convert only Strings to Date.");
	}

	if (inplace) {
		if (this->type == Dtype::DATE) return column();

		type = Dtype::DATE;
		secType = Dtype::DATE;

		for (auto& val : values) {

			val = val.to_type(Dtype::DATE, dateformat);
		}
	}
	else if (this->type == Dtype::DATE) return *this;
	else {
		column new_col = *this;
		new_col.type = Dtype::DATE;
		new_col.secType = Dtype::DATE;

		for (auto& val : new_col.values) {
			val = val.to_type(Dtype::DATE, dateformat);
		}
		return new_col;
	}

	return column();
}

column column::to_date_attr (bool inplace, Dtype date_attr_type)
{
	if (type == date_attr_type) return *this;
	
	if (!(type == Dtype::DATE || secType == Dtype::DATE_ATTR || secType == Dtype::NUMBER)) {
		cout << "can get year from this column because this column type is '" << type << "' \n";
		assert(false);
	}

	string new_name = name + " - " +
			(date_attr_type == Dtype::DATE_YEAR  ? "year" :
			 date_attr_type == Dtype::DATE_MONTH ? "month" :
			 date_attr_type == Dtype::DATE_DAY   ? "day" : "");

	if (inplace) {
		type = date_attr_type;
		secType = Dtype::DATE_ATTR;
		name = new_name;

		for (Object& val : values) {
			val = val.to_type(date_attr_type);
		}

		return column();
	}

	column new_col;
	new_col.name = new_name;
	new_col.type = date_attr_type;
	new_col.secType = Dtype::DATE_ATTR;
	new_col.size = size;
	new_col.values = values;

	for (Object& val : new_col.values) {
		val = val.to_type(date_attr_type);
	}

	return new_col;
}
column column::to_date_year (bool inplace)
{
	return to_date_attr(inplace, Dtype::DATE_YEAR);
}
column column::to_date_month(bool inplace)
{
	return to_date_attr(inplace, Dtype::DATE_MONTH);
}
column column::to_date_day  (bool inplace)
{
	return to_date_attr(inplace, Dtype::DATE_DAY);
}

column column::to_type(Dtype new_type, bool inplace)
{
	if (inplace) {
		this->type = new_type;
		this->secType = Object::get_sec_type(this->type);
		Object::to_type(this->values, new_type);
	}
	else {
		column new_col = *this;
		new_col.type = new_type;
		new_col.secType = Object::get_sec_type(this->type);
		Object::to_type(new_col.values, new_type);
		return new_col;
	}
	return NULL;
}

set<Object> column::to_set()
{
	return set<Object>(values.begin(), values.end());
}

unordered_set<Object> column::to_uset()
{
	return unordered_set<Object>(values.begin(), values.end());
}

////////////////

// Info About Column's Data

unordered_map<string, Object> column::describe()
{
	unordered_map<string, Object> res;

	vector<string> statistics = {
		"mean", "std", "median",
		"min", "max",
		"25%", "50%","75%",
		"unique", "top", "freq"
	};

	for (string stat : statistics) {
		res[stat] = Object();
	}

	if (Object::get_sec_type(type) == Dtype::NUMBER) {

		double minval = 999999999;
		double maxval = -999999999;

		double values_sum = 0;

#pragma omp parallel for reduction(+:values_sum) reduction(min:minval) reduction(max:maxval) num_threads (8)

		for (int i = 0; i < size; ++i) {

			if (values[i].secType == Dtype::NUMBER) {

				double temp = values[i].value_num();
				values_sum += temp;

				if (temp < minval) {
					minval = temp;
				}
				if (temp > maxval) {
					maxval = temp;
				}
			}
		}

		double const meanRes = values_sum / size;

		res["mean"] = Object(meanRes);
		res["std"] = Object(std_2(true, meanRes));
		res["min"] = Object(minval);
		res["max"] = Object(maxval);

		int const newSize = (int)values.size() - na_count();

		vector < double > tempvs(newSize);
		int idx = 0;
		for (int i = 0; i < size; i++)
		{
			if (values[i].secType == Dtype::NUMBER) {

				tempvs[idx] = values[i].value_num();
				++idx;
			}
		}

		std::sort(tempvs.begin(), tempvs.end());

		res["median"] = Object(median(true, tempvs));

		res["25%"] = Object(column::quantile_2(.25, tempvs, true));
		res["50%"] = Object(column::quantile_2(.5, tempvs, true));
		res["75%"] = Object(column::quantile_2(.75, tempvs, true));
	}
	else {
		auto vount = value_count();

		res["unique"] = Object((double)vount.size());

		Object modevalue = mode(true, vount);

		res["top"] = modevalue;
		res["freq"] = Object(vount[modevalue]);
	}

	return res;
}

vector<Object> column::getDescribeObjectValues(Dtype include) {

	unordered_map<string, Object> des = describe();

	vector<string> property_;

	if (include == Dtype::NA) {
		property_ = {
		"mean",
		"std",
		"median",
		"min",

		"25%",
		"50%",
		"75%",
		"max",

		"unique",
		"top",
		"freq"
		};
	}
	else if (this->secType == Dtype::NUMBER) {
		property_ = {
		"mean",
		"std",
		"median",
		"min",

		"25%",
		"50%",
		"75%",
		"max",
		};
	}
	else {
		property_ = {
		"unique",
		"top",
		"freq"
		};

	}

	vector<Object> vals(property_.size());

	for (int i = 0; i < property_.size(); i++) {
		vals[i] = des[property_[i]];
	}

	return vals;
}

void column::describe_print()
{

	vector<Object> property_;
	if (secType == Dtype::NUMBER) {

		property_.emplace_back("mean");
		property_.emplace_back("std");
		property_.emplace_back("median");
		property_.emplace_back("min");
		property_.emplace_back("25%");
		property_.emplace_back("50%");
		property_.emplace_back("75%");
		property_.emplace_back("max");
	}
	else {
		property_.emplace_back("unique");
		property_.emplace_back("top");
		property_.emplace_back("freq");
	}

	column prop_col("index", property_);

	vector<Object> vals = getDescribeObjectValues(type);

	column val_col(name, vals);

	Table tabel({ prop_col ,val_col });
	tabel.print();
}

unordered_map<Object, int  > column::value_count()
{
	unordered_map<Object, int> results;
	for (const auto& val : values) {
		++results[val];
	}
	return results;
}
unordered_map<Object, float> column::value_count_pct()
{
	unordered_map<Object, float> results;
	for (const auto& val : values) {
		++results[val];
	}

	auto size_ = values.size();
	for (auto& valcounter : results) {
		valcounter.second /= size_;
	}
	return results;
}

void column::value_count_print    () {

	unordered_map<Object, int> curr_value_count = value_count();

	vector<Object> index_vec(curr_value_count.size());
	vector<Object> value_vec(curr_value_count.size());

	int idx = 0;

	for (const auto& val : curr_value_count) {
		index_vec[idx] = Object(val.first);
		value_vec[idx++] = Object(val.second, Dtype::INT32);
	}
	column indexcol("index", Tools::gen_seq_obj(0, (int)curr_value_count.size()));

	column namecol(name, index_vec);
	column valuecol("Count", value_vec);

	Table table({ indexcol, namecol, valuecol });
	table.print();
}
void column::value_count_pct_print() {

	auto curr_value_count = value_count_pct();

	vector<Object> index_vec(curr_value_count.size());
	vector<Object> value_vec(curr_value_count.size());

	int idx = 0;

	for (const auto& val : curr_value_count) {
		index_vec[idx] = Object(val.first);
		value_vec[idx++] = Object(val.second, Dtype::FLOAT);
	}

	column indexcol("index", Tools::gen_seq_obj(0, (int)curr_value_count.size()));

	column namecol(name, index_vec);
	column valuecol("pct", value_vec);

	Table table({ indexcol, namecol, valuecol });
	table.print();
}

unordered_set<Object> column::unique() {

	return to_uset();
}

int column::unique_count()
{
	return (int)(to_uset().size());
}
int column::na_count    ()
{
	int na_counter = 0;
	for (int i = 0; i < size; i++)
	{
		if (values[i].type == Dtype::NA) {
			++na_counter;
		}
	}
	return na_counter;
}

////////////////

// Alteration

void   column::drop  (int idx)
{
	values.erase(values.begin() + idx);
	--size;
}
column column::drop  (set<int> indexes, bool inplace)
{
	unordered_set<int> in_indexes;

	for (int i : indexes)
		in_indexes.insert(i);

	int new_size = static_cast<int>(values.size() - indexes.size());

	auto end = in_indexes.end();

	column new_col;

	new_col.values = vector<Object>();
	new_col.values.reserve(new_size);


	int i = 0;
	for (Object& val : values) {

		if (in_indexes.find(i++) == end) {
			new_col.values.push_back(val);
		}
	}

	if (inplace) {
		values = std::move(new_col.values);
		size = (int)values.size();
		new_col = column();
		return new_col;
	}

	new_col.set_name(name);
	new_col.size = new_size;
	new_col.type = this->type;
	new_col.secType = this->secType;
	new_col.mixed_type = this->mixed_type;

	return new_col;
}
column column::dropna(bool in_place)
{
	if (in_place) {
		*this = drop(naidxs());
	}
	else {
		column new_cols = *this;
		return new_cols.drop(new_cols.naidxs());
	}
	return column();
}

column column::concat(column othercol)
{
	assert(name    == othercol.name    && "Both Col must have same name");
	assert(secType == othercol.secType && "Both Col must have same type");

	column newcol = *this;
	newcol.type = std::max(type, othercol.type);
	newcol.secType = std::max(secType, othercol.secType);
	newcol.mixed_type = true;

	for (Object val : othercol.values) {
		newcol.appendPushBack(val);
	}
	return newcol;
}

column column::repeat(int times)
{
	if (times == 0) return *this;

	int ac_size = len();
	int new_size = ac_size * times;

	vector<Object> new_values(new_size);

	for (int i = 0; i < new_size; i++)
	{
		new_values[i] = values[i % ac_size];
	}

	return column(name, new_values);
}
column column::repeat_for_each(int times)
{
	if (times == 0 || times == 1) return *this;

	int ac_size = len();
	int new_size = ac_size * times;
	vector<Object> new_values(new_size);

	int idx = 0;
	int temp_times = times;
	for (int i = 0; i < (int)(new_size / times); i++)
	{
		temp_times = times;
		while (temp_times--)
		{
			new_values[idx++] = values[i];
		}
	}
	return column(name, new_values);

}

column column::sample(int size_, int seed, bool with_replacement)
{
	return (*this)[Tools::rand_vec_nums(size_, size - 1, 0, seed, with_replacement)];
}
column column::shuffle(int seed)
{
	return (*this)[Tools::rand_vec_nums(size, size - 1, 0, seed, false)];
}

////////////////

// Sorting

vector<int> column::map_sort(
	vector<Object>& arr,
	bool reverse,
	SortAlgo algo)
{

	switch (algo)
	{
	case SortAlgo::heap:	  return Sort::heap(arr, reverse);
	case SortAlgo::insertion: return Sort::insertion(arr, reverse);
	case SortAlgo::bubble:    return Sort::bubble(arr, reverse);
	case SortAlgo::selection: return Sort::selection(arr, reverse);
	case SortAlgo::double_selection: return Sort::double_selection(arr, reverse);
	case SortAlgo::merge:	  return Sort::merge_sort(arr, reverse);
	case SortAlgo::quick:	  return Sort::quick_sort(arr, reverse);

	case SortAlgo::merge_and_insertion:
		return Sort::merge_insertion_sort(arr, reverse);

	default:
		break;
	}

	vector<int> temp;

	return temp;
}


vector<int> column::sortAndGetIndexs(bool revers, SortAlgo algo)
{
	return map_sort(values, revers, algo);
}

column column::sortAndGetCol(bool revers, SortAlgo algo)
{
	vector<Object> tempValues = values;

	map_sort(tempValues, revers, algo);

	column new_col = *this;
	new_col.values = tempValues;

	return new_col;
}

void column::sort(bool revers, SortAlgo algo)
{
	map_sort(values, revers, algo);
}

////////////////

// Algorithmic Methods

Object column::kth_element(int k)
{
	vector<Object> temp_values = values;
	return kth_element(temp_values, 0, size - 1, k);
}

Object column::kth_largest_element (int k)
{
	vector<Object> temp_values = values;
	return kth_element(temp_values, 0, size - 1, size - k + 1);
}
Object column::kth_smallest_element(int k)
{
	return kth_element(k);
}

column column::kth_largest_elements (int k, bool reverse)
{
	vector<Object> temp_values = values;
	kth_element(temp_values, 0, size - 1, size - k + 1);

	string new_name = std::to_string(k) + "_largest_items";

	vector<Object> new_vec(temp_values.begin() + (size - k), temp_values.end());

	//Sort::insertion(new_vec, reverse);

	return column(new_name, new_vec);
}
column column::kth_smallest_elements(int k, bool reverse)
{
	vector<Object> temp_values = values;
	kth_element(temp_values, 0, size - 1, k);

	string new_name = std::to_string(k) + "_smallest_items";

	vector<Object> new_vec(temp_values.begin(), temp_values.begin() + k);

	//Sort::insertion(new_vec, reverse);

	return column(new_name, new_vec);
}

Object column::kth_top_cat (uint32_t k)
{

	unordered_map<Object, uint32_t> value_counter;

	for (const auto& val : values) {
		value_counter[val]++;
	}

	using FreqPair = std::pair<uint32_t, Object>;
	std::priority_queue<FreqPair, vector<FreqPair>, std::greater<>> min_heap;

	for (const auto& entry : value_counter) {
		min_heap.push({ entry.second, entry.first });
		if (min_heap.size() > k) {
			min_heap.pop();
		}
	}

	if (!min_heap.empty()) {
		return min_heap.top().second;
	}

	return Object();
}
Object column::kth_least_cat(uint32_t k)
{
	unordered_map<Object, uint32_t> value_counter;

	for (const auto& val : values) {
		value_counter[val]++;
	}

	using FreqPair = std::pair<uint32_t, Object>;
	std::priority_queue<FreqPair> max_heap;

	for (const auto& entry : value_counter) {
		max_heap.push({ entry.second, entry.first });

		if (max_heap.size() > k) {
			max_heap.pop();
		}
	}

	if (!max_heap.empty()) {
		return max_heap.top().second;
	}

	return Object();
}

column column::kth_top_cats (uint32_t k)
{
	auto value_counts_res = value_count();

	if (k > value_counts_res.size()) {
		k = (uint32_t)value_counts_res.size();
	}

	vector<std::pair<Object, int>> vec(value_counts_res.begin(), value_counts_res.end());

	std::sort(vec.begin(), vec.end(),
		[](
			const std::pair<Object, int>& a,
			const std::pair<Object, int>& b) {
				return a.second > b.second; // Sort in descending order based on value
		});

	vector<Object> result_values;
	result_values.reserve(k);
	for (size_t i = 0; i < k; i++)
	{
		result_values.emplace_back(vec[i].first);
	}

	return column(name, result_values, mixed_type);
}
column column::kth_lest_cats(uint32_t k)
{
	auto value_counts_res = value_count();

	if (k > value_counts_res.size()) {
		k = (uint32_t)value_counts_res.size();
	}

	vector<std::pair<Object, int>> vec(value_counts_res.begin(), value_counts_res.end());

	std::sort(vec.begin(), vec.end(),
		[](
			const std::pair<Object, int>& a,
			const std::pair<Object, int>& b) {
				return a.second < b.second; // Sort in descending order based on value
		});

	vector<Object> result_values;
	result_values.reserve(k);
	for (size_t i = 0; i < k; i++)
	{
		result_values.emplace_back(vec[i].first);
	}

	return column(name, result_values, mixed_type);
}

SubArrayBoundris column::maxSubSeq()
{
	return maxSubArraySub(values, 0, size - 1);
}
SubArrayBoundris column::minSubSeq()
{
	return minSubArraySub(values, 0, size - 1);
}

////////////////

// Duplication

set<int> column::duplicated()
{
	set<int> duplicated_indices;
	if (values.empty()) {
		return duplicated_indices;
	}

	unordered_set<Object> unique_values;
	size_t value_size = values.size();
	unique_values.reserve(value_size);

	for (size_t i = 0; i < value_size; ++i) {

		const Object& current = values[i];
		if (current.type == Dtype::NA) {
			continue;
		}
		// Try to insert and check if it was successful
		auto [_, was_inserted] = unique_values.emplace(current);

		if (!was_inserted) {
			duplicated_indices.emplace(i);
		}
	}
	return duplicated_indices;
}

column column::drop_duplicated(bool inplace)
{
	set<int> dub_idxs = this->duplicated();

	if (inplace) {
		values.erase(
			std::remove_if(values.begin(), values.end(),
				[&dub_idxs, idx = 0](const auto&) mutable {
					return dub_idxs.count(idx++);
				}),
			values.end()
		);
		size -= (int)dub_idxs.size();
	}
	else {
		column new_col = *this;
		new_col.drop_duplicated(inplace = true);
		return new_col;
	}
	return column();
}

////////////////

// Handling Memory

size_t column::get_mem_size()
{
	size_t not_fully_used_size = (this->values.capacity() - this->size) * 8;
	size_t fully_used_size = Object::get_mem_size(values);
	size_t mem_size = not_fully_used_size + fully_used_size;
	return mem_size;
}

void column::optimiz_mem(bool with_mixed_type)
{
	type = Object::optimize_mem(this->values, with_mixed_type);
	secType = Object::get_sec_type(type);
	mixed_type = with_mixed_type;
}

////////////////

// Apply

column column::apply(std::function<Object(const Object&)> func)
{
	column new_col = *this;

	for (auto& value : new_col.values) {
		value = func(value); // Apply the function to each value
	}
	return new_col; // Return a new column with the results}
}

void column::papply(const std::function<Object(const Object&)>& func)
{
	omp_set_num_threads(omp_get_max_threads());

#pragma omp parallel for
	for (size_t i = 0; i < size; ++i) {
		values[i] = func(values[i]); // Apply the function to each value
	}
}

////////////////

// Filtering

column column::filter(vector<set<int>>& indexes)
{
	if (indexes.empty()) return column();

	set<int> result = indexes[0];

	for (size_t i = 1; i < indexes.size(); i++)
	{
		set<int> curr_set = indexes[i];
		set<int> intersections;

		std::set_intersection(
			result.begin(), result.end(),
			curr_set.begin(), curr_set.end(),
			std::inserter(
				intersections,
				intersections.begin()
			)
		);
		result = intersections;

		if (result.empty()) break;
	}
	return (*this)[result];
}
column column::filter(set<int>& indexes) { return (*this)[indexes]; }

set<int> column::filterByConditionIdx(std::function<bool(const Object&)> condition)
{
	set<int> matched_idx;
	int i = 0;
	for (const auto& val : values) {
		if (condition(val)) matched_idx.insert(i);
		++i;
	}
	return matched_idx;
}
column   column::filterByCondition   (std::function<bool(const Object&)> condition)
{
	return (*this)[filterByConditionIdx(condition)];
}

set<int> column::filterByConditionIdx(std::function<bool(const size_t idx, const Object&)>condition) {

	set<int> matched_idx;
	int i = 0;
	for (const auto& val : values) {
		if (condition(i, val)) {
			matched_idx.insert(i);
		}
		++i;
	}
	return matched_idx;
}
column   column::filterByCondition   (std::function<bool(const size_t idx, const Object&)>condition) {
	return (*this)[filterByConditionIdx(condition)];
}

set<int> column::isin(std::initializer_list<Object> invalues)
{
	return isin(vector<Object>(invalues.begin(), invalues.end()));
}

////////////////

// utilities

void column::calc_max_str_val_len()
{
	maxLenStr = (int)name.length();
	int temp_len;

	for (const Object& val : values) {
		temp_len = val.len();
		if (temp_len > maxLenStr)
			maxLenStr = temp_len;
	}
}

////////////////

//Random Generating Values

column column::rand_nums(int size, int max, int min, unsigned int seed)
{
	return column("NA", Object::rand_nums(size, max, min, seed), false);
}

////////////////

// Private Methods

vector<Object> column::calc(char op, Object val) {

	vector<Object> newvalues;
	newvalues.reserve(size);

	for (int i = 0; i < size; i++)
	{
		if (values[i].type == Dtype::NA) {
			newvalues.emplace_back(Object());
			continue;
		};

		switch (op)
		{
		case '+': newvalues.emplace_back(values[i] + val); break;
		case '-': newvalues.emplace_back(values[i] - val); break;
		case '*': newvalues.emplace_back(values[i] * val); break;
		case '/': newvalues.emplace_back(values[i] / val); break;
		case '%': newvalues.emplace_back(values[i] % val); break;
		}
	}
	return newvalues;
}


Object column::kth_element(
	vector<Object>& arr,
	int start,
	int end, int k)
{
	if (start == end) return arr[start];

	int q = Tools::randimized_partition(arr, start, end);
	int j = q - start + 1;

	if (k == j) return arr[q];

	else if (k < j) {

		return kth_element(arr, start, q - 1, k);
	}
	else {

		return kth_element(arr, q + 1, end, k - j);
	}

	return Object();
}

SubArrayBoundris column::middelMaxSub  (vector<Object> arr, int low, int mid, int high){
	int best_lidx = mid;
	int best_ridx = mid;

	long double best_lsum = (long double)arr[mid].value_num();
	long double best_rsum = best_lsum;

	long int temp = 0;

	for (int i = mid; i >= low; --i)
	{
		temp += (long int)arr[i].value_num();

		if (temp > best_lsum) {
			best_lsum = temp;
			best_lidx = i;
		}
	}

	temp = 0;
	for (int i = mid; i <= high; ++i)
	{
		temp += (long int)arr[i].value_num();

		if (temp > best_rsum) {
			best_rsum = temp;
			best_ridx = i;
		}
	}

	return { best_lidx, best_ridx, best_lsum + best_rsum - arr[mid].value_num() };
}
SubArrayBoundris column::maxSubArraySub(vector<Object> arr, int low, int high)
{
	if (low == high) return { low, high, arr[low].value_num() };

	int mid = (low + high) / 2;

	SubArrayBoundris leftSubSum = maxSubArraySub(arr, low, mid);
	SubArrayBoundris rightSubSum = maxSubArraySub(arr, mid + 1, high);
	SubArrayBoundris midSubsum = middelMaxSub(arr, low, mid, high);

	if (
		leftSubSum.sum >= rightSubSum.sum &&
		leftSubSum.sum >= midSubsum.sum)
		return leftSubSum;

	else if (
		leftSubSum.sum <= rightSubSum.sum &&
		rightSubSum.sum >= midSubsum.sum)
		return rightSubSum;

	return midSubsum;
}

SubArrayBoundris column::middelMinSub  (vector<Object> arr, int low, int mid, int high)
{
	int best_lidx = mid;
	int best_ridx = mid;

	long double best_lsum = (long double)arr[mid].value_num();
	long double best_rsum = best_lsum;

	long int temp = 0;

	for (int i = mid; i >= low; --i)
	{
		temp += (long int)arr[i].value_num();

		if (temp < best_lsum) {
			best_lsum = temp;
			best_lidx = i;
		}
	}

	temp = 0;
	for (int i = mid; i <= high; ++i)
	{
		temp += (long int)arr[i].value_num();

		if (temp < best_rsum) {
			best_rsum = temp;
			best_ridx = i;
		}
	}

	return { best_lidx, best_ridx, best_lsum + best_rsum - arr[mid].value_num() };
}
SubArrayBoundris column::minSubArraySub(vector<Object> arr, int low, int high)
{
	if (low == high) return { low, high, arr[low].value_num() };

	int mid = (low + high) / 2;

	SubArrayBoundris leftSubSum = minSubArraySub(arr, low, mid);
	SubArrayBoundris rightSubSum = minSubArraySub(arr, mid + 1, high);
	SubArrayBoundris midSubsum = middelMinSub(arr, low, mid, high);

	if (
		leftSubSum.sum <= rightSubSum.sum &&
		leftSubSum.sum <= midSubsum.sum)
		return leftSubSum;

	else if (
		leftSubSum.sum >= rightSubSum.sum &&
		rightSubSum.sum <= midSubsum.sum)
		return rightSubSum;

	return midSubsum;
}

//////////////////////////////////////////////////

// Comparison

set<int> column::operator < (Object value) { return COLUMN_COMPARE_OP(this, value, < ); }
set<int> column::operator > (Object value) { return COLUMN_COMPARE_OP(this, value, > ); }
set<int> column::operator <=(Object value) { return COLUMN_COMPARE_OP(this, value, <=); }
set<int> column::operator >=(Object value) { return COLUMN_COMPARE_OP(this, value, >=); }
set<int> column::operator ==(Object value) { return COLUMN_COMPARE_OP(this, value, ==); }

vector<bool> column::operator ==(column other)
{
	assert((secType == other.secType && other.type != Dtype::NA) && "This not valid comparison!");

	vector<bool> reults(size);
	for (int i = 0; i < size; i++)
	{
		reults[i] = values[i] == other.values[i];
	}
	return reults;
}

//////////////////////////////////////////////////

// Arithmetic Operations

column column::operator + (Object other) { return COLUMN_ARITHMETICS_SCALAR_OP(this, other, +); }
column column::operator - (Object other) { return COLUMN_ARITHMETICS_SCALAR_OP(this, other, -); }
column column::operator * (Object other) { return COLUMN_ARITHMETICS_SCALAR_OP(this, other, *); }
column column::operator / (Object other) { return COLUMN_ARITHMETICS_SCALAR_OP(this, other, /); }
column column::operator % (Object other) { return COLUMN_ARITHMETICS_SCALAR_OP(this, other, %); }

column column::operator + (column other) { return COLUMN_ARITHMETICS_OP_COLUMN(this, &other, +); }
column column::operator - (column other) { return COLUMN_ARITHMETICS_OP_COLUMN(this, &other, -); }
column column::operator * (column other) { return COLUMN_ARITHMETICS_OP_COLUMN(this, &other, *); }
column column::operator / (column other) { return COLUMN_ARITHMETICS_OP_COLUMN(this, &other, /); }
column column::operator % (column other) { return COLUMN_ARITHMETICS_OP_COLUMN(this, &other, %); }

//2746