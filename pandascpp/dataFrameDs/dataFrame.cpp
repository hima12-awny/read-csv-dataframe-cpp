#include "dataFrame.h"

// Constructors
dataFrame::dataFrame()
{
	size = 0;
	columns = {};
	data = {};
}
dataFrame::dataFrame(string path, bool mixed_type) {

	auto startTime = std::chrono::high_resolution_clock::now();
	const int NUMBER_OF_LINES = count_file_lines(path);
	this->path = path;
	int idx = 0;

	fstream file{ path };
	assert(file.is_open() && "file can't open");

	cout << "Load data...\n";
	string line;
	getline(file, line);
	istringstream row(line);
	string val;

	// reading first row always as columns names.
	for (val; getline(row, val, ',');) {
		column tempCol(NUMBER_OF_LINES, mixed_type);
		tempCol.name = val;
		data[val] = tempCol;
		columns.push_back(val);
	}

	// and other rows are values. 
	for (line; getline(file, line);)
	{
		istringstream row(line);
		for (val; getline(row, val, ',');) {

			if (val.length() == 0) { data[columns[idx]].append(Object(   )); }
			else				   { data[columns[idx]].append(Object(val)); }

			idx++;
		}
		idx = 0;
	}

	file.close();

	size = data.begin()->second.len();

	std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>
		(std::chrono::high_resolution_clock::now() - startTime); // Calculate the duration in microseconds

	cout << "Load data done.\n";
	cout << "Time to load data taken: " << duration.count() / 1000.0 << " sec" << std::endl; // Print the time taken
}
dataFrame::dataFrame(vector<column> cols)
{
	if (is_same_length(cols) == false) { make_cols_same_length(cols); }
	uint16_t col_counter = 0;
	for (column& col : cols) {
		if (col.name == "NA") { col.name = "col_" + to_string(col_counter++); }
		data[col.name] = col;
	}

	columns = vector<string>(cols.size());
	int idx = 0;
	for (column col : cols) {

		data[col.name] = col;
		columns[idx++] = col.name;
	}
	size = cols[0].size;
}
dataFrame::dataFrame(unordered_map<string, column> input_data)
{
	size = input_data.begin()->second.size;

	for (auto col : input_data) {
		columns.emplace_back(col.first);
		col.second.set_name(col.first);
		data.emplace(col.first, col.second);
	}
}
dataFrame::dataFrame(std::initializer_list<std::pair<const std::string, column>> init)
{
	for (auto pair : init) {
		columns.push_back(pair.first);
		pair.second.set_name(pair.first);
		data.emplace(pair.first, pair.second);
	}
	size = data.begin()->second.len();	
}
dataFrame::dataFrame(unordered_map<string, column> input_data, vector<string> columns)
{
	this->columns = columns;

	size = input_data.begin()->second.size;

	for (auto col : columns) {
		input_data[col].set_name(col);
		data.emplace(col, input_data[col]);
	}
}
dataFrame::dataFrame(unordered_map<string, vector<Object>> input_data)
{
	size = input_data.begin()->second.size();
	for (auto col : input_data) {
		columns.emplace_back(col.first);
		data.emplace(col.first, column(col.first, col.second));
	}
}
dataFrame::dataFrame(unordered_map<string, vector<Object>> input_data, vector<string> columns)
{
	this->columns = columns;

	size = input_data.begin()->second.size();
	for (auto col : columns) {
		data.emplace(col, column(col, input_data[col]));
	}
}

dataFrame& dataFrame::operator=(const dataFrame& other) {
	if (this == &other)
		return *this;

	this->size = other.size;
	this->columns = other.columns;
	this->data = other.data;

	return *this;
}

/////////////////////////////////////
// Data Accessing
column& dataFrame::operator[](string col) {
	assert(is_valid_column(col) && "this Column name is not found");
	return data[col];
}
const column& dataFrame::operator[](string col) const {
	assert(is_valid_column(col) && "Column name is not found");
	return data.at(col); // Use .at() to ensure bounds checking and const correctness}
}

dataFrame  dataFrame::operator[](vector<string> colsNames) {

	for (string col : colsNames) {
		if (!is_valid_column(col)) {
			std::cout << "colName :" << col; assert(false && "Column name is not found");
		}
	}
	vector<column> selectedCols(colsNames.size());

	for (int i = 0; i < colsNames.size(); i++)
	{
		selectedCols[i] = data[colsNames[i]];
	}
	return dataFrame(selectedCols);
}
dataFrame  dataFrame::operator[](initializer_list<string> colsNames) { 
	return (*this)[vector<string>(colsNames.begin(), colsNames.end())]; 
};

#define FILTER_DF_BY_INDEXES(columns, indexes) \
    std::vector<column> filterdCols((columns).size()); \
    int idx = 0; \
    for (const std::string& colname : (columns)) { \
        filterdCols[idx++] = data[colname][indexes]; \
    } \
    return dataFrame(filterdCols);

dataFrame dataFrame::operator[](   set<int> indexes) { FILTER_DF_BY_INDEXES(columns, indexes); }
dataFrame dataFrame::operator[](vector<int> indexes) { FILTER_DF_BY_INDEXES(columns, indexes); }
dataFrame dataFrame::operator[](initializer_list<int> indexes) {
	FILTER_DF_BY_INDEXES(columns, vector<int>(indexes.begin(), indexes.end())); 
}

/////////////////////////////////////
// Information About the Dataframe

vector<int> dataFrame::shape()
{
	return { data.begin()->second.size, (int)data.size() };
}

unordered_map<Dtype, uint16_t> dataFrame::dtype_count()
{

	unordered_map<Dtype, uint16_t> counter;
	for (const auto& col : data) {
		++counter[col.second.type];
	}
	return counter;
}


void dataFrame::info() {

	omp_set_num_threads(1);

	vector<Object> property_;
	property_.emplace_back("Range Index");
	property_.emplace_back("Data Columns");
	property_.emplace_back("DTypes");
	property_.emplace_back("Memory Usage");

	column indext1(
		"Property",
		property_
	);

	int numberofcols = (int)columns.size();

	auto dtype_counter = this->dtype_count();

	string types = "";
	for (auto dt : dtype_counter) {
		types += Object::type_str(dt.first) + "(" + to_string(dt.second) + "), ";
	}

	types = types.substr(0, types.length() - 2);

	vector<Object> values;
	values.emplace_back(to_string(data[columns[0]].size) + " entries");
	values.emplace_back(to_string(columns.size()) + " columns");
	values.emplace_back(types);

	//values.emplace_back(to_string((int)(filesize(path) / 1024.0)) + " KB");

	values.emplace_back(to_string((this->get_mem_size() / 1024.0)) + " KB");

	column valt1("Value", values);

	Table tab1({ indext1 ,valt1 });
	tab1.print();


	vector<Object> num_of_nonna_for_cols(numberofcols);
	vector<Object> num_of_na_for_cols(numberofcols);
	vector<Object> dtypevals(numberofcols);
	int numofna;

	column tempCol;

	for (int i = 0; i < numberofcols; i++)
	{
		tempCol = data[columns[i]];

		numofna = (int)tempCol.naidxs().size();

		num_of_nonna_for_cols[i] = Object(tempCol.size - numofna);
		num_of_na_for_cols[i] = Object(numofna);
		dtypevals[i] = Object(tempCol.type_str());
	}

	column indext2("Index", Tools::gen_seq_obj(0, numberofcols));

	column colname("Column", Object::from_vector_string(columns));

	column nonna("Non-Null Count", num_of_nonna_for_cols);
	column na("Null Count", num_of_na_for_cols);
	column dtypscol("DType", dtypevals);

	Table tab2({ indext2 ,colname, nonna, na, dtypscol });
	tab2.print();

	omp_set_num_threads(omp_get_max_threads());

}

dataFrame dataFrame::describe(Dtype include) {

	omp_set_num_threads(2);

	vector<string> includedCols;

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
		includedCols = columns;
	}

	else if (include == Dtype::NUMBER) {
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

		includedCols = this->get_num_cols();
	}
	else if (include == Dtype::STRING) {

		property_ = {
		"unique",
		"top",
		"freq"
		};

		includedCols = get_str_cols();
	}
	else {
		assert(false && "INVALID INCLUDE TYPE");
	}
	vector<column> cols(includedCols.size() + 1);

	cols[0] = column("index", Object::from_vector_string(property_));

	int n = (int)includedCols.size() + 1;
#pragma omp parallel
	{
		// Use thread-private variables to store thread-specific indexes
#pragma omp for
		for (int idx = 1; idx < n; ++idx) {
			const std::string& colname = includedCols[idx - 1];
			cols[idx] = column(colname, data.at(colname).getDescribeObjectValues(include));
		}
	}
	omp_set_num_threads(omp_get_max_threads());
	return dataFrame(cols);
}

dataFrame dataFrame::corr()
{

	struct CorrStruct {
		string col1;
		string col2;
		double corr;
	};

	unordered_map<string, double> means;
	unordered_map<string, double> stds;


	auto num_cols = this->get_num_cols();
	size_t num_cols_size = num_cols.size();


	vector<Object> cols_names = Object::from_vector_string(num_cols);
	unordered_map<string, vector<Object>> cols_corrs;
	cols_corrs["ColsName"] = cols_names;


	vector<CorrStruct> corrs;
	corrs.reserve(num_cols_size * (num_cols_size + 1) / 2.0);

	for (size_t i = 0; i < num_cols_size; i++)
	{
		for (size_t j = i; j < num_cols_size; j++)
		{

			auto col1 = num_cols[i];
			auto col2 = num_cols[j];
			double corr_;

			if (col1 == col2) {
				corr_ = 1.0;
			}
			else {

				if (means.find(col1) == means.end()) {
					means[col1] = data[col1].mean();
				}

				if (means.find(col2) == means.end()) {
					means[col2] = data[col2].mean();
				}

				if (stds.find(col1) == stds.end()) {
					stds[col1] = data[col1].std_2(true, means[col1]);
				}
				if (stds.find(col2) == stds.end()) {
					stds[col2] = data[col2].std_2(true, means[col2]);
				}

				corr_ = data[col1].corr(
					data[col2],
					true, means[col1], means[col2],
					true, stds[col1], stds[col2]
				);
			}

			if (cols_corrs.find(col1) == cols_corrs.end()) {
				cols_corrs[col1] = vector<Object>();
				cols_corrs[col1].reserve(num_cols_size);
			}

			if (cols_corrs.find(col2) == cols_corrs.end()) {
				cols_corrs[col2] = vector<Object>();
				cols_corrs[col2].reserve(num_cols_size);
			}

			cols_corrs[col1].emplace_back(corr_);

			if (col1 != col2) {
				cols_corrs[col2].emplace_back(corr_);
			}

			corrs.push_back({ col1, col2, corr_ });
		}
	}


	vector<column> cols_;
	cols_.reserve(num_cols_size + 1);

	cols_.emplace_back(column("ColsName", cols_names));

	for (string colName : num_cols) {
		cols_.emplace_back(column(colName, cols_corrs[colName]));
	}


	dataFrame corr_df(cols_);

	return corr_df;
}

////////////////////////////////////
// Getters/Setters

vector<string> dataFrame::get_num_cols () { return get_sectype_cols(Dtype::NUMBER); }
vector<string> dataFrame::get_str_cols () { return get_sectype_cols(Dtype::STRING); }
vector<string> dataFrame::get_date_cols() { return get_sectype_cols(Dtype::DATE  ); }

vector<string> dataFrame::get_sectype_cols(Dtype cols_sectype)
{
	vector<string> num_cols;
	for (string col_name : columns) {
		if (data[col_name].secType == cols_sectype) {
			num_cols.push_back(col_name);
		}
	}
	return num_cols;
}

dataFrame dataFrame::add_col(column col, bool inplace) {

	if (inplace) {
		data[col.name] = col;
		columns.push_back(col.name);
		return dataFrame();
	}
	dataFrame new_df = *this;
	new_df.data[col.name] = col;
	new_df.columns.push_back(col.name);
	return new_df;
}

size_t dataFrame::get_mem_size()
{
	size_t total_mem_size = 0;
	for (auto col : data) {
		total_mem_size += col.second.get_mem_size();
	}

	return total_mem_size;
}

dataFrame dataFrame::range(int from, int to)
{
	bool valid_range = from >= 0 && from <= size && to >= from && to <= size;
	if (!valid_range) {
		cout << "Invalid range: from=" << from << ", to=" << to << " and your size=" << size << endl; assert(0);
		return dataFrame();
	}
	return (*this)[Tools::gen_seq_int(from, to)];
}
dataFrame dataFrame::head(int n) {

	return range(0, n);
}
dataFrame dataFrame::tail(int n) {
	return  this->range(this->size - n, this->size);
}

unordered_map<string, Object> dataFrame::iloc(int idx) {

	unordered_map<string, Object> recored;
	for (auto col : columns) {
		recored[col] = data[col][idx];
	}
	return recored;
}
Object dataFrame::   iloc(int r, int c) {

	return data[columns[c]][r];
}
dataFrame dataFrame::iloc(vector<int> vrange, vector<int> hrange)
{
	if ((vrange.size() != 2 || hrange.size() != 2) || 
		(vrange[0] >= vrange[1]) || 
		(hrange[0] >= hrange[1])) assert(false && "Invalid Range");

	vector<string> newColNames;
	for (int i = hrange[0]; i < hrange[1]; i++) { newColNames.push_back(columns[i]); };

	vector<column> newColValue;
	for (string colname : newColNames) {
		newColValue.push_back(data[colname].range(vrange[0], vrange[1]));
	}
	return dataFrame(newColValue);
}

dataFrame dataFrame::copy() { return *this; }

dataFrame dataFrame::sample(int size_, int seed, bool with_replacement)
{
	return (*this)[Tools::rand_vec_nums(size_, this->size - 1, 0, seed, with_replacement)];
}

dataFrame dataFrame::shuffle(int seed)
{
	return (*this)[Tools::rand_vec_nums(size, this->size - 1, 0, seed, false)];
}

/////////////////////////////////////
// print  //////////////////////////

void dataFrame::p(bool printGenIndex) {

	int idx = 0,
		newsize = (int)(printGenIndex ? columns.size() + 1 : columns.size());

	vector<column> tempCols(newsize);

	if (printGenIndex) { tempCols[idx++] = column("Index", Tools::gen_seq_obj(0, size)); }

	for (string col : columns) { tempCols[idx++] = data[col]; }

	Table table(tempCols);
	table.print();
}
void dataFrame::phead(int limit) { this->head(limit).p(); }
void dataFrame::ptail(int limit) { this->tail(limit).p(); }
void dataFrame::cols_p() {

	cout << "\n["; for (auto name : columns) cout << name << ", "; cout << "]\n";
}

/////////////////////////////////////
// Dealing with bad data
inline void dataFrame::drop(int idx)
{
	for (string colname : columns) {

		data[colname].values.erase(data[colname].values.begin() + idx);
		--data[colname].size;
	}
	--size;
}
inline void dataFrame::drop(set<int>& idxs)
{
	// Get the number of columns
	int num_columns = (int)columns.size();

	// Parallelize the loop over columns using OpenMP
	#pragma omp parallel for
	for (int i = 0; i < num_columns; ++i) {
		string colname = columns[i];
		data[colname].drop(idxs, true);
	}

	// Update the size after dropping rows
	#pragma omp atomic
	this->size -= (int)idxs.size();
}

set<int>  dataFrame::naidxs (vector<string> colnames)
{
	colnames = colnames.size() == 0 ? columns : colnames;

	vector<set<int>> all_na_idexs(colnames.size());

	#pragma omp parallel for
	for (size_t i = 0; i < colnames.size(); i++)
	{
		all_na_idexs[i] = data[colnames[i]].naidxs();
	}

	set<int> na_idexs;
	for (const auto& s : all_na_idexs) {

		std::set_union(
			na_idexs.begin(), na_idexs.end(),
			s.begin(), s.end(),
			std::inserter(na_idexs, na_idexs.end())
		);
	}
	return na_idexs;
}
dataFrame dataFrame::get_na (vector<string> colnames) { return (*this)[naidxs()]; }
void      dataFrame::drop_na(vector<string> colnames)
{
	auto na_idxs = naidxs(colnames);
	drop(na_idxs);
}

set<int>  dataFrame::outliers_idx (vector<string> cols, double m)
{
	vector<string> num_cols = cols.size() == 0 ? get_num_cols() : cols;

	vector<set<int>> removed_idxs(num_cols.size());

#pragma omp parallel for
	for (size_t i = 0; i < num_cols.size(); i++)
	{
		removed_idxs[i] = data[num_cols[i]].get_outliers_idxs(m);
	}

	set<int> final_removed_idx;
	for (const auto& s : removed_idxs) {

		std::set_union(
			final_removed_idx.begin(), final_removed_idx.end(),
			s.begin(), s.end(),
			std::inserter(final_removed_idx, final_removed_idx.end())
		);
	}

	return final_removed_idx;
}
dataFrame dataFrame::get_outliers (vector<string> cols, double m)
{
	return (*this)[outliers_idx(cols, m)];
}
dataFrame dataFrame::drop_outliers(vector<string> cols, double m, bool inplace)
{
	auto final_removed_idx = outliers_idx(cols, m);

	if (inplace) {
		drop(final_removed_idx);
		return *this;
	}

	dataFrame new_df = *this;
	new_df.drop(final_removed_idx);
	return new_df;
}

set<int> dataFrame::duplicated()
{
	set<int> dup_i;
	unordered_set<vector<const Object*>,
		std::hash<vector<const Object*>>, ObjectPtrVectorEqual> unique_row;

	int df_size = (int)data.begin()->second.size,
	   col_size = (int)columns.size();

	vector<const column*> cols_ref;
	cols_ref.reserve(columns.size());

	for (const string& colname : columns) { cols_ref.push_back(&data[colname]); }

	for (int i = 0; i < df_size; i++)
	{
		vector<const Object*> temp_row;
		temp_row.reserve(col_size);

		for (const column* col : cols_ref) { temp_row.push_back(&(col->values[i])); }

		if (!unique_row.emplace(temp_row).second) { dup_i.insert(i); }
	}
	return dup_i;
}

dataFrame dataFrame::drop_duplicates(bool inplace)
{
	set<int> dup_idxs = duplicated();
	if (inplace) {
		drop(dup_idxs);
	}
	else {
		dataFrame new_df = *this;
		new_df.drop(dup_idxs);
		return new_df;
	}
	return dataFrame();
}

/////////////////////////////////////
// Filtration

inline std::set<int> intersection_idx(std::vector<std::set<int>>& sets) {

	if (sets.empty())
		return {};

	vector<vector<int>> setidx_size;
	setidx_size.reserve(sets.size());

	int i = 0;
	for (const auto& set_ : sets) {
		vector<int> t = { i++, (int)set_.size() };

		setidx_size.push_back(t);
	}

	std::sort(setidx_size.begin(), setidx_size.end(),
		[](const vector<int> a, const vector<int> b) {
			return a[1] < b[1];
		});

	vector<set<int>*> new_sets(sets.size());

	for (size_t i = 0; i < sets.size(); i++)
	{
		new_sets[i] = &(sets[setidx_size[i][0]]);
	}

	std::set<int> result = *new_sets[0];


	// Perform intersection with each subsequent set
	for (size_t i = 1; i < sets.size(); ++i) {
		std::set<int> temp;
		const auto& current_set = *new_sets[i];

		// Keep only elements that are in both sets
		for (const auto& elem : result) {
			if (current_set.find(elem) != current_set.end()) {
				temp.insert(elem);
			}
		}

		// Early exit if the intersection becomes empty
		if (temp.empty()) {
			return {};
		}

		result = std::move(temp); // Move temp to result for the next iteration
	}

	return result;
}

dataFrame dataFrame::filter(vector<set<int>> indexes)
{
	if (indexes.empty()) return dataFrame();
	set<int> result = intersection_idx(indexes);
	if (result.empty()) return dataFrame();
	return (*this)[result];
}

set<int> dataFrame::filter_by_condition_idx(
	vector<string> cols,
	vector<std::function<bool(const Object&)>> conditions)
{
	auto num_cols = (int)cols.size();
	auto num_conditions = conditions.size();
	if (num_conditions != 1 && num_cols != num_conditions) {
		cout << "\nNumber of columns: " << num_cols << " Number of Conditions: " << num_conditions << endl;
		cout << "Invalid Filters by conditions, Must num cols = num conditions if there is are more than one column";
		assert(0);
	}

	vector<set<int>> cols_idx_res(num_cols);

	#pragma omp parallel for num_threads(num_cols)
	for (size_t i = 0; i < num_cols; i++) {
		cols_idx_res[i] = data[cols[i]].filterByConditionIdx(
			conditions[num_conditions == 1 ? 0 : i]
		);
	}

	return intersection_idx(cols_idx_res);
}

dataFrame dataFrame::filter_by_condition(
	vector<string> cols,
	vector<std::function<bool(const Object&)>> conditions)
{
	auto final_idx = filter_by_condition_idx(cols, conditions);
	return (*this)[final_idx];
}

set<int> dataFrame::filter_by_condition_idx(
	map<
		string,
		std::function<bool(const Object&)>
	> cols_conditions)
{
	// filter dataframe by bool function for each column name in cols.
	// and if many columns names and one bool function, this indicates 
	// that apply this function to all columns in cols.
	// else, num of columns must the same number of functions if number of function is not 1.

	vector<string> colsnames;
	vector<std::function<bool(const Object&)>> colconditions;

	for (const auto& col_condition : cols_conditions) {

		if (this->is_valid_column(col_condition.first)) {

			colsnames.push_back(col_condition.first);
			colconditions.push_back(col_condition.second);
		}
		else {
			cout << "Invalid Filtering this Colname Is not Valid: " << col_condition.first << endl;
			assert(0);
		}
	}

	return filter_by_condition_idx(colsnames, colconditions);
}
dataFrame dataFrame::filter_by_condition(
	map<
		string, 
		std::function<bool(const Object&)>
	> cols_conditions
)
{
	auto final_idx = filter_by_condition_idx(cols_conditions);

	return (*this)[final_idx];
}

/////////////////////////////////////
// Data Exploratory

dataFrame dataFrame::groupBy(
	vector<string> by,
	string targetcol,
	StatFun func)
{
	for (const auto& colname : by) {
		if (!is_valid_column(colname)) { cout << "this \"" << colname << "\" Invalid Column name\n"; assert(0); }
	}

	unordered_map<
		vector<Object>,
		vector<Object>
	> groupMap;

	int cols_size = (int)by.size();
	auto key = vector<Object>(cols_size);

	for (int i = 0; i < size; i++)
	{
		std::transform(by.begin(), by.end(), key.begin(),
			[&](const string& col) { return data[col][i]; });

		auto& group = groupMap.try_emplace(key, vector<Object>()).first->second;

		group.emplace_back(data[targetcol][i]);
	}


	int new_size = (int)groupMap.size();
	vector<column> new_cols(cols_size + 1);

	for (size_t i = 0; i < cols_size; i++)
	{
		new_cols[i] = column(new_size, true);
		new_cols[i].set_name(by[i]);
		new_cols[i].size = new_size;
		new_cols[i].type = data[by[i]].type;
		new_cols[i].secType = data[by[i]].secType;
	}

	new_cols[cols_size] = column(new_size, true);
	new_cols[cols_size].set_name(targetcol);
	new_cols[cols_size].size = new_size;


	int i = 0;
	Object stat_value;

	for (auto& entery : groupMap) {

		vector<Object> vals = std::move(entery.second);
		if (vals.size() == 1) {
			stat_value = func == COUNT ? 1 : func == STD ? Object() : vals[0];
		}
		else {
			switch (func)
			{
			case MEAN:  stat_value = column::mean(vals, 0, vals.size());		break;
			case MEDIAN:stat_value = column::median(vals, 0, vals.size());		break;
			case STD:   stat_value = column::std(vals, 0, vals.size());		break;
			case MODE:  stat_value = column::mode(vals, 0, vals.size());		break;
			case SUM:   stat_value = column::sum(vals, 0, vals.size());		break;
			case COUNT: stat_value = Object(vals.size(), Dtype::INT32); break;
			default:    stat_value = Object();									break;
			}
		}

		for (size_t j = 0; j < cols_size; j++) {
			new_cols[j].values[i] = entery.first[j];
		}

		new_cols[cols_size].values[i++] = stat_value;
		if (new_cols[cols_size].type < stat_value.type) {
			new_cols[cols_size].type = stat_value.type;
		}
	}

	new_cols[cols_size].secType = Object::get_sec_type(new_cols[cols_size].type);

	return dataFrame(new_cols);
}


dataFrame dataFrame::orderBy(vector<string> by, vector<bool> revers) {

	for (const auto& colname : by) {
		if (!is_valid_column(colname)) { cout << "this \"" << colname << "\" Invalid Column name\n"; assert(0); }
	}

	size_t reverse_size = revers.size();

	if (reverse_size != 1 && reverse_size != by.size()) {
		assert(false && "Invalid order parameters, revers size must same by size");
	}

	unordered_map<string, column> temp_data = data;

	// Reserve memory for sorting keys and index
	vector<
		pair<
		uint64_t,
		vector<const Object*>
		>
	> temp_by_values;
	temp_by_values.reserve(size);

	size_t by_size = by.size();

	// Pre-fetch column references to avoid repeated map lookups
	vector<const column*> by_columns;
	by_columns.reserve(by_size);

	for (const string& by_col : by) {
		by_columns.push_back(&data[by_col]);
	}

	// Prepare rows for sorting
	for (int i = 0; i < size; i++) {

		vector<const Object*> row_values;
		row_values.reserve(by_size);

		for (const column* col : by_columns) {
			row_values.emplace_back(&(col->values[i]));  // Use pointer for fast access
		}
		temp_by_values.emplace_back(i, std::move(row_values));
	}

	std::sort(temp_by_values.begin(), temp_by_values.end(),
		[&by_size, &revers, &reverse_size](
			const pair<uint64_t, vector<const Object*>>& a,
			const pair<uint64_t, vector<const Object*>>& b) {

				bool curr_revers;
				for (size_t i = 0; i < by_size; i++) {
					if (*(a.second[i]) == *(b.second[i])) continue;

					curr_revers = (reverse_size == 1) ? revers[0] : revers[i];
					if (curr_revers) {
						return *(a.second[i]) > *(b.second[i]);
					}
					else {
						return *(a.second[i]) < *(b.second[i]);
					}
				}

				return a.first < b.first;
		}
	);

	vector<int> new_index;
	new_index.reserve(size);

	for (const auto& row : temp_by_values) {
		new_index.emplace_back(row.first);
	}

	for (const string& colname : columns) {
		temp_data[colname] = temp_data[colname][new_index];
	}

	dataFrame new_df = dataFrame(temp_data);
	new_df.columns = columns;

	return new_df;
}

inline const unordered_map<
	Object,
	unordered_map<string, Object>
> dataFrame::get_merg_df_values_on(const dataFrame& df, const string on) const
{
	unordered_map<Object, unordered_map<string, Object>> df_values_on;

	auto df_size = df.size;

	for (int i = 0; i < df_size; i++) {

		const Object on_val = df[on][i];

		if (df_values_on.find(on_val) != df_values_on.end()) continue;

		for (const string& other_colname : df.columns) {
			if (other_colname == on) continue;

			df_values_on[on_val][other_colname] = df[other_colname][i];
		}
	}

	return df_values_on;
}

dataFrame dataFrame::merge(const dataFrame& other, const string on, string how)
{
	auto left_values = get_merg_df_values_on(*this, on),
		right_values = get_merg_df_values_on(other, on);

	unordered_map<string, column> new_df_data;
	vector<string> all_cols(columns.size() + other.columns.size() - 1);
	int i = 0;

	for (const string& colname : columns) {
		new_df_data[colname] = column();
		new_df_data[colname].mixed_type = data[colname].mixed_type;
		new_df_data[colname].set_name(data[colname].name);
		all_cols[i++] = colname;
	}
	for (const string& colname : other.columns) {
		if (colname == on) continue;

		new_df_data[colname] = column();
		new_df_data[colname].mixed_type = other[colname].mixed_type;
		new_df_data[colname].set_name(other[colname].name);

		all_cols[i++] = colname;
	}


	if (how == "inner") {

		for (const auto& on_val_keyval : left_values) {

			auto it = right_values.find(on_val_keyval.first);
			if (it == right_values.end()) continue;

			new_df_data[on].appendPushBack(on_val_keyval.first);

			for (const auto& this_colname_value : on_val_keyval.second) {
				new_df_data[this_colname_value.first].appendPushBack(this_colname_value.second);
			}
			for (const auto& other_colname_value : it->second) {
				new_df_data[other_colname_value.first].appendPushBack(other_colname_value.second);
			}
		}
	}
	else if (how == "outer") {

		unordered_set<Object> all_on_keys;
		for (const auto& on_keyval : left_values) {
			all_on_keys.insert(on_keyval.first);
		}
		for (const auto& on_keyval : right_values) {
			all_on_keys.insert(on_keyval.first);
		}

		for (const auto& on_key : all_on_keys) {

			auto it_left = left_values.find(on_key),
				it_right = right_values.find(on_key);

			new_df_data[on].appendPushBack(on_key);

			if (it_left == left_values.end()) {
				for (const string& colname : columns) {
					if (colname == on) continue;
					new_df_data[colname].appendPushBack(Object());
				}
			}
			else {
				for (const auto& colname_val : it_left->second) {
					new_df_data[colname_val.first].appendPushBack(colname_val.second);
				}
			}

			if (it_right == right_values.end()) {
				for (const string& colname : other.columns) {
					if (colname == on) continue;
					new_df_data[colname].appendPushBack(Object());
				}
			}
			else {
				for (const auto& colname_val : it_right->second) {
					new_df_data[colname_val.first].appendPushBack(colname_val.second);
				}
			}
		}
	}
	else if (how == "right" || how == "left") {

		auto* high_values = how == "right" ? &right_values : &left_values;
		auto* less_values = how == "right" ? &left_values : &right_values;

		auto* less_columns = how == "right" ? &columns : &other.columns;

		for (const auto& on_val_keyval : *high_values) {

			new_df_data[on].appendPushBack(on_val_keyval.first);

			for (const auto& colname_value : on_val_keyval.second) {
				new_df_data[colname_value.first].appendPushBack(colname_value.second);
			}

			auto it = less_values->find(on_val_keyval.first);

			if (it == less_values->end()) {

				for (const auto& colname : *less_columns) {
					if (colname == on) continue;
					new_df_data[colname].appendPushBack(Object());
				}
			}
			else {
				for (const auto& colname_value : it->second) {
					new_df_data[colname_value.first].appendPushBack(colname_value.second);
				}
			}
		}
	}
	return dataFrame(new_df_data, all_cols);
}

/////////////////////////////////////
// Apply
dataFrame dataFrame::apply(map<const std::string, std::function<Object(const Object&)>> col_functions, bool inplace)
{
	if (inplace) {
		for (const auto& col_function : col_functions) {
			data[col_function.first] = data[col_function.first].apply(col_function.second);
		}
		dataFrame df;
		return df;
	}

	auto deep_copy_df = *this;
	for (const auto& col_function : col_functions) {
		deep_copy_df[col_function.first] = deep_copy_df[col_function.first].apply(col_function.second);
	}
	return deep_copy_df;
}

void dataFrame::papply(map<const string, std::function<Object(const Object&)>> col_functions)
{
	vector<string> cols;
	cols.reserve(col_functions.size());

	for (const auto& col_function : col_functions) {
		cols.emplace_back(col_function.first);
	}

	// Use OpenMP for parallel processing of column functions
	#pragma omp parallel for 
	for (size_t i = 0; i < cols.size(); ++i) {
		const string& curr_col = cols[i];
		data[curr_col].papply(col_functions.at(curr_col));
	}
}
/////////////////////////////////////
// Others
void dataFrame::optimiz_mem(bool with_mixed_types)
{
	for (auto& name_col : this->data) {
		name_col.second.optimiz_mem(with_mixed_types);
	}
}

void dataFrame::to_csv(const std::string& targetPath) {

	std::ofstream file{ targetPath };
	assert(file.is_open() && "file can't open");

	int col_size = static_cast<int>(columns.size());

	// Write the header
	std::ostringstream header;
	for (int i = 0; i < col_size; i++) {
		header << columns[i];
		if (i < col_size - 1) header << ",";
	}
	file << header.str() << "\n";

	std::ostringstream line;

	for (int i = 0; i < size; i++) {
		line.str("");  
		line.clear();  

		for (int e = 0; e < col_size; e++) {
			const std::string& col_name = columns[e];
			auto& cell = data[col_name][i];
			line << (cell.type == Dtype::NA ? "" : cell.get());

			if (e < col_size - 1) line << ",";
		}
		file << line.str() << "\n";
	}

	std::cout << "Save file with path: " << targetPath << ".\n";
}

dataFrame dataFrame::concat(dataFrame otherdf)
{
	assert(columns.size() == otherdf.columns.size() && "both data-frames must have same size of columns");
	for (string colname : otherdf.columns) {
		if (!is_valid_column(colname)) {
			cout << "col name: " << colname << "in new dataframe not found in current dataframe.\n";
			assert(false && "both dataframes must have same col names");
		}
	}
	dataFrame newdf = *this;
	for (string colname : otherdf.columns) { newdf[colname] = newdf[colname].concat(otherdf[colname]); }
	newdf.size += otherdf.size;
	return newdf;
}

 //Private Methods

bool dataFrame::is_valid_column(string name) const {
	return std::find(this->columns.begin(), this->columns.end(), name) != this->columns.end();
}
bool dataFrame::is_same_length(vector<column> cols)
{
	int init_size = cols[0].size;
	for (column col : cols) if (col.size != init_size) return false;
	return true;
}
int  dataFrame::get_max_col_length(vector<column> cols)
{
	int max_col_len = 0;
	for (column col : cols) if (col.size > max_col_len) max_col_len = col.len();
	return max_col_len;
}
void  dataFrame::make_cols_same_length(vector<column>& cols)
{
	int max_col_len = get_max_col_length(cols);
	for (auto& col : cols) {
		if (col.len() == max_col_len) continue;
		
		int diff_len = max_col_len - col.len();
		while (diff_len--) col.appendPushBack(Object());
	}
}
std::ifstream::pos_type filesize(string filename)
{
	std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
	return in.tellg();
}
int dataFrame::count_file_lines(const std::string& filePath)
{
	std::ifstream file(filePath);
	if (!file.is_open()) { std::cerr << "Failed to open the file." << std::endl; return -1; }

	const int buffer_size = (const int)filesize(filePath);
	std::vector<char> buffer(buffer_size);
	int lineCount = 0;
	file.read(buffer.data(), buffer_size);
	for (char c : buffer) if (c == '\n') ++lineCount;
	return lineCount;
}

bool dataFrame::operator==(dataFrame other)
{

	if (columns.size() != other.columns.size()) {
		std::cout << "This not Equal col len to compare!\n";
		assert(0);
	}

	for (string colname : other.columns)
	{
		if (!is_valid_column(colname)) {
			std::cout << "This col name: " << colname << " not in compared df !\n";
			assert(0);
		}
	}

	bool result = true;

	// Parallelize the outer loop for column comparison
#pragma omp parallel for
	for (int i = 0; i < columns.size(); ++i) {

		std::string colname = columns[i];

		const auto& temp_res = (this->data.at(colname)).values;
		const auto& other_col = (other.data.at(colname)).values;

		// Compare column values
		for (int j = 0; j < temp_res.size(); ++j) {

			if (temp_res[j] != other_col[j]) {

				// If a mismatch is found, update result and break from the loop
#pragma omp critical
				{
					result = false;
				}
				break;
			}
		}
	}

	return result;

	return 1;
}
// 1550