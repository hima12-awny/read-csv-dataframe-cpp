#pragma once
#include "../columnDs/column.h"

#include "../tableclass/Table.h"


#include <fstream>
#include <sstream>
#include <cassert>
#include <chrono>
#include <map>
#include <iterator>
#include <numeric>


class dataFrame
{
	#pragma region Private Methouds

	bool is_valid_column(string name) const;
	bool is_same_length(vector<column> cols);

	int get_max_col_length(vector<column> cols);
	void make_cols_same_length(vector<column>& cols);

	int count_file_lines(const std::string& filePath);

	inline const unordered_map<
		Object,
		unordered_map<string, Object>
	> get_merg_df_values_on(const dataFrame& df, const string on) const;
	#pragma endregion

public:

	#pragma region Attributes

		string path;
		unordered_map<string, column> data;
		vector<string> columns;
		int size;

	#pragma endregion

	#pragma region Constructors

		dataFrame();    

		// this read csv file by file path, and enable mixed type column's values if true.
		dataFrame(string path, bool mixed_type = true);

		// dataframe by multiple columns. 
		dataFrame(vector<column> cols);

		// dataframe from data, key as column name and value as it's column. 
		dataFrame(unordered_map<string, column> input_data);

		dataFrame(std::initializer_list<std::pair<const std::string, column>> init);

		// dataframe from data, key as column name and value as it's column. 
		// with sorted columns names.
		dataFrame(unordered_map<string, column> input_data, vector<string> columns);

		// data from unorder_map that key as column name, value as many values of Objects in vector.
		dataFrame(unordered_map<string, vector<Object>> input_data);

		// data from unorder_map that key as column name, value as many values of Objects in vector,
		// with sorted column names.
		dataFrame(unordered_map<string, vector<Object>> input_data, vector<string> columns);

		// Copy assignment operator that copy data of another df to assignee.
		dataFrame& operator=(const dataFrame& other);

	#pragma endregion

	#pragma region Data Accessing

			// get column by it's name from the dataframe - normal.
			column& operator[](string col);

			// get column by it's name from the dataframe - const case.
			const column& operator[](string col) const;

			// get dataframe by columns names as input.
			dataFrame operator[](vector<string> colsNames);
			dataFrame operator[](initializer_list<string> colsNames);

			// get dataframe by filtered input indexes.
			dataFrame operator[](set<int> indexes);
			dataFrame operator[](vector<int> indexes);
			dataFrame operator[](initializer_list<int> indexes);

	#pragma endregion

	#pragma region Getters/Setters
	
    // get dataframe from index to index.
	dataFrame range(int from, int to);

	// get first n rows dataframe.
	dataFrame head(int n = 5);

	// get first n rows dataframe.
	dataFrame tail(int n = 5);


	// get all numbers columns names. 
	vector<string> get_num_cols ();

	// get all string columns names. 
	vector<string> get_str_cols ();

	// get all date columns names. 
	vector<string> get_date_cols();

	// get all columns names that match this second type. 
	vector<string> get_sectype_cols(Dtype cols_sectype);


	// add new col to dataframe. 
	// and if inplace true add it in this dataframe else add to new df and return it.
	dataFrame add_col(column col, bool inplace = 0);

	// get dataframe memory total size.
	size_t get_mem_size();


	// get recored by index, that key as name of column and it's value the Object value.
	unordered_map<string, Object> iloc(int idx);

	// get value that corresponds to row and col indexes. this deals with dataframe as 2D Matrix. 
	Object						  iloc(int row, int col);

	// get dataframe with specified vertical range(columns) and horizontal range(rows)
	dataFrame					  iloc(vector<int> vrange, vector<int> hrange);


	// get deep copy of the dataframe.
	dataFrame copy();


	// get random sample of dataframe rows.
	dataFrame sample(int size_, int seed = 0, bool with_replacement = 1);

	// shuffle dataframe rows.
	dataFrame shuffle(int seed = 0);

	#pragma endregion

	#pragma region Information About the Dataframe
	
	// return 2 values, the number of rows, number of columns in df
	vector<int> shape();

	// print all information about the df, like the dtypes of columns, numbers of na/s and so on.
	void info();

	// get basic statistical summarization of dataframe columns.
	dataFrame describe(Dtype include = Dtype::NA);

	// get correlations of number columns.
	dataFrame corr();

	// get how many columns for each dtype.
	unordered_map<Dtype, uint16_t> dtype_count();

	#pragma endregion

	#pragma region Print Functions

		// print the whole dataframe, with option to print the index column.
		void p(bool printGenIndex = true);

		// print the head of dataframe.
		void phead(int limit = 5);

		// print the tail of dataframe.
		void ptail(int limit = 5);

		// print the columns of the dataframe.
		void cols_p();

	#pragma endregion

	#pragma region Dealing with bad data

		// drop row by index.
		inline void drop(int idx);

		// drop many rows by their indexes. 
		inline void drop(set<int>& idxs);


		// get rows indexes that have at lest one na value.
		set<int> naidxs(vector<string> colnames = {});

		// get dataframe with rows that have at lest one na value. 
		dataFrame get_na(vector<string> colnames = {});

		// drop rows that have at lest one na value.  
		void drop_na(vector<string> colnames = {});


		// get rows indexes that have at lest values that is outlier with respect to it's column. 
		set<int> outliers_idx(
			vector<string> cols = {},
			double m = 2.0
		);
	
		// get dataframe with rows that have at lest values that is outlier with respect to it's column. 
		dataFrame get_outliers(
			vector<string> cols = {},
			double m = 2.0
		);

		// drop rows that have at lest values that is outlier with respect to it's column. 
		dataFrame drop_outliers(
			vector<string> cols = {},
			double m = 2.0,
			bool inplace = false
		);
		

		// get duplicated rows indexes.
		set<int> duplicated();

		// drop duplicated rows from the df.
		dataFrame drop_duplicates(bool inplace = false);

	#pragma endregion

	#pragma region Filtration

	// filter dataframe based on intersection of all input indexes.
	dataFrame filter(vector<set<int>> indexes);

	// get filter dataframe indexes by bool function for each column name in cols.
	set<int> filter_by_condition_idx(
		vector<string> cols,
		vector<std::function<bool(const Object&)>> conditions
	);

	// get filter dataframe by bool function for each column name in cols.
	dataFrame filter_by_condition(
		vector<string> cols,
		vector<std::function<bool(const Object&)>> conditions
	);

	// get filter dataframe indexes by bool function(map value) for each column name(map key).
	set<int> filter_by_condition_idx(
		map <
			string,
			std::function<bool(const Object&)>
		> cols_conditions
	);

	// get filter dataframe by bool function(map value) for each column name(map key).
	dataFrame filter_by_condition(
		map <
			string,
			std::function<bool(const Object&)>
		> cols_conditions
	);
	#pragma endregion

	#pragma region Data Exploratory

	// group dataframe by columns names, and apply basic statics function in target column.
	dataFrame groupBy(
		vector<string> by,
		string	       targetcol,
		StatFun        func
	);

	// order the dataframe by columns, sorting ascending by defaults.
	// enable revers, if revers with size 1 so apply this revers option for all by columns.
	// is not, must be the same length of the by columns.
	dataFrame orderBy(
		vector<string> by, 
		vector<bool  > revers = { false }
	);

	// merge two dataframes based on column name, 
	// and the how to merge them Options = ['inner', 'outer', 'left', 'right']
	dataFrame merge(const dataFrame& other, const string on, string how = "inner");

	#pragma endregion

	#pragma region Apply

	// apply function to columns as key and their functions as value in input map.
	dataFrame apply(map<const std::string, std::function<Object(const Object&)>> col_functions, bool inplace=false);

	// apply function in parallel way to columns as key and their functions as value in input map.
	void papply(map<const string, std::function<Object(const Object&)>> col_functions);
	#pragma endregion

	// this optimize the memory/size of the dataframe for each column. 
	void optimiz_mem(bool with_mixed_types = true);

	// convert/save the dataframe into csv formate with path to save to it.
	void to_csv(const std::string& targetPath);

	// concatenate another dataframe to this one.
	dataFrame concat(dataFrame otherdf);

	// to enable cout operator
	friend ostream& operator<<(ostream& os, dataFrame df) { df.p(); return os; }

	// to enable equality comparison to another dataframe.
	bool operator == (dataFrame other);
};

// Overloaded operators for pretty printing
template <typename T>
ostream& operator<<(ostream& os, const vector<T>& vec) {
	os << "[";
	for (size_t i = 0; i < vec.size(); ++i) {
		os << vec[i];
		if (i != vec.size() - 1) os << ",  ";
	}
	os << "]";
	return os;
}

template <typename T>
ostream& operator<<(ostream& os, const set<T>& set_) {
	os << "[";
	size_t i = 0;
	for (const T& value : set_) {
		os << value;
		if (i++ != set_.size() - 1) os << ", ";
	}
	os << "]";
	return os;
}

// Debug print macro
#define dprint(var) DPrint(#var, var)
template <typename T>
void DPrint(const string& var_name, const T& var_value) {
	cout << var_name << " = " << var_value << endl;
}