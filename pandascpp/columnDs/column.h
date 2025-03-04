#pragma once

#ifndef Column_CLS
#define Column_CLS

#include <unordered_map>
#include <unordered_set>
#include <map>

#include <vector>
#include <string>
#include <set>

#include <iomanip>
#include <algorithm>
#include <cassert>

#include <mutex>
#include <execution>

#include <omp.h>

#include "../helpers/Tools.h"
#include "../ObjectDs/Object.h"


#include "../sort_cls/Sort.h"

#include <chrono>

#include <initializer_list>
#include <type_traits>
#include <iterator>

using namespace std;

// 144 byte
// 104

// Macro to normalize an index based on the size of the container.
#define NORMALIZE_INDEX(idx, size) ((idx) < 0 ? ((size) + (idx)) % (size) : (idx))


class column
{

	#pragma region Private Methods
		// Calculate operation on all elements with a given value.
		vector<Object> calc(char op, Object val);

		vector<int> map_sort(vector<Object>& arr, bool reverse, SortAlgo algo);

		Object kth_element(vector<Object>& arr, int start, int end, int k);

		SubArrayBoundris middelMaxSub(vector<Object> arr, int low, int mid, int high);
		SubArrayBoundris maxSubArraySub(vector<Object> arr, int low, int high);

		SubArrayBoundris middelMinSub(vector<Object> arr, int low, int mid, int high);
		SubArrayBoundris minSubArraySub(vector<Object> arr, int low, int high);
	#pragma endregion


public:

	#pragma region Attributes

		vector<Object> values;
		string name = "NA";
		Dtype  type = Dtype::NA,
			secType = Dtype::NA;
		bool   mixed_type = false;
		int    size = 0,

			maxLenStr = 0;
	#pragma endregion

	#pragma region Constructors

		template<typename Container>
		using is_iterable = std::void_t<
			decltype(std::begin(std::declval<Container>())),
			decltype(std::end(std::declval<Container>()))
		> ;

		// Helper to detect if type is iterable
		template<typename Container, typename = void>
		struct is_iterable_container : std::false_type {};

		template<typename Container>
		struct is_iterable_container<Container, is_iterable<Container>> : std::true_type {};


		
		column() { *this = column("NA", vector<Object>(), false); }

		column(int size, bool mixed_type = false) {
			values = vector<Object>(size);
			this->mixed_type = mixed_type;
		}

		template<typename Container>
		column(const Container& container,
			typename std::enable_if_t<is_iterable_container<Container>::value>* = nullptr) 
		{

			*this = column(
				"NA",
				container,
				true
			);
		}

		template<typename Container>
		column(std::string name_,
			const Container& container,
			bool mixed_type_ = false,
			typename std::enable_if_t<is_iterable_container<Container>::value>* = nullptr
		)
			: values(std::begin(container), std::end(container)),  
			name(name_),
			size((int)values.size()),
			maxLenStr((int)name.length()),
			mixed_type(mixed_type_),
			type(Object::get_highest_type(values)),
			secType(Object::get_sec_type(type))
		{
		}

		// Initializer list constructor
		column(std::initializer_list<Object> init)
			: values(init),
			name("NA"),
			size((int)init.size()),
			maxLenStr(0),
			mixed_type(false),
			type(Object::get_highest_type(values)),
			secType(Object::get_sec_type(type))
		{
		}

		// Constructor with name and initializer list
		column(std::string name_, std::initializer_list<Object> init)
			: values(init),
			name(name_),
			size((int)init.size()),
			maxLenStr((int)name_.length()),
			mixed_type(false),
			type(Object::get_highest_type(values)),
			secType(Object::get_sec_type(type))
		{
		}


	#pragma endregion

	#pragma region Index-Based Accessors
		Object&		  operator[](int idx)		{ return values[NORMALIZE_INDEX(idx, size)]; }
		const Object& operator[](int idx) const { return values[NORMALIZE_INDEX(idx, size)]; }

		column operator [] (   set<int> indexes) { return get_indexes(indexes); }
		column operator [] (vector<int> indexes) { return get_indexes(indexes); }
	#pragma endregion

	#pragma region Iterators
		// Begin and end methods for range-based for loops
		vector<Object>::iterator       begin()       { return values.begin(); }
		vector<Object>::const_iterator begin() const { return values.begin(); }
			
		vector<Object>::iterator       end()	   { return values.end(); }
		vector<Object>::const_iterator end() const { return values.end(); }
	#pragma endregion

	#pragma region Getter/Setter Attributes

		// Set the name of the column.
		void set_name(string name);

		// Get the type of the column as a string.
		string type_str() const;

		// Get the length (number of elements) in the column.
		int len();

	#pragma endregion

	#pragma region Append Functions

		// Append Object Value to the end of the values with specified Size In advance (Supports Mixed Dtype Value Columns).
		void append(Object val);

		// Append Multiples Value from vector to The Values Like in Python (Extend) with specified Size In advance
		void append(const vector<Object>& vals);

		// Append Object Value to the end of the values (Supports Mixed Dtype Value Columns).
		void appendPushBack(Object val);

		// Append Multiples Value from vector to The Values Like in Python (Extend)
		void appendPushBack(const vector<Object>& vals);

	#pragma endregion

	#pragma region Print Function

		// Print The Value Row useful in Printing the Table.
		void printAt(int idx);

		// Print The Ranges Value from index to another.
		void p(int from = -1, int to = -1);

		// Print where NA values exists as Index.
		void print_naidxs();

		// Print The Head Data Range first value to Limits 
		void phead(int limit = 5);

		// Print The Tail Data Range from (-limits) to the end of the data.
		void ptail(int limit = 5);

	#pragma endregion

	#pragma region Get Column Values
		
		// Get Data Range from index to another.
		column range(int from, int to);

		// Get Data Range from index 0 to limit=(default 5).
		column head(int limit = 5);

		// Get Data Range from index size-(limit=default 5) to the size of the data.
		column tail(int limit = 5);

		// get copy of all column values
		column copy();

		// Get Values of this Many indexes from iterable container
		template<typename Container>
		column get_indexes(const Container& indexes,
			typename std::enable_if_t<is_iterable_container<Container>::value>* = nullptr);

		column get_indexes(std::initializer_list<int> indexes);

	#pragma endregion 

	#pragma region Handling Missing Data

		// get the indexes of NA values in vector;
		set<int> naidxs();

		// get the indexes of not NA values in vector;
		set<int> not_naidxs();

		// fill NA Value, fill With mode if the type is String, Mean if number.
		// and if inplace=true make the change in the original column else make the change in new one and return it.
		column fillna(bool inplace = false);

		// fill NA Value, fill With Input Value,
		// and if inplace=true make the change in the original column else make the change in new one and return it.
		column fillna(Object val, bool inplace = false);
	#pragma endregion

	#pragma region Statistical Operations
		// this region Methods is valid only for number operations.

		// get the minimum value.
		double min();

		// get the maximum value.
		double max();

		// calculate the sum of values
		double sum();

		// calculate the mean of values
		double mean();

		// calculate the standard divination.
		// with option if have mean as parameter to speedup the operation.
		double std(bool haveMean = false, double meanValue = 0);

		// calculate the standard divination, multi-thread version.
		// with option if have mean as parameter to speedup the operation.
		double std_2(bool haveMean = false, double meanValue = 0);

		// calculate the median, with option if the values sorted.
		double median(bool sorted = false, vector<double> optionalValues = {});

		// calculate the mode(most frequent value), with option of values counts.
		Object mode(bool with_valu_count = false, unordered_map<Object, int> value_counts_ = {});


		// Calculate the correlation between this column and another.
		double corr(
			column& other,
			bool with_means = false, double mean_x = 0, double mean_y = 0,
			bool with_stds  = false, double std_x  = 0, double std_y  = 0);

		// Calculate Quantile
		double quantile(float q, bool withInSortedVec = false, vector<double> sortedValues = {});
		double quantile_2(float q, vector<double>& sortedValues, bool withInSortedVec = false);

		// Apply a function to rolling windows.
		column rolling(int window, StatFun  func);

		// Function for intervals.
		column for_each_interval(int interval_size, StatFun func);

		// Difference calculation across elements.
		column diff(int periods = 1, string change_func = "normal");

		// Inline methods for statistical calculations on subsets.
		static inline Object mean  (const vector<Object>& values_, size_t start, size_t end);
		static inline Object std   (const vector<Object>& values_, size_t start, size_t end);
		static inline Object mode  (const vector<Object>& values_, size_t start, size_t end);
		static inline Object sum   (const vector<Object>& values_, size_t start, size_t end);
		static inline Object median(      vector<Object>& values_, size_t start, size_t end);

	#pragma endregion

	#pragma region Outliers Detection

		// Detect and remove outliers based on z-score threshold `m`.
		column remove_outliers(double m = 2.0, bool inplace = false);
		set<int>& remove_get_outliers_idxs(double m = 2.0);
		set<int> get_outliers_idxs(double m = 2.0);

	#pragma endregion

	#pragma region String Operations

		// Transformations for string elements in the column.
		// Convert to lowercase.
		column to_lw(); 

		// Convert to uppercase.
		column to_up(); 

		// Convert to title case.
		column to_title(); 

		// Remove leading/trailing whitespace.
		column strip(); 

		// Search and pattern extraction.
		// Indexes of elements containing a substring.
		set<int> contains(string target);

		// Extract regex matches.
		column str_extract_pattern(const string& pattern);

	#pragma endregion

	#pragma region Conversion

	// converts values to Double Number if the column not Number.
	column to_num (bool inplace=false);

	// converts values to Strings
	column to_str (bool inplace=false);

	// converts values to dates
	column to_date(bool inplace=false, DateFormat dateformat=DateFormat::AUTO);
	static column to_date(column col, DateFormat dateFormate);

	// concerts values to date attributes like (date_year, date_month, date_day)
	column to_date_attr(bool inplace, Dtype date_attr_type);

	column to_date_year (bool inplace = false);
	column to_date_month(bool inplace = false);
	column to_date_day  (bool inplace = false);

	// converts all values to New Data type as Parameter
	// and if inplace = true converts the this.values and return void default column. 
	// inplace = false converts copy of column's values and return it. 	 
	column to_type(Dtype new_type, bool inplace = false);

	// Get New Container to column's values
	// put the values in set and get it.
	set<Object> to_set();
	// put the values in unordered set and return.
	unordered_set<Object> to_uset();

#pragma endregion

	#pragma region "Info About Column's Data"

	// generate description for column data based on it's data type. 
	// return map name of statistic as key and it's value.
	unordered_map<string, Object> describe();

	// get the values of statistics based on column's type.
	vector<Object> getDescribeObjectValues(Dtype include = Dtype::NA);

	// print describe table 
	void describe_print();

	// for each value count how much in all values
	unordered_map<Object, int> value_count();

	// for each value count the percentage of it with respect of all values.
	unordered_map<Object, float> value_count_pct();

	// print table of value count and it's pct
	void value_count_print();

	// print table of value count and it's pct
	void value_count_pct_print();

	// get unique values of all values.
	unordered_set<Object> unique();

	// get size of unique values.
	int unique_count();

	// get size of NA values.
	int na_count();

#pragma endregion

	#pragma region Alteration

	// drop value with index (inplace=true by defaults)
	void drop(int idx);

	// drop set of indexes at once.
	column drop(set<int> indexes, bool inplace= false);

	// drop all indexes wheres it's values NA
	column dropna(bool in_place = false);

	// add values of other column to this column and return th new one.
	column concat(column othercol);

	// repeat the whole length of value number of times.
	column repeat(int times);

	// repeat each value of all values number of times.
	column repeat_for_each(int times);

	// get sample of values, with the size of output size, and seed number of random generator.
	column sample(int size_, int seed = 0, bool with_replacement = 1);

	// get shuffled values in new column based on seed value. 
	column shuffle(int seed = 0);

#pragma endregion

	#pragma region Sorting
	// sort values with user-defined class Named Sort
	// ascending by defaults, you can make revers=true to make it descending 
	// use algo to specify which sorting algorithm used (see available ones in SortAlgo Enum).
	// this sort in copy of values.
	column sortAndGetCol(bool revers = false, SortAlgo algo = SortAlgo::heap);

	// Same Above, and return the reordered indexes not the values.
	vector<int> sortAndGetIndexs(bool revers = false, SortAlgo algo = SortAlgo::heap);

	// Same Above, but Sort in this values, and don't return column.
	void sort(bool revers = false, SortAlgo algo = SortAlgo::heap);
#pragma endregion

	#pragma region Algorithmic Methods
	// get kth element from the begin of the values (smallest to biggest) O(N) 
	// this handle data as value bigger or smaller as value.
	Object kth_element(int k);

	// get one largest kth element. O(N)
	Object kth_largest_element(int k);

	// get one smallest kth element. O(N)
	Object kth_smallest_element(int k);

	// get many elements that are the most largest in the values. O(N) and insertion sort.
	column kth_largest_elements(int k, bool reverse = false);

	// get many elements that are the most smallest in the values. O(N) and insertion sort.
	column kth_smallest_elements(int k, bool reverse = false);

	// get kth top one category with respect to frequency in values. O(N) 
	Object kth_top_cat(uint32_t k);

	// get kth lest one category with respect to frequency in values. O(N) 
	Object kth_least_cat(uint32_t k);

	// get kth top many category with respect to frequency in values. O(N) and insertion sort.
	column kth_top_cats(uint32_t k);

	// get kth lest many category with respect to frequency in values. O(N) and insertion sort.
	column kth_lest_cats(uint32_t k);

	// get max sub-sequence from numbers values O(NlogN).
	SubArrayBoundris maxSubSeq();

	// get min sub-sequence from numbers values O(NlogN).
	SubArrayBoundris minSubSeq();

#pragma endregion

	#pragma region Duplication

	set<int> duplicated();

	column drop_duplicated(bool inplace = false);

#pragma endregion

	#pragma region Handling Memory

	// get total Column size in memory.
	size_t get_mem_size();

	// convert the column dtype to most suitable dtype based on it's range values or data types.
	// if with_mixed_type = true, enable the values have different data type.
	// if with_mixed_type = false, get max suitable data type and make converts all values into it.
	void optimiz_mem(bool with_mixed_type = true);

#pragma endregion

	#pragma region Apply

		// Apply user-defined function
		column apply(std::function<Object(const Object&)> func);

		// Parallel Apply user-defined function
		void papply(const std::function<Object(const Object&)>& func);

	#pragma endregion

	#pragma region Filtering

		// take many indexes and get values with intersection of them.
		column filter(vector<set<int>>& indexes);

		// get the values of this indexes.
		column filter(set<int>& indexes);

		// filter values by user function with Object value and get the matched indexes.
		set<int> filterByConditionIdx(std::function<bool(const Object&)>condition);

		// filter values by user function with Object value and get the matched values in column.
		column filterByCondition(std::function<bool(const Object&)>condition);

		// filter values by user function with Object value and current idx and get the matched indexes.
		set<int> filterByConditionIdx(std::function<bool(const size_t idx, const Object&)>condition);

		// filter values by user function with Object value and current idx get the matched values in column.
		column filterByCondition(std::function<bool(const size_t idx, const Object&)>condition);

		// get values' indexes that in input vector.
		template<typename Container>
		set<int> isin(const Container& invalues,
			typename std::enable_if_t<is_iterable_container<Container>::value>* = nullptr);

		set<int> isin(std::initializer_list<Object> invalues);

	#pragma endregion

	#pragma region Comparesion Operators

		set<int> operator < (Object value);
		set<int> operator > (Object value);

		set<int> operator <= (Object value);
		set<int> operator >= (Object value);

		set<int> operator == (Object value);

		vector<bool> operator == (column other);

	#pragma endregion

	#pragma region Arthmatic Operators

		column operator + (Object other);
		column operator - (Object other);
		column operator * (Object other);
		column operator / (Object other);
		column operator % (Object other);

		column operator + (column other);
		column operator - (column other);
		column operator * (column other);
		column operator / (column other);
		column operator % (column value);

		void operator += (Object other) {
			values = calc('+', other);
		}
		void operator -= (Object other) {
			values = calc('-', other);
		}
		void operator *= (Object value) {
			values = calc('*', value);
		}
		void operator /= (Object value) {
			values = calc('/', value);
		}

	#pragma endregion

	#pragma region Random Generating Values

		// generate random numbers as column.
		static column rand_nums(int size, int max, int min = 0, unsigned int seed = 0);

	#pragma endregion
	
	#pragma region utilities

			// useful while printing 
			void calc_max_str_val_len();

	#pragma endregion

	friend ostream& operator<<(ostream& os, column col)
	{
		os << "[";
		for (int i = 0; i < col.size; i++)
		{
			if (i < col.size - 1) os << col.values[i] << ", ";
			
			else os << col.values[i];
			
		}
		os << "]";
		return os;
	}
};

inline Object column::sum(const std::vector<Object>& values_, size_t start, size_t end)
{
	if (start >= end || end > values_.size()) {
		throw std::invalid_argument("Invalid indexes");
	}

	double sum_ = 0;
	for (size_t i = start; i < end; ++i) {
		if (values_[i].secType != Dtype::NUMBER) continue;
		sum_ += values_[i].value_num();

	}
	return Object(sum_);
}

inline Object column::mean(const std::vector<Object>& values_, size_t start, size_t end)
{
	if (start >= end || end > values_.size()) {
		throw std::invalid_argument("Invalid indexes");
	}

	double sum_ = 0;
	for (size_t i = start; i < end; i++)
	{
		if (values_[i].secType != Dtype::NUMBER) continue;
		sum_ += values_[i].value_num();

	}
	return Object(sum_ / (double)(end - start));
}

inline Object column::std(const std::vector<Object>& values_, size_t start, size_t end)
{
	if (start >= end || end > values_.size() || end - start < 2) {
		throw std::invalid_argument("Invalid indexes or sample size must be at least 2 for std calculation");
	}
	double mean_value = mean(values_, start, end).value_num();

	double temp_diff;
	double sum_sq_diff = 0;

	for (size_t i = start; i < end; i++)
	{
		if (values_[i].secType != Dtype::NUMBER) continue;
		temp_diff = values_[i].value_num() - mean_value;
		sum_sq_diff += temp_diff * temp_diff;

	}

	return Object(std::sqrt(sum_sq_diff / (end - start - 1)));
}

inline Object column::median(vector<Object>& values_, size_t start, size_t end)
{
	if (start >= end || end > values_.size()) {
		throw std::invalid_argument("Invalid indexes");
	}

	std::sort(values_.begin() + start, values_.begin() + end);

	size_t mid = start + (end - start) / 2;
	if ((end - start) % 2 == 0) {
		return Object((values_[mid - 1] + values_[mid]) / 2.0);
	}
	else {
		return values_[mid];
	}
}

inline Object column::mode(const std::vector<Object>& values_, size_t start, size_t end)
{
	if (start >= end || end > values_.size()) {
		throw std::invalid_argument("Invalid indexes");
	}

	std::map<Object, size_t> counts;
	Object max_element = values_[start];
	size_t max_count = counts[max_element];

	for (size_t i = start; i < end; ++i) {

		counts[values_[i]]++;

		if (counts[values_[i]] > max_count) {

			max_count = counts[values_[i]];
			max_element = values_[i];
		}
	}


	return max_element;
}



#endif // !Column_CLS

#define CHECK_NUMBER_METHOUDS(obj) \
    assert(obj.secType == Dtype::NUMBER && "This method only used for Numbers only")

#define CHECK_STRING_METHOUDS(obj) \
    assert(obj.secType == Dtype::STRING && "This method only used for Strings only")

#define STRING_FUNCTION(col, func) \
do { \
    CHECK_STRING_METHOUDS((col)); \
    column new_column = col; \
    for (Object& val : new_column.values) { \
        val = val.func(); \
    } \
    return new_column; \
} while(0)


#define COLUMN_COMPARE_OP(this_ptr, value, op) \
    [&]() -> set<int> { \
        assert((this_ptr) != nullptr && "Column pointer cannot be null"); \
        assert(((this_ptr)->secType == (value).secType && (value).type != Dtype::NA) && "Invalid comparison types"); \
        set<int> indexes; \
        const auto* const _col = (this_ptr); \
        const auto& _val = (value); \
        for (int i = 0; i < _col->size; i++) { \
            if (_col->values[i].type == Dtype::NA) continue; \
            if (_col->values[i] op _val) indexes.insert(i); \
        } \
        return indexes; \
    }()


#define COLUMN_ARITHMETICS_SCALAR_OP(this_ptr, other, op) \
    [&]() -> column { \
        column new_col = (*this_ptr);\
        \
        const auto* const _col = (this_ptr); \
        const auto& _other = (other); \
        \
        for (int i = 0; i < _col->size; i++) { \
            if (_col->values[i].type == Dtype::NA) { \
                new_col.values[i] = Object(); \
            } else { \
                new_col.values[i] = _col->values[i] op _other; \
            } \
        } \
        return new_col; \
    }()


#define COLUMN_ARITHMETICS_OP_COLUMN(col1_ptr, col2_ptr, op) \
    [&]() -> column { \
        assert((col1_ptr)->secType == (col2_ptr)->secType && "two columns must be the same type"); \
        assert((col1_ptr)->size == (col2_ptr)->size && "two columns must be the same length"); \
        \
        const auto* const _col1 = (col1_ptr); \
        const auto* const _col2 = (col2_ptr); \
        \
        column new_col = *_col1; \
        new_col.set_name("(" + _col1->name + ")" + " " #op " " + "(" + _col2->name + ")"); \
        new_col.type = std::max(_col1->type, _col2->type); \
        \
        for (int i = 0; i < _col1->size; i++) { \
            if (_col1->values[i].type != Dtype::NA && _col2->values[i].type != Dtype::NA) { \
                new_col.values[i] = _col1->values[i] op _col2->values[i]; \
            } \
        } \
        return new_col; \
    }()

template<typename Container>
inline set<int> column::isin(const Container& invalues, typename std::enable_if_t<is_iterable_container<Container>::value>*)
{
	unordered_set<Object> invalues_uset(invalues.begin(), invalues.end());
	set<int> idxs;
	const auto uset_end = invalues_uset.end();

	uint32_t i = 0;
	for (const Object& value : values) {
		if (invalues_uset.find(value) != uset_end) {
			idxs.insert(i);
		}
		++i;
	}
	return idxs;
}
