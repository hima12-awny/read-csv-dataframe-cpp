
#ifndef SORT_H
#define SORT_H

#include "../ObjectDs/Object.h"
#include <random>
#include <thread>
#include <tuple>
#include <map>
#include <cmath>



class RandomGenerator {
private:
	// Random number generator and distribution
	static std::random_device rd;
	static std::mt19937 gen;

public:
	// Static method to generate a random number between min and max
	static int getRandomNumber(int min, int max) {
		std::uniform_int_distribution<int> dis(min, max);
		return dis(gen);
	}
};



struct IndexedValue {
	int idx;
	Object* value;

	// Overload comparison operators for IndexValue
	bool operator< (const IndexedValue& other) const { return *value < *other.value; }
	bool operator> (const IndexedValue& other) const { return *value > *other.value; }

	bool operator==(const IndexedValue& other) const { return *value == *other.value; }
	bool operator!=(const IndexedValue& other) const { return *value != *other.value; }

	bool operator <=(const IndexedValue& other) const { return *value <= *other.value; }
	bool operator >=(const IndexedValue& other) const { return *value >= *other.value; }
};


class Sort
{

	static void indexed_values(vector<Object>& arr, vector<IndexedValue>& indexedValues);
	static void indexed_values(vector<Object>& arr);

	static void get_final_idx_values(
		vector<int>& final_ids,
		vector<Object>& final_arr
	);


	// helpers for Insertion
	static inline void insertion(int start, int end);

	// helpers for Selection
	static int  arg_min    (int start, int end);
	static void arg_min_max(int& start, int& end, int& min_index, int& max_index);

	// helpers for Heap
	static void heapify(int n, int i);


	static void merge(int start, int mid, int end);
	static void merge_sort(int start, int end);


	// helpers for Quick
	static int partitionV3(int start, int end);
	static int partitionV4(int start, int end);

	static void quick_sort(int start, int end);

	// helpers for Merge Insertion
	static void merge_insertion_sort(int start, int end);

	static vector<IndexedValue> _arr;


public:

	Sort();
	static vector<int> insertion(vector<Object>& arr, bool reverse = false);
	static vector<int> bubble	(vector<Object>& arr, bool reverse = false);
	static vector<int> selection(vector<Object>& arr, bool reverse = false);
	static vector<int> double_selection(vector<Object>& arr, bool reverse = false);

	static vector<int> heap	     (vector<Object>& arr, bool reverse = false);
	static vector<int> merge_sort(vector<Object>& arr, bool reverse = false);
	static vector<int> quick_sort(vector<Object>& arr, bool reverse = false);

	static vector<int> merge_insertion_sort(vector<Object>& arr, bool reverse = false);


	// Testing The User Sorting Algorithm
	static vector<Object> gen_random_nums(int n, int to = 10, int from = 0);
	static bool is_sorted(const vector<Object>& arr, bool reverse = false);
	static bool test_sort_algo(vector<Object> arr, SortAlgo algo, bool reverse, bool& is_passed, float& time_tooken);
	static void test(int arr_size, vector<SortAlgo> algos, bool make_times_pct);

};


#endif // SORT_H