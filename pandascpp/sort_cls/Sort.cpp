#include "Sort.h"


std::vector<IndexedValue> Sort::_arr; // Define the static member here

Sort::Sort(){}


void Sort::indexed_values(vector<Object>& arr, vector<IndexedValue>& indexedValues)
{
    int arr_size = (int)arr.size();
    indexedValues = vector<IndexedValue>(arr_size);

    for (int i = 0; i < arr_size; i++) {
        indexedValues[i] = { i, &arr[i] };
    }

    Sort::_arr = indexedValues;
}

void Sort::indexed_values(vector<Object>& arr)
{
    int arr_size = (int)arr.size();
    Sort::_arr = vector<IndexedValue>(arr_size);

    for (int i = 0; i < arr_size; i++) {
        Sort::_arr[i] = { i, &arr[i] };
    }
}


void Sort::get_final_idx_values(vector<int>& final_ids, vector<Object>& final_arr)
{
    final_ids.reserve(Sort::_arr.size());
    final_arr.reserve(Sort::_arr.size());

    for (const auto& indexed_value : Sort::_arr) {
        final_ids.emplace_back(indexed_value.idx);
        final_arr.emplace_back(*indexed_value.value);
    }
}


#define GET_FINAL_VALUES(arr, reverse)    \
    if(reverse) std::reverse(Sort::_arr.begin(), Sort::_arr.end()); \
    std::vector<int> final_idx;                          \
    std::vector<Object> final_arr;                       \
    get_final_idx_values(final_idx, final_arr); \
    arr = final_arr;                                     \
    return final_idx;

inline void Sort::insertion(int start, int end)
{

    IndexedValue key;
    int i;

    for (int j = 1; j <= end; j++)
    {

        key = Sort::_arr[j];

        i = j - 1;

        while (i >= start && Sort::_arr[i] > key) {

            Sort::_arr[i + 1] = Sort::_arr[i];
            --i;
        }

        Sort::_arr[++i] = key;
    }
}


int Sort::arg_min(int start, int end)
{
    int min_idx = start;

    for (int i = start + 1; i < end; i++) {
        if (Sort::_arr[i] < Sort::_arr[min_idx]) {
            min_idx = i;
        }
    }

    return min_idx;
}

void Sort::arg_min_max(int& start, int& end, int& min_index, int& max_index)
{

    min_index = start;
    max_index = start;

    auto min_val = Sort::_arr[start];
    auto max_val = Sort::_arr[start];

    for (int i = start + 1; i <= end; ++i) {
        auto temp = Sort::_arr[i];

        if (temp < min_val) {
            min_val = temp;
            min_index = i;
        }

        else if (temp > max_val) {
            max_val = temp;
            max_index = i;
        }
    }
}


void Sort::heapify(
    int n,
    int i)
{
    int largest = i;
    int left = 2 * i + 1;
    int right = 2 * i + 2;

    if (left  < n && Sort::_arr[left]  > Sort::_arr[largest]) {
        largest = left;
    }

    if (right < n && Sort::_arr[right] > Sort::_arr[largest]) {
        largest = right;
    }

    if (largest != i) {

        std::swap(Sort::_arr[i], Sort::_arr[largest]);

        heapify(n, largest);
    }
}



void Sort::merge(
    int start, int mid, int end)
{

    int left_items_size = mid - start + 1;
    vector<IndexedValue> left_items(left_items_size);

    for (int i = 0; i < left_items_size; i++)
    {
        left_items[i] = Sort::_arr[start + i];
    }

    int left = 0;
    int right = mid + 1;
    int current_i = start;

    while (left < left_items_size && right <= end)
    {
        if (left_items[left] <= Sort::_arr[right])
        {
            Sort::_arr[current_i] = left_items[left];
            ++left;
        }
        else
        {
            Sort::_arr[current_i] = Sort::_arr[right];
            ++right;
        }
        ++current_i;
    }

    while (left < left_items_size)
    {
        Sort::_arr[current_i] = left_items[left];
        ++left;
        ++current_i;
    }

}



void Sort::merge_sort(
    int start, int end)
{
    if (start < end) {

        int mid = start + (end - start) / 2;

        merge_sort(start, mid);
        merge_sort(mid + 1, end);

        merge(start, mid, end);
    }

}


int Sort::partitionV3(int start, int end)
{

    IndexedValue pivot = Sort::_arr[start];

    int left = start + 1;
    int right = end;

    while (left <= right) {

        while (left <= right && Sort::_arr[left ] <= pivot) ++left;

        while (left <= right && Sort::_arr[right] >= pivot) --right;

        if (left < right) {
            std::swap(Sort::_arr[left], Sort::_arr[right]);
            left++;
            right--;
        }
    }

    std::swap(Sort::_arr[start], Sort::_arr[right]);
    return right;
}

int Sort::partitionV4(int start, int end)
{

    int random_pivot_index = start + rand() % (end - start + 1);
    std::swap(Sort::_arr[random_pivot_index], Sort::_arr[start]);

    return partitionV3(start, end);
}


void Sort::quick_sort(int start, int end)
{
    if (start >= end) return;

    int pivot_idx = partitionV4(start, end);

    quick_sort(start, pivot_idx - 1);
    quick_sort(pivot_idx + 1, end);
}


void Sort::merge_insertion_sort(int start, int end)
{
    if ((end - start) <= 30) {

        insertion(start, end);
    }
    else {

        int mid = start + (end - start) / 2;

        merge_insertion_sort(start, mid);
        merge_insertion_sort(mid + 1, end);

        merge(start, mid, end);

    }
}



vector<int> Sort::insertion (vector<Object>& arr, bool reverse)
{

    int size = (int)arr.size();

    indexed_values(arr);

    insertion(0, size - 1);

    GET_FINAL_VALUES(arr, reverse);
}

vector<int> Sort::bubble    (vector<Object>& arr, bool reverse)
{
    int size = (int)arr.size();
    indexed_values(arr);

    bool swapped;

    for (int i = 0; i < size; i++)
    {
        swapped = 0;

        for (int e = 0; e < size-1-i; e++)
        {
            if (Sort::_arr[e] > Sort::_arr[e+1]) {

                if(swapped == 0) swapped = 1;
                std::swap(Sort::_arr[e + 1], Sort::_arr[e]);
            }
        }
        if (swapped == 0) break;
    }

    GET_FINAL_VALUES(arr, reverse);
}

vector<int> Sort::selection (vector<Object>& arr, bool reverse)
{
    int size = (int)arr.size();

    indexed_values(arr);


    int arg_min_idx;

    for (int i = 0; i < size-1; i++)
    {
        arg_min_idx = arg_min(i+1, size);

        if (Sort::_arr[arg_min_idx] < Sort::_arr[i]) {

            std::swap(Sort::_arr[arg_min_idx], Sort::_arr[i]);
        }
    }

    GET_FINAL_VALUES(arr, reverse);
}


vector<int> Sort::double_selection(vector<Object>& arr, bool reverse)
{
    int size = static_cast<int>(arr.size());

    indexed_values(arr);  

    int lft = 0, rit = size - 1;
    int arg_min, arg_max;

    while (lft < rit) 
    {
        arg_min_max(lft, rit, arg_min, arg_max); 

        if (Sort::_arr[arg_min] < Sort::_arr[lft]) {
            std::swap(Sort::_arr[arg_min], Sort::_arr[lft]);
        }

        if (arg_max == lft) {
            arg_max = arg_min;
        }

        if (Sort::_arr[arg_max] > Sort::_arr[rit]) {
            std::swap(Sort::_arr[arg_max], Sort::_arr[rit]);
        }

        ++lft; 
        --rit; 
    }

   
    GET_FINAL_VALUES(arr, reverse);
}


vector<int> Sort::heap(vector<Object>& arr, bool reverse)
{
    int n = (int)arr.size();

    indexed_values(arr);

    for (int i = n / 2 + 1; i >= 0; --i)
    {
        heapify(n, i);
    }

    for (int i = n - 1; i > 0; --i)
    {
        std::swap(Sort::_arr[0], Sort::_arr[i]);

        heapify(i, 0);
    }

    GET_FINAL_VALUES(arr, reverse);

}

vector<int> Sort::merge_sort(vector<Object>& arr, bool reverse)
{
    int size = (int)arr.size();

    indexed_values(arr);

    merge_sort(0, size-1);

    GET_FINAL_VALUES(arr, reverse);
}

vector<int> Sort::quick_sort(vector<Object>& arr, bool reverse)
{
    int size = (int)arr.size();

    indexed_values(arr);

    quick_sort(0, size - 1);

    GET_FINAL_VALUES(arr, reverse);
}

vector<int> Sort::merge_insertion_sort(vector<Object>& arr, bool reverse)
{
    int size = (int)arr.size();

    indexed_values(arr);

    merge_insertion_sort(0, size - 1);

    GET_FINAL_VALUES(arr, reverse);
}


vector<Object> Sort::gen_random_nums(int n, int to, int from)
{

    vector<Object> arr(n);
    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<int> dis(from, to);
    for (int i = 0; i < n; i++)
    {
        arr[i] = Object(dis(gen));
    }
    return arr;
}

bool Sort::is_sorted(const vector<Object>& arr, bool reverse)
{
    int arr_size = (int)arr.size();

    for(int i = 1; i < arr_size; i++) {

        if ( (reverse && arr[i - 1] < arr[i]) || 
            (!reverse && arr[i - 1] > arr[i])) return false;
    }

    return true;
}

bool Sort::test_sort_algo(vector<Object> arr, SortAlgo algo, bool reverse, bool& is_passed, float& time_tooken)
{
    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();

    // Run the selected algorithm with timing and verbose output
    switch (algo) {
    case SortAlgo::heap:
        std::cout << "Running Heap Sort... ";
        heap(arr, reverse);
        break;
    case SortAlgo::insertion:
        std::cout << "Running Insertion Sort... ";
        insertion(arr, reverse);
        break;
    case SortAlgo::bubble:
        std::cout << "Running Bubble Sort... ";
        bubble(arr, reverse);
        break;
    case SortAlgo::selection:
        std::cout << "Running Selection Sort... ";
        selection(arr, reverse);
        break;
    case SortAlgo::double_selection:
        std::cout << "Running Double Selection Sort... ";
        double_selection(arr, reverse);
        break;
    case SortAlgo::merge:
        std::cout << "Running Merge Sort... ";
        merge_sort(arr, reverse);
        break;
    case SortAlgo::merge_and_insertion:
        std::cout << "Running Merge-Insertion Sort... ";
        merge_insertion_sort(arr, reverse);
        break;
    case SortAlgo::quick:
        std::cout << "Running Quick Sort... ";
        quick_sort(arr, reverse);
        break;
    default:
        std::cout << "This Algorithm Does Not Exist\n";
        assert(0);
        return false;
    }

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    // ANSI escape codes for colors
    const string red   = "\033[31m";
    const string green = "\033[32m";
    const string reset = "\033[0m";

    // Check if sorted and print the result
    is_passed = is_sorted(arr, reverse);
    if (is_passed) {
        cout << green << "PASSED" << reset << " ";
    }
    else {
        cout << red << "FAILED" << reset << " ";
    }


    time_tooken = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    std::cout << " in " << time_tooken << " ms\n";
}

void Sort::test(int arr_size, vector<SortAlgo> algos, bool make_times_pct)
{
    if (algos.empty()) {
        cout << "Must Test On ALGO !!!!!!";
        assert(0);
    }

    vector<Object> arr = gen_random_nums(arr_size, 1000, -1000);

    int test_num = (int)algos.size();
    int test_passed = 0;

    float total_times = 0.0;
    map<SortAlgo, float> algos_times;

    
    for (size_t i = 0; i < test_num; i++)
    {
        cout << "Test Case (" << i + 1 << "): ";
        bool is_passed;
        float time_tooken;

        test_sort_algo(arr, algos[i], false, is_passed, time_tooken);

        if (is_passed) ++test_passed;

        algos_times[algos[i]] = time_tooken;
        total_times += time_tooken;
    }


    cout << "Score: " << std::round(test_passed / (float)(test_num) *100) << " %\n\n";

    if (make_times_pct) {

        for (auto& pair : algos_times) {
            pair.second /= total_times;
        }

        for (int i = 0; i < test_num; i++)
        {
            cout << "Test Case (" << i + 1 << ") time pct: " << round(algos_times[algos[i]] * 100) << " %\n";

        }
    }

}


// 610