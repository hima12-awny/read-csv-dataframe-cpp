
#include "pandascpp/dataFrameDs/dataFrame.h"
#include <iostream>
#include <iomanip> // For setw, setfill
#include <ctype.h>
#include <random>

using namespace std;

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

void printTestResult(const string& testName, bool passed, int& passedCount, int& totalCount) {
    const int WIDTH = 50;

    // ANSI color codes
    const string RED = "\033[1;31m";     // Bright Red
    const string GREEN = "\033[1;32m";   // Bright Green
    const string RESET = "\033[0m";      // Reset to default color

    cout << left << setw(WIDTH) << setfill('.') << testName << ": ";

    if (passed) {
        cout << GREEN << "PASSED" << RESET << endl;
        passedCount++;
    }
    else {
        cout << RED << "FAILED" << RESET << endl;
    }

    totalCount++;
}

// Helper function to print section headers
void printSectionHeader(const string& header) {
    cout << "\n" << string(60, '=') << "\n"
        << setw(60) << setfill(' ') << left << header << "\n"
        << string(60, '=') << endl;
}

// Helper function to print summary
void printSummary(int passedCount, int totalCount) {
    cout << string(60, '-') << "\n"
        << "Summary: " << passedCount << " / " << totalCount << " tests passed ("
        << fixed << setprecision(2) << (totalCount ? (passedCount * 100.0 / totalCount) : 0.0) << "%)\n"
        << string(60, '-') << endl;
}

// Test Functions (unchanged logic, just formatted output)
void testConstruction(int& passedCount, int& totalCount) {
    dataFrame df1;
    printTestResult("Empty Constructor", df1.size == 0 && df1.columns.empty(), passedCount, totalCount);

    unordered_map<string, column> data = { {"A", column("A", {1, 2, 3})}, {"B", column("B", {4, 5, 6})} };
    dataFrame df2(data);
    printTestResult("Map Constructor", df2.size == 3 && df2.columns.size() == 2 && df2["A"][0] == 1, passedCount, totalCount);

    vector<column> cols = { column("A", {1, 2, 3}), column("B", {4, 5, 6}) };
    dataFrame df3(cols);
    printTestResult("Vector Constructor", df3.size == 3 && df3["B"][2] == 6, passedCount, totalCount);
}

void testDataAccessing(int& passedCount, int& totalCount) {
    dataFrame df({ {"A", column("A", {1, 2, 3})}, {"B", column("B", {4, 5, 6})} });
    column& colA = df["A"];
    printTestResult("Column Access", colA[1] == 2, passedCount, totalCount);

    dataFrame subset = df[{"A"}];
    printTestResult("Subset by Column Names", subset.columns.size() == 1 && subset["A"][0] == 1, passedCount, totalCount);

    dataFrame idxSubset = df[{0, 2}];
    printTestResult("Subset by Indexes", idxSubset.size == 2 && idxSubset["B"][1] == 6, passedCount, totalCount);
}

void testGettersSetters(int& passedCount, int& totalCount) {
    dataFrame df({ {"A", column("A", {1, 2, 3})}, {"B", column("B", {4, 5, 6})} });
    dataFrame headDf = df.head(2);
    printTestResult("Head", headDf.size == 2 && headDf["A"][1] == 2, passedCount, totalCount);

    dataFrame tailDf = df.tail(2);
    printTestResult("Tail", tailDf.size == 2 && tailDf["B"][0] == 5, passedCount, totalCount);

    column newCol("C", { 7, 8, 9 });
    dataFrame newDf = df.add_col(newCol, false);
    printTestResult("Add Column", newDf.columns.size() == 3 && newDf["C"][2] == 9, passedCount, totalCount);

    auto record = df.iloc(1);
    printTestResult("iloc Record", record["A"] == 2 && record["B"] == 5, passedCount, totalCount);
}

void testInformation(int& passedCount, int& totalCount) {
    dataFrame df({ {"A", column("A", {1, 2, 3})}, {"B", column("B", {4, 5, 6})} });
    vector<int> shape = df.shape();
    printTestResult("Shape", shape[0] == 3 && shape[1] == 2, passedCount, totalCount);

    dataFrame desc = df.describe(Dtype::NUMBER);
    printTestResult("Describe", (desc["A"][desc["index"] == "mean"])[0] == 2.0, passedCount, totalCount);
}

void testBadData(int& passedCount, int& totalCount) {
    vector<Object> mixedData = { Object(1), Object(), Object(3) };
    dataFrame df({ {"A", column("A", mixedData)} });
    set<int> naIdxs = df.naidxs();
    printTestResult("NA Indexes", naIdxs.size() == 1 && naIdxs.count(1), passedCount, totalCount);

    df.drop_na();
    printTestResult("Drop NA", df.size == 2 && df["A"][1] == 3, passedCount, totalCount);
}

void testFiltration(int& passedCount, int& totalCount) {
    dataFrame df({ {"A", column("A", {1, 2, 3})}, {"B", column("B", {4, 5, 6})} });
    auto condition = [](const Object& obj) { return obj > 2; };
    dataFrame filtered = df.filter_by_condition({ "A" }, { condition });
    printTestResult("Filter by Condition", filtered.size == 1 && filtered["A"][0] == 3, passedCount, totalCount);
}

void testExploratory(int& passedCount, int& totalCount) {
    dataFrame df({ {"A", column("A", {1, 2, 2})}, {"B", column("B", {4, 5, 6})} });
    dataFrame grouped = df.groupBy({ "A" }, "B", StatFun::COUNT);
    printTestResult("Group By", grouped["B"][1] == 2, passedCount, totalCount);

    dataFrame ordered = df.orderBy({ "A" }, { true });
    printTestResult("Order By", ordered["A"][0] == 2, passedCount, totalCount);
}

// Advanced Tests
vector<Object> generateRandomNumbers(int size, int min, int max, unsigned int seed) {
    mt19937 gen(seed);
    uniform_int_distribution<> dis(min, max);
    vector<Object> data(size);
    for (int i = 0; i < size; ++i) data[i] = Object(dis(gen));
    return data;
}

vector<Object> generateRandomStrings(int size, unsigned int seed, int length = 5) {
    mt19937 gen(seed);
    uniform_int_distribution<> dis(0, 25);
    vector<Object> data(size);
    string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    for (int i = 0; i < size; ++i) {
        string s;
        for (int j = 0; j < length; ++j) s += alphabet[dis(gen)];
        data[i] = Object(s);
    }
    return data;
}

void testLargeConstructionAndOptimization(int& passedCount, int& totalCount) {
    const int SIZE = 10000;
    unordered_map<string, vector<Object>> input_data = {
        {"ID", generateRandomNumbers(SIZE, 1, 1000, 42)},
        {"Value", generateRandomNumbers(SIZE, 0, 100, 43)},
        {"Category", generateRandomStrings(SIZE, 44)}
    };
    dataFrame df(input_data);
    printTestResult("Large DataFrame Construction", df.size == SIZE && df.columns.size() == 3, passedCount, totalCount);

    size_t beforeSize = df.get_mem_size();
    df.optimiz_mem(true);
    size_t afterSize = df.get_mem_size();
    printTestResult("Memory Optimization", afterSize <= beforeSize, passedCount, totalCount);
}

void testComplexMerge(int& passedCount, int& totalCount) {
    unordered_map<string, vector<Object>> sales_data = {
        {"ProductID", {1, 2, 3, 4}}, {"Sales", {100, 200, 150, 300}}, {"Region", {"North", "South", "East", "West"}}
    };
    dataFrame df1(sales_data);

    unordered_map<string, vector<Object>> product_data = {
        {"ProductID", {1, 2, 3, 5}}, {"Price", {10, 20, 15, 25}}, {"Category", {"Electronics", "Clothing", "Books", "Toys"}}
    };
    dataFrame df2(product_data);

    dataFrame innerMerged = df1.merge(df2, "ProductID", "inner");
    printTestResult("Inner Merge", innerMerged.size == 3 && innerMerged["Sales"][0] == 100 && innerMerged["Price"][0] == 10, passedCount, totalCount);

    dataFrame outerMerged = df1.merge(df2, "ProductID", "outer");
    printTestResult("Outer Merge", outerMerged.size == 5 && outerMerged["ProductID"][4] == 5 && outerMerged["Sales"][4].type == Dtype::NA, passedCount, totalCount);

    dataFrame leftMerged = df1.merge(df2, "ProductID", "left");
    printTestResult("Left Merge", leftMerged.size == 4 && leftMerged["Price"][3].type == Dtype::NA, passedCount, totalCount);
}

void testAdvancedGroupBy(int& passedCount, int& totalCount) {
    const int SIZE = 1000;
    unordered_map<string, vector<Object>> data = {
        {"Region", generateRandomStrings(SIZE, 45)}, {"Product", generateRandomStrings(SIZE, 46)}, {"Sales", generateRandomNumbers(SIZE, 10, 500, 47)}
    };
    dataFrame df(data);

    dataFrame grouped = df.groupBy({ "Region", "Product" }, "Sales", StatFun::COUNT);
    printTestResult("GroupBy Count", grouped.columns.size() == 3, passedCount, totalCount);

    dataFrame summed = df.groupBy({ "Region" }, "Sales", StatFun::SUM);
    Object totalSales = 0.0;
    for (int i = 0; i < df.size; ++i) totalSales += df["Sales"][i];
    Object groupedTotal = 0.0;
    for (int i = 0; i < summed.size; ++i) groupedTotal += summed["Sales"][i];
    printTestResult("GroupBy Sum Consistency", abs((totalSales - groupedTotal).value_num()) < 1e-6, passedCount, totalCount);
}

void testMultiColumnOrderBy(int& passedCount, int& totalCount) {
    unordered_map<string, vector<Object>> data = {
        {"Name", {"Alice", "Bob", "Alice", "Charlie"}}, {"Age", {25, 30, 22, 28}}, {"Score", {95.5, 88.0, 92.0, 85.5}}
    };
    dataFrame df(data);

    dataFrame ordered = df.orderBy({ "Name", "Age" }, { false, true });
    printTestResult("Multi-Column OrderBy", ordered["Name"][0] == Object("Alice") && ordered["Age"][0] == 25 && ordered["Age"][1] == 22, passedCount, totalCount);

    dataFrame scoreOrdered = df.orderBy({ "Score" }, { true });
    printTestResult("Single Column OrderBy Desc", scoreOrdered["Score"][0] == 95.5 && scoreOrdered["Score"][3] == 85.5, passedCount, totalCount);
}

void testEdgeCases(int& passedCount, int& totalCount) {
    vector<Object> values = { 1, 2, Object(), 1000, 3 };
    dataFrame df({ {"Values", column("Values", values)} });

    dataFrame noNA = df.copy();
    noNA.drop_na();
    printTestResult("Drop NA", noNA.size == 4 && noNA["Values"][2] == 1000, passedCount, totalCount);

    dataFrame noOutliers = df.drop_outliers({ "Values" }, 2.0, false);
    printTestResult("Drop Outliers", noOutliers.size == 4 && noOutliers["Values"].max() == 3.0, passedCount, totalCount);
}

void testPerformanceApply(int& passedCount, int& totalCount) {
    const int SIZE = 5000;
    dataFrame df({ {"Numbers", column("Numbers", generateRandomNumbers(SIZE, 0, 100, 48))} });

    auto doubleFunc = [](const Object& obj) { return obj * 2; };
    map<const string, std::function<Object(const Object&)>> col_function = { {"Numbers", doubleFunc} };
    dataFrame applied = df.apply(col_function, false);
    printTestResult("Apply Function", applied["Numbers"][0] == (df["Numbers"][0] * 2), passedCount, totalCount);

    df.papply({ {"Numbers", doubleFunc} });
    printTestResult("Parallel Apply", df["Numbers"][0] == applied["Numbers"][0], passedCount, totalCount);
}


// Level 3 Advanced Tests
void testMassiveDataFrameScalability(int& passedCount, int& totalCount) {
    const int SIZE = 100000; // 100K rows
    unordered_map<string, vector<Object>> input_data = {
        {"UserID", generateRandomNumbers(SIZE, 1, 10000, 101)},
        {"Revenue", generateRandomNumbers(SIZE, 0.0, 1000.0, 102)},
        {"Dept", generateRandomStrings(SIZE, 103, 3)}
    };
    auto start = chrono::high_resolution_clock::now();
    dataFrame df(input_data);
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;

    bool passed = (df.size == SIZE && df.columns.size() == 3 && elapsed.count() < 5.0); // Expect creation < 5s
    printTestResult("Massive DataFrame Creation", passed, passedCount, totalCount);

    size_t memSize = df.get_mem_size();
    df.optimiz_mem(true);
    passed = (df.get_mem_size() <= memSize && df["Revenue"][SIZE - 1].type != Dtype::NA);
    printTestResult("Massive DataFrame Optimization", passed, passedCount, totalCount);
}

void testNestedGroupByMerge(int& passedCount, int& totalCount) {
    unordered_map<string, vector<Object>> sales = {
        {"StoreID", {1, 1, 2, 2, 3}}, {"Product", {"A", "B", "A", "C", "A"}}, {"Sales", {100, 200, 150, 300, 50}}
    };
    dataFrame dfSales(sales);

    unordered_map<string, vector<Object>> stores = {
        {"StoreID", {1, 2, 3}}, {"Location", {"NY", "CA", "TX"}}
    };
    dataFrame dfStores(stores);

    dataFrame grouped = dfSales.groupBy({ "StoreID" }, "Sales", StatFun::SUM);
    dataFrame merged = grouped.merge(dfStores, "StoreID", "inner");

    bool passed = (merged.size == 3 && merged["Sales"][0] == 300 && merged["Location"][0] == Object("NY"));
    printTestResult("Nested GroupBy and Merge", passed, passedCount, totalCount);

    dataFrame multiGrouped = dfSales.groupBy({ "StoreID", "Product" }, "Sales", StatFun::MEAN);
    passed = (multiGrouped["Sales"][multiGrouped["Product"] == "A"][0].value_num() == 100); // Mean of "A" in StoreID 1
    printTestResult("Multi-Column GroupBy Mean", passed, passedCount, totalCount);
}

void testComplexOrderByWithNA(int& passedCount, int& totalCount) {
    unordered_map<string, vector<Object>> data = {
        {"ID",    {1,           2,        3,    4,    5}},
        {"Score", {95.5, Object(),     88.0, 92.0, 85.5}}, // NA in Score
        {"Time",  {10  ,       20, Object(),   15,   25}}  // NA in Time
    };
    dataFrame df(data);

    dataFrame ordered = df.orderBy({ "Score", "Time" }, { true, false });
    bool passed = (ordered["Score"][0] == 95.5 && ordered["Score"][4].type == Dtype::NA);
    printTestResult("OrderBy with NA (Score Desc, Time Asc)", passed, passedCount, totalCount);

    dataFrame filteredOrdered = df.filter_by_condition({ "Score" }, { [](const Object& o) { return o > 90; } })
        .orderBy({ "Time" }, { true });

    passed = (filteredOrdered.size == 2 && filteredOrdered["Score"][0] == 92.0);
    printTestResult("Filter then OrderBy with NA", passed, passedCount, totalCount);
}

void testParallelApplyOnLargeData(int& passedCount, int& totalCount) {
    const int SIZE = 50000;
    dataFrame df({ {"Values", column("Values", generateRandomNumbers(SIZE, 0.0, 1000.0, 104))} });

    auto squareFunc = [](const Object& obj) { return obj * obj; };
    map<const string, std::function<Object(const Object&)>> colFunc = { {"Values", squareFunc} };

    auto startSerial = chrono::high_resolution_clock::now();
    dataFrame serialResult = df.apply(colFunc, false);
    auto endSerial = chrono::high_resolution_clock::now();
    chrono::duration<double> serialTime = endSerial - startSerial;

    auto startParallel = chrono::high_resolution_clock::now();
    df.papply(colFunc);
    auto endParallel = chrono::high_resolution_clock::now();
    chrono::duration<double> parallelTime = endParallel - startParallel;

    bool passed = (df["Values"][0] == serialResult["Values"][0] && parallelTime < serialTime * 1.5); // Parallel should be faster or close
    printTestResult("Parallel vs Serial Apply Consistency", passed, passedCount, totalCount);

    passed = (serialResult["Values"][SIZE - 1].value_num() > 0); // Ensure no data corruption
    printTestResult("Large Data Apply Integrity", passed, passedCount, totalCount);
}

void testOutlierDetectionAndRemoval(int& passedCount, int& totalCount) {
    vector<Object> data = { 1, 2, 3, 1000, 4, 5, Object(), 2000, 6 }; // Outliers: 1000, 2000
    dataFrame df({ {"Data", column("Data", data)} });

    set<int> outliers = df.outliers_idx({ "Data" }, 1);

    bool passed = (outliers.size() == 2 && outliers.count(3) && outliers.count(7));
    printTestResult("Outlier Detection", passed, passedCount, totalCount);

    dataFrame cleaned = df.drop_outliers({ "Data" }, 1, false);
    passed = (cleaned.size == 7 && cleaned["Data"].max() == 6.0);
    printTestResult("Outlier Removal with NA", passed, passedCount, totalCount);
}

void testMemoryIntensiveOperations(int& passedCount, int& totalCount) {

    const int SIZE = 20000;
    dataFrame df({ {"A", column("A", generateRandomNumbers(SIZE, 0, 100, 105))},
                   {"B", column("B", generateRandomStrings(SIZE, 106))} });

    dataFrame doubled = df.concat(df); // Double the size
    bool passed = (doubled.size == SIZE * 2 && doubled["A"][SIZE] == df["A"][0]);
    printTestResult("DataFrame Concatenation", passed, passedCount, totalCount);

    dataFrame shuffled = doubled.shuffle(107);
    passed = (shuffled.size == SIZE * 2 && shuffled["A"].unique_count() == df["A"].unique_count());
    printTestResult("Shuffle Large DataFrame", passed, passedCount, totalCount);
}



 //Main function with formatted output
int main() {
    int level1_passed = 0, level1_total = 0;
    int level2_passed = 0, level2_total = 0;
    int level3_passed = 0, level3_total = 0;


    printSectionHeader("Running Tests Level 1");
    testConstruction  (level1_passed, level1_total);
    testDataAccessing (level1_passed, level1_total);
    testGettersSetters(level1_passed, level1_total);
    testInformation   (level1_passed, level1_total);
    testBadData       (level1_passed, level1_total);
    testFiltration    (level1_passed, level1_total);
    testExploratory   (level1_passed, level1_total);
    printSummary      (level1_passed, level1_total);

    printSectionHeader("Running Tests Level 2");
    testLargeConstructionAndOptimization(level2_passed, level2_total);
    testComplexMerge                    (level2_passed, level2_total);
    testAdvancedGroupBy                 (level2_passed, level2_total);
    testMultiColumnOrderBy              (level2_passed, level2_total);
    testEdgeCases                       (level2_passed, level2_total);
    testPerformanceApply                (level2_passed, level2_total);
    printSummary                        (level2_passed, level2_total);


    printSectionHeader("Running Level 3 Advanced DataFrame Tests");
    testMassiveDataFrameScalability(level3_passed, level3_total);
    testNestedGroupByMerge          (level3_passed, level3_total);
    testComplexOrderByWithNA        (level3_passed, level3_total);
    testParallelApplyOnLargeData    (level3_passed, level3_total);
    testOutlierDetectionAndRemoval  (level3_passed, level3_total);
    testMemoryIntensiveOperations   (level3_passed, level3_total);
    printSummary                    (level3_passed, level3_total);

    printSectionHeader("Overall Test Results");
    printSummary(
        level1_passed + level2_passed + level3_passed, 
        level1_total  + level2_total  + level3_total);
    
    return 0;
}
