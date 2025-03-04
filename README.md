# Pandascpp: A C++ DataFrame Library

![C++](https://img.shields.io/badge/C%2B%2B-17-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Profile-blue?logo=linkedin)](https://www.linkedin.com/in/ibrahim-awny/)
[![Gmail](https://img.shields.io/badge/Gmail-Email-red?logo=gmail)](mailto:hima12awny@gmail.com)



**Pandascpp** is a lightweight, high-performance C++ library that emulates the functionality of Python's Pandas library, 
providing two core data structures: `column` and `dataFrame`. This project aims to bring data manipulation and analysis capabilities to C++ developers,
with support for mixed data types with [Object](https://github.com/hima12-awny/cpp-dynamic-object) Data Structure, statistical operations, and parallel processing.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
  - [Column](#column)
  - [DataFrame](#dataframe)
- [Installation](#installation)
- [Usage](#usage)
  - [Column Examples](#column-examples)
  - [DataFrame Examples](#dataframe-examples)
- [API Documentation](#api-documentation)
  - [Column API](#column-api)
  - [DataFrame API](#dataframe-api)

## Overview

Pandascpp introduces two primary classes:
- **`column`**: A versatile, type-safe vector container for a single column of data, supporting mixed data types (numbers, strings, dates, for more details see [Object](https://github.com/hima12-awny/cpp-dynamic-object)) and operations like filtering, sorting, and statistical analysis.
- **`dataFrame`**: A 2D tabular structure built on `column` objects, offering Pandas-like functionality such as merging, grouping, ordering, and applying functions.

This library is tailored for C++ developers who need to manipulate and analyze data with the same ease and flexibility as Python's Pandas, all within a native C++ environment without requiring Python or additional external dependencies beyond the C++ Standard Library.
## Features

### Column
- **Mixed Data Types**: Store integers, doubles, strings, or dates in a single column using the `Object` wrapper.
- **Statistical Operations**: Compute mean, median, standard deviation, and more for numeric data.
- **String Manipulations**: Transform strings (e.g., to uppercase, strip whitespace) and extract patterns.
- **Outlier Detection**: Identify and remove outliers based on z-scores.
- **Sorting and Filtering**: Sort values and filter based on conditions.

See more in [column.h](https://github.com/hima12-awny/read-csv-dataframe-cpp/blob/master/pandascpp/columnDs/column.h)

### DataFrame
- **Tabular Structure**: Organize data in rows and columns, with named access to columns.
- **Data Access**: Subset by column names, indexes, or ranges; access rows via `iloc`.
- **Exploratory Analysis**: Group by columns, order by values, merge with other DataFrames, and compute correlations.
- **Memory Optimization**: Reduce memory usage with `optimiz_mem` automatically.
- **Parallel Processing**: Apply functions to columns in parallel using `papply`.

See more in [dataFrame.h](https://github.com/hima12-awny/read-csv-dataframe-cpp/blob/master/pandascpp/dataFrameDs/dataFrame.h)

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/hima12-awny/read-csv-dataframe-cpp
   cd read-csv-dataframe-cpp
   ```

2. **Prerequisites**:
   - C++17-compliant compiler (e.g., GCC 7+, MSVC 2017+)
   - Optional: OpenMP for parallel features in
      - open project Properties and go to C/C++ -> Langues -> Open MP support make it ```Yes```.  
      - Then in C/C++ -> Langue -> in Command Line write ```-openmp:llvm ```


## Column Examples

#### Basic Column Creation and Printing
```cpp
#include "pandascpp/columnDs/column.h"
#include <iostream>

int main() {
    column col("Numbers", {1, 2.5, Object(), 100});
    cout << "Column: ";
    cout << col; // Output: [1, 2.5, NA, 100]
    return 0;
}
```

#### Statistical Analysis
```cpp
#include "pandascpp/columnDs/column.h"
#include <iostream>

int main() {
    column col("Values", {10, 20, 30, 40, 50});
    cout << "Mean: "    << col.mean()   << "\n";   // Output: 30
    cout << "Median: "  << col.median() << "\n";   // Output: 30
    cout << "Std Dev: " << col.std()    << "\n";   // Output: ~15.81
    return 0;
}
```

#### String Manipulation
```cpp
#include "pandascpp/columnDs/column.h"
#include <iostream>

int main() {
    column col("Names", {" alice ", "BOB", "Charlie"});
    column upper = col.to_up();
    cout << upper; // Output: [ALICE, BOB, CHARLIE]
    
    column stripped = col.strip();
    cout << stripped;
    return 0;
}
```

#### Filtering and Sorting
```cpp
#include "pandascpp/columnDs/column.h"
#include <iostream>

int main() {
    column col("Scores", {85, 95, 60, 75, 90});
    column filtered = col[col > 80];
    cout << filtered; // Output: [85, 95, 90]

    column sorted = col.sortAndGetCol(true); // Descending
    cout << sorted; // Output: [95, 90, 85, 75, 60]
    return 0;
}
```

#### Outlier Removal
```cpp
#include "pandascpp/columnDs/column.h"
#include <iostream>

int main() {
    column col("Data", {1, 2, 3, 1000, 4});
    column cleaned = col.remove_outliers(2.0, false);
    cout << cleaned; // Output: [1, 2, 3, 4]
    return 0;
}
```

#### Handling Missing Data and Outliers
```cpp
#include "pandascpp/columnDs/column.h"
#include <iostream>

int main() {
    column col("Values", {1, Object(), 3, 1000, 5});
    col.dropna(true);
    col.p(); 

    column cleaned = col.remove_outliers(2.0, false);
    cleaned.p();
    return 0;
}
```
### Example Using User-Defined Sorting Algorithms with column
```cpp

#include "pandascpp/columnDs/column.h"
#include "pandascpp/sort_cls/Sort.h"
#include <iostream>

int main() {
    // Step 1: Create a column with sales data
    column sales("Sales", Object::rand_nums(10, 1000, 0, 0));

    // Step 2: Sort using different algorithms from Sort class
    std::cout << "Original Sales Data:\n";
    cout << sales << endl; 

    // Heap Sort (ascending)
    column heapSorted = sales.sortAndGetCol(false , SortAlgo::heap);
    heapSorted.set_name("HeapSorted");

    std::cout << "\nHeap Sort (Ascending):\n";
    cout << heapSorted << endl;

    // Quick Sort (ascending)
    column quickSorted = sales.sortAndGetCol(false, SortAlgo::quick);
    quickSorted.set_name("QuickSorted");
    std::cout << "\nQuick Sort (Descending):\n";
    cout << quickSorted << endl;

    // Merge Insertion Sort (ascending)
    column mergeInsertSorted = sales.sortAndGetCol(false, SortAlgo::merge_and_insertion);
    mergeInsertSorted.set_name("MergeInsertSorted");
    std::cout << "\nMergeInsertSorted Sort (Descending):\n";
    cout << mergeInsertSorted << endl;


    // Step 3: Test sorting efficiency with Sort::test
    std::cout << "\nTesting Sorting Algorithms Efficiency:\n";
    vector<SortAlgo> algos = { 
        SortAlgo::heap, 
        SortAlgo::quick, 
        SortAlgo::merge_and_insertion 
    };

    bool is_passed;
    float time_taken;
    for (SortAlgo algo : algos) {
        Sort::test_sort_algo(sales.values, algo, false, is_passed, time_taken);
    }

    return 0;
}
```


### DataFrame Examples

#### Creating and Displaying a DataFrame
```cpp
#include "pandascpp/dataFrameDs/dataFrame.h"
#include <iostream>

int main() {
    dataFrame df({
        {"ID", column("ID", {1, 2, 3})},
        {"Value", column("Value", {10.5, 20.0, 15.0})}
    });
    df.p(); // Output: Table with ID and Value columns
    return 0;
}
```

#### Subsetting and Accessing Data
```cpp
#include "pandascpp/dataFrameDs/dataFrame.h"
#include <iostream>

int main() {
    dataFrame df({
        {"Name", column("Name", {"Alice", "Bob", "Charlie"})},
        {"Age",  column("Age",  {25, 30, 28})}
    });
    dataFrame subset = df[{"Name"}];
    cout << subset; // Output: Table with only Name column

    auto row = df.iloc(1);
    std::cout << "Row 1: Name=" << row["Name"] << ", Age=" << row["Age"] << "\n";
    // Output: Row 1: Name=Bob, Age=30
    return 0;
}
```

#### Grouping and Aggregating
```cpp
#include "pandascpp/dataFrameDs/dataFrame.h"
#include <iostream>

int main() {
    dataFrame df({
        {"Category", column("Category", {"A", "B", "A", "B"})},
        {"Sales",    column("Sales",    {100, 200, 150, 300})}
    });
    dataFrame grouped = df.groupBy({"Category"}, "Sales", StatFun::SUM);
    cout << grouped; // Output: A: 250, B: 500
    return 0;
}
```

#### Merging DataFrames
```cpp
#include "pandascpp/dataFrameDs/dataFrame.h"
#include <iostream>

int main() {
    dataFrame df1({
        {"ID",    column("ID",    {1,  2,  3})},
        {"Value", column("Value", {10, 20, 30})}
    });

    dataFrame df2({
        {"ID",    column("ID",    {1, 2,  4})},
        {"Price", column("Price", {5, 10, 15})}
    });
    dataFrame merged = df1.merge(df2, "ID", "outer");
    cout << merged; // Output: Table with ID, Value, Price (includes NA)
    return 0;
}
```

#### Applying Functions in Parallel
```cpp
#include "pandascpp/dataFrameDs/dataFrame.h"
#include <iostream>

int main() {
    dataFrame df({
        {"Numbers", column("Numbers", {1, 2, 3, 4, 5})}
    });
    auto doubleFunc = [](const Object& o) { return o * 2; };
    df.papply({{"Numbers", doubleFunc}});
    cout << df;
    return 0;
}
```


## API Documentation

### Column API
| Method/Property | Description | Example |
|-----------------|----------------|---------|
| `column(name, values, mixed_type)` | Constructor with name and initial values | `column("A", {1, 2, 3})` |
| `operator[](int idx)` | Access value at index | `col[0]` |
| `mean()` | Compute mean of numeric values | `col.mean()` |
| `sortAndGetCol(bool reverse)` | Sort and return new column | `col.sortAndGetCol(true)` |
| `fillna(Object val, bool inplace)` | Fill NA values | `col.fillna(0, true)` |
| `apply(func)` | Apply function to all values | `col.apply([](const Object& o) { return o * 2; })` |
| `get_outliers_idxs(double m)` | Get indices of outliers | `col.get_outliers_idxs(2.0)` |

### DataFrame API
| Method/Property | Description | Example |
|-----------------|----------------|---------|
| `dataFrame(map<string, column>)` | Constructor from column map | `dataFrame({{"A", col1}, {"B", col2}})` |
| `operator[](string col)` | Access column by name | `df["A"]` |
| `iloc(int row)` | Get row as map | `df.iloc(1)` |
| `groupBy(by, target, func)` | Group by columns and aggregate | `df.groupBy({"A"}, "B", StatFun::COUNT)` |
| `merge(other, on, how)` | Merge with another DataFrame | `df.merge(df2, "ID", "inner")` |
| `orderBy(cols, reverse)` | Sort by columns | `df.orderBy({"A"}, {true})` |
| `papply(col_functions)` | Parallel apply to columns | `df.papply({{"A", func}})` |
| `optimiz_mem(bool mixed)` | Optimize memory usage | `df.optimiz_mem(true)` |

For full details, see more in [column.h](https://github.com/hima12-awny/read-csv-dataframe-cpp/blob/master/pandascpp/columnDs/column.h)
 and [dataFrame.h](https://github.com/hima12-awny/read-csv-dataframe-cpp/blob/master/pandascpp/dataFrameDs/dataFrame.h)


# Made by ***Ibrahim Awny*** Feel free to contact me.
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Profile-blue?logo=linkedin)](https://www.linkedin.com/in/ibrahim-awny/)
[![Gmail](https://img.shields.io/badge/Gmail-Email-red?logo=gmail)](mailto:hima12awny@gmail.com)


