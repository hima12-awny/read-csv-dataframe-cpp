#include "pandascpp/dataFrameDs/dataFrame.h"

int main() {

    // Step 1: Create sales data with mixed types (numbers, strings, NA)
    dataFrame sales({
        {"ProductID", {1, 2, 3, 1, 2, 4, 3, 1}},
        {"Region"   , {"North", "South", "East", "North", "South", "West", "East", "North"}},
        {"Sales"    , {100.5, 200.0, 150.0, Object(), 300.0, 1000.0, 120.0, 150}} // NA and outlier
        });

    // Step 2: Create product details DataFrame
    dataFrame products({
        {"ProductID"  , {1, 2, 3, 4}},
        {"ProductName", {"Laptop", "Mouse", "Keyboard", "Monitor"}},
        {"UnitPrice"  , {500.0, 20.0, 50.0, 200.0}}
        });

    // Step 3: Clean sales data - Fill NA and remove outliers
    sales["Sales"].fillna(true); // Replace NA with mean by defaults in numbers.
    dataFrame cleanedSales = sales.drop_outliers({ "Sales" }, 2.0, true); // Remove outliers (e.g., 1000)
    dprint(cleanedSales);

    // Step 4: Group by Region and ProductName, calculate total sales
    dataFrame grouped = cleanedSales.groupBy({ "Region", "ProductID"}, "Sales", StatFun::SUM);
    dprint(grouped);

    // Step 5: Apply a parallel function to normalize sales by UnitPrice
    grouped["Sales"] = grouped["Sales"] / Object(10.0);
    dprint(grouped);

    // Step 6: Merge sales with product details
    dataFrame merged = grouped.merge(products, "ProductID", "inner");
    dprint(merged);

    // Step 7: Sort by Sales (descending) to find top performers
    dataFrame topPerformers = grouped.orderBy({ "Sales" }, { true });
    dprint(topPerformers);

    // Step 8: Filter top performers (Sales >= 15 after normalization)
    dataFrame filtered = topPerformers[topPerformers["Sales"] >= 15];
    dprint(filtered);

    // Step 9: Optimize memory and display results
    filtered.optimiz_mem(true);
    dprint(filtered);

    cout << "Top Performing Products by Region:";
    cout << filtered;

	return 0;
}