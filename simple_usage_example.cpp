
// Simple Usage Example

#include "pandascpp/dataFrameDs/dataFrame.h"
#include <iostream>
int main()
{
    string FILE_PATH = "./data/housing.csv";

    dataFrame df(FILE_PATH);

    // this print the first 5 rows.
    df.phead();

    // this print the info about the dataframe.
    df.info();

    // this print the dataframe of description of the data.
    df.describe().p();
}