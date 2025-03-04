
#ifndef ENUMS
#define ENUMS

#include <iostream>
#include <string>
#include <unordered_map>
#include <typeinfo>


using namespace std;



enum class Dtype {

	NA,
	////////////////////

	INT8,
	INT16,
	INT32,
	INT64,
	FLOAT,
	DOUBLE,
	NUMBER,

	////////////////////

	DATE_YEAR,
	DATE_MONTH,
	DATE_DAY,
	DATE_ATTR,

	DATE,
	STRING
};


enum class SortAlgo {
	heap,
	insertion,
	bubble,
	selection,
	double_selection,
	merge,
	merge_p,
	merge_and_insertion,
	quick
};


std::ostream& operator<<(std::ostream& os, const Dtype& dtype); // Declaration


// Overload the << operator for SortAlgo
std::ostream& operator<<(std::ostream& os, const SortAlgo& algo);


// Declaration of the unordered map
extern const unordered_map<Dtype, string> dtypeMap;


enum class DateFormat {
	AUTO,
	YYYY_MM_DD,
	YYYY_DD_MM,
	DD_MM_YYYY,
	MM_DD_YYYY
};

std::ostream& operator<<(std::ostream& os, const DateFormat& dtype); // Declaration

enum StatFun {
	MEAN,
	MEDIAN,
	STD,
	MODE,
	SUM,
	COUNT
};
extern const unordered_map<StatFun, string> statfun_str;

#endif