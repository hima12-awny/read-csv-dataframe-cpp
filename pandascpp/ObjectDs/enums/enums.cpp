#include "enums.h"


// Definition and initialization of the unordered map
const unordered_map<Dtype, string> dtypeMap = {
	{Dtype::STRING, "STRING"},

	//////////////////////////////// Numbers
	{Dtype::NUMBER, "NUMBER"},

	{Dtype::INT8, "INT8"},
	{Dtype::INT16, "INT16"},
	{Dtype::INT32, "INT32"},
	{Dtype::INT64, "INT64"},

	{Dtype::FLOAT, "FLOAT"},
	{Dtype::DOUBLE, "DOUBLE"},
	////////////////////////////////


	{Dtype::DATE, "DATE"},

	{Dtype::DATE_ATTR, "DATE_ATTR"},
	{Dtype::DATE_YEAR, "DATE_YEAR"},
	{Dtype::DATE_MONTH, "DATE_MONTH"},
	{Dtype::DATE_DAY, "DATE_DAY"},

	{Dtype::NA, "NA"}
};

const unordered_map<StatFun, string> statfun_str = {
	{StatFun::MEAN, "MEAN"},
	{StatFun::MEDIAN, "MEAN"},
	{StatFun::STD, "STD"},
	{StatFun::COUNT, "COUNT"},
	{StatFun::MODE, "MODE"},
	{StatFun::SUM, "SUM"},
};




std::ostream& operator<<(std::ostream& os,const Dtype& dtype) {

	os << dtypeMap.at(dtype);

	return os;
}

std::ostream& operator<<(std::ostream& os, const SortAlgo& algo)
{
	switch (algo) {
	case SortAlgo::heap:
		os << "Heap Sort";
		break;
	case SortAlgo::insertion:
		os << "Insertion Sort";
		break;
	case SortAlgo::bubble:
		os << "Bubble Sort";
		break;
	case SortAlgo::selection:
		os << "Selection Sort";
		break;
	case SortAlgo::double_selection:
		os << "Double Selection Sort";
		break;
	case SortAlgo::merge:
		os << "Merge Sort";
		break;
	case SortAlgo::merge_p:
		os << "Parallel Merge Sort";
		break;
	case SortAlgo::merge_and_insertion:
		os << "Merge-Insertion Sort";
		break;
	case SortAlgo::quick:
		os << "Quick Sort";
		break;
	default:
		os << "Unknown Sort Algorithm";
		break;
	}
	return os;
}


	
std::ostream& operator<<(std::ostream& os, const DateFormat& dateFormate) {

	switch (dateFormate)
	{
	case DateFormat::YYYY_MM_DD:
		os << "YYYY_MM_DD";
		break;

	case DateFormat::YYYY_DD_MM:
		os << "YYYY_DD_MM";
		break;

	case DateFormat::DD_MM_YYYY:
		os << "DD_MM_YYYY";
		break;
	case DateFormat::MM_DD_YYYY:
		os << "MM_DD_YYYY";
		break;

	default:
		break;
	}
	return os;
}
