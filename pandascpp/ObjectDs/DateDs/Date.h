#pragma once


#ifndef DATE_CLS
#define DATE_CLS

#include <iostream>
#include <string>
#include <cassert>
#include <sstream>
#include <functional>


#include "../enums/enums.h"

class Object;


using namespace std;

class Date
{
	unsigned int  year;
	unsigned char month;
	unsigned char day;

	char find_delimiter(string datestr);
	Date parseDateFromString(string dateStr, DateFormat dateFormat);
	vector<Object> validate_date_str(Object date_str);

public:

	Date();
	Date(const char date_str[]);
	Date(Object object);
	Date(string& datestr, DateFormat dateformat = DateFormat::AUTO);
	Date(const Object& datestr, DateFormat dateformat = DateFormat::AUTO);
	Date(unsigned int year, unsigned char month, unsigned char day);


	static Date now();

	Date copy();

	Object get_year()  const;
	Object get_month() const;
	Object get_day()   const;

	uint32_t get_attr_num(Dtype attr_type);
	Object get_attr(Dtype attr_type);

	unsigned int get_year_num  ();
	unsigned char get_month_num();
	unsigned char get_day_num  ();

	string get() const;

	DateFormat extractDateFormate(Object date_str);


	bool operator < (const Date& other) const;
	bool operator > (const Date& other) const;

	bool operator <= (const Date& other) const;
	bool operator >= (const Date& other) const;
	bool operator == (const Date& other) const;
	bool operator != (const Date& other) const;


	bool operator < (const Object& other) const;
	bool operator > (const Object& other) const;

	bool operator <= (const Object& other) const;
	bool operator >= (const Object& other) const;
	bool operator == (const Object& other) const;

	bool operator != (const Object& other) const;

	Date& operator = (const Date& otherDate);

	friend ostream& operator << (ostream& os, const Date& date) {
		os << date.get();
		return os;
	}


	struct Hash {

		std::size_t operator()(const Date& date) const {
			std::size_t hash_value = 0;

			// Use different prime constants for each field
			hash_value ^= std::hash<unsigned int>{}(date.day) + 0x9e3779b9 + (hash_value << 6) + (hash_value >> 2);
			hash_value ^= std::hash<unsigned int>{}(date.month) + 0x7f4a7c15 + (hash_value << 6) + (hash_value >> 2);
			hash_value ^= std::hash<int>{}(date.year) + 0x7fed7fed + (hash_value << 6) + (hash_value >> 2);

			return hash_value;
		}
	};
};


namespace std {
	template <>
	struct hash<Date> {
		std::size_t operator()(const Date& obj) const {
			return Date::Hash{}(obj);
		}
	};
}


#endif // !DATE