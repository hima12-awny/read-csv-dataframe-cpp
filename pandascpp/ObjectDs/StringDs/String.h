#pragma once

#ifndef STRING_cls
#define STRING_cls

#include <string>
#include <iostream>
#include <cassert>
#include <sstream>
#include <cctype>
#include <regex>    
#include "../enums/enums.h"

class Object;

using namespace std;


class String {

	string value;

	vector<size_t> get_min_find(string value, vector<string> from, size_t start_pos);

public:

	String();
	String(string value);
	String(const char value[]);

	string get() const { return value; }

	String stripl();
	String stripr();
	String strip ();

	int len() const { return (int)value.length(); };

	int count(string target);

	String		   substr  (int start,     int end=-1);
	size_t         find    (string target, int start=0);
	bool		   contains(string target);
	String         replace (string from, string to);
	vector<Object> split   (string sep="", bool parsing_numbers=false);
	String static  join    (string sep, vector<Object> arr);

	String up();
	String lw();
	String title();

	Object extract_pattern(const std::string& pattern);

	static vector<Object> extract_pattern(
		const vector<Object>& values, 
		const string& pattern
	);

	String operator + (const String& other) const;
	String operator - (const String& other);
	String operator * (const float& other) const;

	String operator + (const float& other) const;
	String operator - (const float& other) const;
	String operator / (const float& other) const;

	String operator / (const String& other) const;
	String operator * (const String& other) const;


	void operator += (const String& other);
	void operator -= (const String& other);
	void operator *= (const float& other);

	// Comparison operators String
	bool operator < (const String& other) const;
	bool operator > (const String& other) const;
	bool operator <=(const String& other) const;
	bool operator >=(const String& other) const;
	bool operator ==(const String& other) const;
	bool operator !=(const String& other) const;


	String& operator=(const String& onther) {
		
		value = onther.value;
		return *this;
	}

	friend std::ostream& operator<<(std::ostream& os, const String& obj) {
		os << obj.value;
		return os;
	}

	// Hash function for Object
	struct Hash {
		std::size_t operator()(const String& str) const {
			std::size_t hash_value = 0;

			hash_value ^= std::hash<std::string>{}(str.value);

			return hash_value;
		}
	};

};

namespace std {
	template <>
	struct hash<String> {
		std::size_t operator()(const String& str) const {
			return String::Hash{}(str);
		}
	};
}

#endif // !STRING

