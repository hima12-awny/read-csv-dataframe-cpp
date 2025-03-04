#pragma once

#ifndef Object_CLS
#define Object_CLS

#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <random>
#include <functional> 

#include "StringDs/String.h"
#include "DateDs/Date.h"

#include "enums/enums.h"



using namespace std;

#pragma warning(disable : 4244)

// Base class for all types
class BaseValue {
public:
	virtual ~BaseValue() = default;
	virtual void print() const = 0;  // Pure virtual function for printing value
	virtual size_t size() const = 0; // Pure virtual function to get size of the value
};

// Templated derived class for storing values of different types
template<typename T>
class TypedValue : public BaseValue {
public:
	T value;

	TypedValue(T val) : value(val) {}

	void print() const override {
		std::cout << value << std::endl;
	}

	// Method to retrieve the stored value
	T& getValue() {
		return value;
	}

	// Method to get the size of the value
	size_t size() const override {
		return sizeof(value);
	}

	~TypedValue() {}
};



class Object
{

	pair<Dtype, Dtype> getTypeFromStr(const string& str);


	template<typename T>
	void setVal(const T& otherValue) {
		value = std::make_unique<TypedValue<T>>(otherValue);
	}

	void copyFrom(const Object& other);


	// Contact the arithmetic operation between this object and Other Object
	// based on the operation function like
	// plus operation  -> std::plus<>{}
	// minus operation -> std::minus<>{}
	template<typename operationFunc>
	Object arithmetic_operation(
		const Object& other, 
		operationFunc operation, 
		const char operation_char) const;

	inline void parseAndSetValue(
		const char invalue[], 
		bool parseInVal, 
		Dtype dtype = Dtype::NA);



public:

	std::unique_ptr<BaseValue> value; 

	Dtype type;
	Dtype secType;

	
	// Constructors ///////////////////////////////////////

	// default one, with NA dtype.
	Object();

	// the value and it's dtype, valid for all dtyps.
	Object(String invalue, Dtype intype);

	// the value and it's dtype, valid for only for numbers.
	Object(double invalue, Dtype intype);

	// the value, automatic extract it's type. valid for all dtype.
	Object(const char invalue[]);
	Object(string invalue);
	Object(String invalue);

	// number value, automatic number dtype extract if it int or double
	Object(double invalue);

	// copy constructor of Date dtype.
	Object(Date date);

	// build date constructor from numbers.
	Object(unsigned int year, unsigned char month, unsigned char day);

	// build date constructor from string and it's DateFormat.
	// if date format is Auto, it will detect it's format automatically 
	// from it's string and convert to it.
	Object(string datestr, DateFormat dateformat);
	Object(Object datestr, DateFormat dateformat);

	// copy constructor
	Object(const Object& invalue);

	///////////////////////////////////////////////////////


	// get methods ///////////////////////////////////////

	// get the value number if the Object is a number only.
	inline double value_num() const;

	// get the representing string of the Object value of the Object.
	inline string get() const;

	// get actual Object value with respect of it's type, to show it or edit it.
	template<typename T>
	T& get_val() const;

	// get actual pointer of value with respect of it's type, to show it or edit it.
	template<typename T>
	T* get_ptr() const;


	// get date attributes, valid for Date type only.
	Object get_date_year();
	Object get_date_month();
	Object get_date_day();


	///////////////////////////////////////////////////////


	// string methods ////////////////////////////////////
	// NOET:: This methods are Valid only for Objects that it's dtype = Dtype::STRING.

	// transform the string to ////////
	Object up   (); // make it upper;
	Object lw   (); // make it lower;
	Object title();	// make it in title format (first letter Uppercase and the rest in lowercase);


	Object stripl(); // strip left
	Object stripr(); // strip right
	Object strip (); // strip for all (left and right)

	// replace word/substring in the value to another word/substring.
	Object replace(string from, string to);
	///////////////////////////////////

	// search in string ///////////////

	// given Regx pattern, get the first string with this pattern if exists in the string. 
	Object extract_pattern(const string& pattern);

	// trying to find a target in string with respect to 
	// the position to start with, and if not found send -1.
	size_t find(string target, int start = 0);

	// check if this string target in string->(true) or not->(false).
	bool contains(string target);

	// count how many the target string in this string value.
	int count(string target);

	///////////////////////////////////
	
	// get substring of the string with start and end index. 
	Object substr(int start, int end=-1);

	// split the string with respect to the separator.
	// and can handle parsing numbers after spiting and put the results in vector of Objects.
	vector<Object> split(string sep="", bool enable_numbers = false);

	// get the length of the string.
	int len() const;
	///////////////////////////////////////////////////////


	// static generate random Objects /////////////////////

	// generate random number between the max and min limits.
	static Object rand_num(int max = 1000, int min = 0);

	// generate one or many random numbers.
	static vector<Object> rand_nums(int size, int max, int min = 0, unsigned int seed = 0);

	// generate random Date with random year and valid month and day.
	static Date rand_date();

	///////////////////////////////////////////////////////

	// converting Objects to another valid Objects Dtype //

	// general method to convert to any type.
	inline Object to_type(const Dtype& new_type, const DateFormat dateformat = DateFormat::AUTO);

	// convert all values of the vector to the new dtype;
	static void to_type(vector<Object>& values, Dtype new_type);

	// convert the Object value to String Representation
	// and make the dtype = Dtype::STRING
	// Example:
	// (float) 1.2 -> (String)"1.2"
	inline Object to_str();

	// convert only valid Objects(valid Strings or Anything but Date or NA value) to number int_x 
	Object to_int8 (); // 1 -byte
	Object to_int16(); // 2 -bytes
	Object to_int32(); // 4 -bytes
	Object to_int64(); // 16-bytes

	// same as above but convert them to floating point value.
	Object to_float (); // 4-bytes
	Object to_double(); // 8-bytes

	// convert the string or number to number to make an date attribute,
	// or get the attribute from the date. 
	Object to_date_year (); // to Dtype::DATE_YEAR
	Object to_date_month(); // to Dtype::DATE_MONTH
	Object to_date_day  (); // to Dtype::DATE_DAY

	// convert the string to date with respect to the date format,
	// and if the date format is AUTO the system should extract the format automatically/
	Object to_date(DateFormat dateformat= DateFormat::AUTO);


	// convert the Object to the most Suitable Format with respect of The Memory.
	Object optimize_mem();

	// same as above, 
	// but when with_mixed_type = false convert all values to the Max Dtype of the all items.
	// but when with_mixed_type = true convert each item with respect it's optimized dtype.
	static Dtype optimize_mem(vector<Object>& values, bool with_mixed_type = true);

	
	// convert value from string vector to Object(STING) vector
	static vector<Object> from_vector_string(vector<string>& values);

	// convert value from double vector to Object(DOUBLE) vector
	static vector<Object> from_vector_double(vector<double>& values);

	// convert value from int vector to Object(INT32) vector
	static vector<Object> from_vector_int(vector<int>& values);

	// convert value from string set to Object(STRING) vector
	static vector<Object> from_set_string(set<string>& values);

	///////////////////////////////////////////////////////


	// dealing with Object dtype //////////////////////////


	// get dtype in string format.
	static string type_str(Dtype type);

	// Extract the Input String dtype
	// Examples:
	// 1. "1.5"  -> Dtype::FLOAT
	// 2. "1"    -> Dtype::INT32
	// 3. "hima" -> Dtype::STRING
	static Dtype get_type(const std::string& str);

	// get the second dtype of the object
	// Examples:
	// 1. Dtype::INT32 -> Dtype::NUMBER
	// 2. Dtype::FLOAT -> Dtype::NUMBER
	// 3. Dtype::STRING -> Dtype::STRING
	static Dtype get_sec_type(Dtype type);

	// check if this Object is number.
	static bool is_num(Dtype type);
	bool is_num() const;

	// get the most (minimum in memory size) suitable dtype can hold the Object value.
	static inline Dtype get_optimized_type(const Object& invalue);

	// Same as Above but for all values of input vector, 
	// but get the max of Results of optimized items in the vector.
	static Dtype get_highest_type(vector<Object>& values);

	///////////////////////////////////////////////////////


	// memory size ////////////////////////////////////////

	// get pre-calculated memory size for each Object Dtype while in runtime.
	// that include the Object 
	// 1. value pointer
	// 2. actual value
	// 3. dtype (the real Date Type of the value like int16 or int8 or String)
	// 4. secType (general Data Type of the value like number or string or date or date_attrs)
	inline size_t get_dtype_mem_size(Dtype dtype) const;

	// call above method with this->dtype
	size_t get_mem_size() const;

	// get total memory size that vector take in runtime. 
	static size_t get_mem_size(vector<Object> values);

	///////////////////////////////////////////////////////


	// print methods /////////////////////////////////////

	// print the Object's value, dtype, size in runtime.
	void print_info() const;

	// print all Items Object's Info in the vector
	static void print_infos(vector<Object> values);

	// print the Object's value with respect to it's type.
	friend ostream& operator <<(ostream& os, const Object& obj);

	///////////////////////////////////////////////////////

	// Arithmetic Operators ////////////////////////////////

	Object operator +(const Object& other) const;
	Object operator -(const Object& other) const;
	Object operator /(const Object& other) const;
	Object operator *(const Object& other) const;

	Object operator +(const double& other) const;
	Object operator -(const double& other) const;
	Object operator /(const double& other) const;
	Object operator *(const double& other) const;

	void operator +=(const Object& other);
	void operator -=(const Object& other);
	void operator /=(const Object& other);
	void operator *=(const Object& other);

	void operator +=(const double& other);
	void operator -=(const double& other);
	void operator /=(const double& other);
	void operator *=(const double& other);

	///////////////////////////////////////////////////////


	// Comparisons Operators ///////////////////////////////


	// compare this object with another input one,
	// return -1 if this Object value is smaller than the another 
	// return  1 if this Object value is bigger  than the another 
	// return  0 if this Object value is equal   to   the another 
	inline int16_t compareTo(const Object& other) const;

	bool operator < (const Object& other) const;
	bool operator > (const Object& other) const;

	bool operator <=(const Object& other) const;
	bool operator >=(const Object& other) const;

	bool operator ==(const Object& other) const;
	bool operator !=(const Object& other) const;

	// Operator overloading for modulo
	Object operator%(const Object& other) const;

	///////////////////////////////////////////////////////


	Object& operator = (const Object& other);

};

#define HASH_VALUE(TYPE, obj) return std::hash<TYPE>()(obj.get_val<TYPE>())

namespace std {

	// hash value of the (Object) based on it's type
	template <>
	struct hash<Object> {
		std::size_t operator()(const Object& obj) const {
			if (obj.type == Dtype::NA) return 0;


			switch (obj.type) {
			case Dtype::STRING:
				return std::hash<std::string>()((obj.get_val<String>()).get());

			case Dtype::DATE: HASH_VALUE(Date, obj);

			case Dtype::DATE_DAY:
			case Dtype::DATE_MONTH:
			case Dtype::DATE_YEAR: HASH_VALUE(uint32_t, obj);

			case Dtype::INT8:   HASH_VALUE(int8_t , obj);
			case Dtype::INT16:  HASH_VALUE(int16_t, obj);
			case Dtype::INT32:  HASH_VALUE(int32_t, obj);
			case Dtype::INT64:  HASH_VALUE(int64_t, obj);
			case Dtype::FLOAT:  HASH_VALUE(float  , obj);
			case Dtype::DOUBLE: HASH_VALUE(double , obj);

			}

			return 0;
		}
	};

	// hash value of the (vector<Object>) based on it's type
	template <>
	struct hash<std::vector<Object>> {
		std::size_t operator()(const std::vector<Object>& vec) const {
			std::size_t seed = 0;
			for (const auto& obj : vec) {
				seed ^= std::hash<Object>()(obj) + 0x9e3779b9 + (seed << 1); // Mix seed
			}
			return seed;
		}
	};

	// hash value of the (vector<const Object*>) based on it's type
	template <>
	struct hash<std::vector<const Object*>> {
		std::size_t operator()(const std::vector<const Object*>& vec) const {
			std::size_t seed = 0;
			for (const auto& obj : vec) {
				seed ^= std::hash<Object>()(*obj) + 0x9e3779b9 + (seed << 1); // Mix seed
			}
			return seed;
		}
	};

}


#endif // !Object_CLS


struct ObjectPtrVectorEqual {
	bool operator()(const std::vector<const Object*>& lhs, const std::vector<const Object*>& rhs) const {
		if (lhs.size() != rhs.size()) return false;
		for (size_t i = 0; i < lhs.size(); ++i) {
			if (*lhs[i] != *rhs[i]) return false;
		}
		return true;
	}
};



#define ASSERT_TYPE_MISMATCH(ob1, ob2, op) \
    switch(op) { \
        case '+': \
        case '-': \
            /* Check type mismatch and secType mismatch for NUMBER types */ \
            if (ob1.type != ob2.type && \
                ob1.secType != ob2.secType && ob1.secType == Dtype::NUMBER) { \
                std::cout << "Calculation Error:: " << ob1.type << "(" << ob1 << ")" \
                          << " " #op " " << ob2.type << "(" << ob2 << ")" << std::endl; \
                assert(0); \
            } \
            break; \
        case '/': \
            /* Check for type mismatch OR invalid STRING/DATE division */ \
            if ((ob1.type != ob2.type && \
                 ob1.secType != ob2.secType && ob1.secType == Dtype::NUMBER) || \
                (ob1.type == Dtype::STRING && ob2.type == Dtype::STRING) || \
                (ob1.type == Dtype::DATE && ob2.type == Dtype::DATE)) { \
                std::cout << "Calculation Error:: can't handle / with incompatible types" << std::endl; \
                assert(0); \
            } \
            break; \
        case '*': { \
            /* Check if operation is valid for STRING/NUMBER combinations or NUMBER types */ \
            bool is_str_op = (ob1.secType == Dtype::STRING && ob2.secType == Dtype::NUMBER) || \
                             (ob1.secType == Dtype::NUMBER && ob2.secType == Dtype::STRING); \
            bool valid_operation = is_str_op || (ob1.secType == ob2.secType && ob1.secType == Dtype::NUMBER); \
            if (ob1.type != ob2.type && !valid_operation) { \
                std::cout << "Calculation Error:: invalid types for * operation" << std::endl; \
                assert(0); \
            } \
            break; \
        } \
        default: \
            std::cout << "Unsupported operation" << std::endl; \
            assert(0); \
    }

#define COMPARE_OBJECTS(OBJ1, OBJ2, TYPE1, TYPE2) \
{ \
    auto* value1 = &(OBJ1->get_val<TYPE1>()); \
    auto* value2 = &(OBJ2.get_val<TYPE2>()); \
	return *value1 < *value2? -1: *value1 > *value2? 1: 0; \
}


#define CONVERT_VALUES_TO_OBJECTS(values, targetType) \
    std::vector<Object> converted_items; \
    converted_items.reserve(values.size()); \
    for (const auto& value : values) { \
        converted_items.emplace_back(Object(value, targetType)); \
    } \
    return converted_items;
