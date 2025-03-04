#include "object.h"



// Constructors ///////////////////////////////////////

Object::Object()
{
	value = nullptr;
	type = Dtype::NA;
	secType = Dtype::NA;
}


Object::Object(String invalue, Dtype intype)
{
	parseAndSetValue(invalue.get().c_str(), false, intype);
}

Object::Object(double invalue, Dtype intype)
{
	type = intype;
	secType = get_sec_type(intype);

	switch (type)
	{
	case Dtype::INT8:
	{
		setVal<int8_t>((int8_t)invalue);
		break;
	}
	case Dtype::INT16:
	{
		setVal<int16_t>((int16_t)invalue);

		break;
	}
	case Dtype::INT32:
	{
		setVal<int32_t>((int32_t)invalue);
		break;
	}
	case Dtype::INT64:
	{
		setVal<int64_t>((int64_t)invalue);
		break;
	}
	case Dtype::FLOAT:
	{
		setVal<float_t>((float_t)invalue);

		break;
	}
	case Dtype::DOUBLE:
	{
		setVal<double>(invalue);
		break;
	}
	case Dtype::STRING:
	{
		setVal<String>(std::to_string(invalue));
		break;
	}
	case Dtype::NA:
		value = NULL;
		break;
	default:
		if (secType == Dtype::DATE_ATTR) {

			setVal<uint32_t>((uint32_t)invalue);
		}
		else value = NULL;
		break;
	}
}

Object::Object(const char invalue[])
{
	parseAndSetValue(invalue, true);
}

Object::Object(string invalue)
{
	parseAndSetValue(invalue.c_str(), true);
}

Object::Object(String invalue)
{
	parseAndSetValue(invalue.get().c_str(), true);
}

Object::Object(double invalue)
{
	if (static_cast<int>(invalue) == invalue) {

		setVal<int32_t>(invalue);
		type = Dtype::INT32;
	}
	else {

		setVal<float>(invalue);
		type = Dtype::FLOAT;
	}
	secType = Dtype::NUMBER;
}

Object::Object(const Object& invalue)
{
	copyFrom(invalue);
}


Object::Object(Date date)
{
	setVal<Date>(date.copy());
	type = Dtype::DATE;
	secType = Dtype::DATE;
}

Object::Object(unsigned int year, unsigned char month, unsigned char day)
{
	*this = Object(Date(year, month, day));
}

Object::Object(string datestr, DateFormat dateformat)
{
	*this = Object(Date(datestr, dateformat));
}

Object::Object(Object datestr, DateFormat dateformat)
{
	*this = Object(Date(datestr, dateformat));
}

///////////////////////////////////////////////////////


pair<Dtype, Dtype> Object::getTypeFromStr(const string& str)
{
	/* detect if an alpha letter or
	 an not a digit and is not a "."
	 so the type is a string
	 else The type is a number.*/

	Dtype type_ = Object::get_type(str);

	Dtype secType = is_num(type_) ? Dtype::NUMBER : Dtype::STRING;

	return pair<Dtype, Dtype>(type_, secType);
}


inline void Object::parseAndSetValue(
	const char invalue[],
	bool parseInVal,
	Dtype dtype)
{
	auto types = parseInVal ? getTypeFromStr(invalue) : pair(dtype, get_sec_type(dtype));
	type = types.first;
	secType = types.second;

	// to parse it and put it in right place.
	switch (type)
	{

	case Dtype::INT8:
	{
		setVal<int8_t>(stoi(invalue));
		break;
	}
	case Dtype::INT16:
	{
		setVal<int16_t>(stoi(invalue));
		break;
	}
	case Dtype::INT32:
	{
		setVal<int32_t>(stoi(invalue));
		break;
	}
	case Dtype::INT64:
	{
		setVal<int64_t>(stol(invalue));
		break;
	}
	case Dtype::FLOAT:
	{
		setVal<float_t>(stof(invalue));
		break;
	}
	case Dtype::DOUBLE:
	{
		setVal<double>(stod(invalue));
		break;
	}
	case Dtype::STRING:
	{
		setVal<String>(invalue);
		break;
	}
	case Dtype::NA:
		value = NULL;
		break;
	default:

		if (secType == Dtype::DATE_ATTR) {

			setVal<uint32_t>(stoi(invalue));
		}
		else value = NULL;
		break;
	}
}


void Object::copyFrom(const Object& other)
{

	if (this == &other) return;

	type = other.type;
	secType = other.secType;

	switch (type) {

	case Dtype::INT8:
		setVal(other.get_val<int8_t>());
		break;

	case Dtype::INT16:
		setVal(other.get_val<int16_t>());

		break;

	case Dtype::INT32:
		setVal(other.get_val<int32_t>());
		break;

	case Dtype::INT64:
		setVal(other.get_val<int64_t>());
		break;


	case Dtype::FLOAT:
		setVal(other.get_val<float>());
		break;

	case Dtype::DOUBLE:
		setVal(other.get_val<double>());

		break;

	case Dtype::STRING:
		setVal(other.get_val<String>());

		break;

	case Dtype::DATE:
		setVal(other.get_val<Date>());
		break;

	case Dtype::DATE_YEAR:
	case Dtype::DATE_MONTH:
	case Dtype::DATE_DAY:
		setVal(other.get_val<uint32_t>());
		break;

	case Dtype::NA:
		value = nullptr;
		break;

	default:
		break;
	}
}


// get methods ///////////////////////////////////////

inline double Object::value_num() const
{

	if (secType == Dtype::NUMBER) {
		switch (type)
		{
		case Dtype::INT8:
			return (double)get_val<int8_t>();

		case Dtype::INT16:
			return (double)get_val<int16_t>();


		case Dtype::INT32:
			return (double)get_val<int32_t>();


		case Dtype::INT64:
			return (double)get_val<int64_t>();


		case Dtype::FLOAT:
			return (double)get_val<float>();


		case Dtype::DOUBLE:
			return (double)get_val<double>();

		}
	}

	cout << "ValueError:: You Are trying to get Value with Dtype Different Dtype the Object: " << type << endl;
	assert(0);

	return double();
}

inline string Object::get() const {

	if (value != nullptr) {

		switch (type) {

		case Dtype::INT8: {
			return to_string(get_val<int8_t>());
		}

		case Dtype::INT16: {
			return to_string(get_val<int16_t>());
		}

		case Dtype::INT32: {
			return to_string(get_val<int32_t>());
		}

		case Dtype::INT64: {
			return to_string(get_val<int64_t>());
		}

		case Dtype::FLOAT: {
			return to_string(get_val<float>());
		}

		case Dtype::DOUBLE: {
			return to_string(get_val<double>());
		}

		case Dtype::DATE: {
			return	(get_val<Date>()).get();
		}
		
		case Dtype::STRING: {
			return (get_val<String>()).get();
		}
			
		default:

			if (secType == Dtype::DATE_ATTR)
				return  to_string(get_val<uint32_t>());

			assert(0 && "The Value IS Not Can't be get");
			return "NULL";
			break;
		}
	}
	else {
		return "NULL";
	}
	return "NULL";
}


template<typename T>
inline T& Object::get_val() const
{
	if (auto* typedValue = dynamic_cast<TypedValue<T>*>(value.get())) {
		return typedValue->getValue();  // Use getValue() from TypedValue
	}
	else {
		//cout << "ERROR: Object(" << type << ")" << " and you trying to get " << typeid(T).name() << end;
		throw std::runtime_error("Type mismatch or value is not set.");
	}
}

template<typename T>
inline T* Object::get_ptr() const
{
	if (auto* typedValue = dynamic_cast<TypedValue<T>*>(value.get())) {

		return (T*)typedValue;  // Use getValue() from TypedValue
	}
	else {
		throw std::runtime_error("Type mismatch or value is not set.");
	}
}


Object Object::get_date_year()
{
	if (type == Dtype::DATE_YEAR) return *this;

	if (type == Dtype::DATE) {

		return get_val<Date>().get_year();
	}

	print_info();
	assert(false && "this Method (getYear) Valid only for DATE dtype.");

	return Object();
}

Object Object::get_date_month()
{
	if (type == Dtype::DATE_MONTH) return *this;

	if (type == Dtype::DATE) {

		return get_val<Date>().get_month();
	}

	print_info();
	assert(false && "this Method (getMonth) Valid only for DATE dtype.");
	return Object();
}

Object Object::get_date_day()
{
	if (type == Dtype::DATE_DAY) return *this;

	if (type == Dtype::DATE) {

		return get_val<Date>().get_day();
	}

	print_info();
	assert(false && "Method (getDay) Valid only for DATE dtype.");

	return Object();
}

///////////////////////////////////////////////////////


// string methods ////////////////////////////////////

Object Object::up()
{
	if (type == Dtype::STRING) {
		return Object(get_val<String>().up(), Dtype::STRING);
	}

	print_info();
	assert(false && "Method (up) Valid only for STRING dtype.");
	return Object();
}
Object Object::lw()
{
	if (type == Dtype::STRING) {
		return Object(get_val<String>().lw(), Dtype::STRING);
	}
	print_info();
	assert(false && "Method (lw) Valid only for STRING dtype.");
	return Object();
}
Object Object::title()
{
	if (type == Dtype::STRING) {
		return Object(get_val<String>().title(), Dtype::STRING);
	}

	print_info();
	assert(false && "Method (title) Valid only for STRING dtype.");
	return Object();
}


Object Object::stripl()
{
	if (type == Dtype::STRING) {
		return get_val<String>().stripl();
	}

	print_info();
	assert(false && "Method (stripl) Valid only for STRING dtype.");


	return Object();
}
Object Object::stripr()
{
	if (type == Dtype::STRING) {
		return get_val<String>().stripr();
	}

	print_info();
	assert(false && "Method (stripr) Valid only for STRING dtype.");
	return Object();
}
Object Object::strip()
{
	if (type == Dtype::STRING) {
		return get_val<String>().strip();
	}

	print_info();
	assert(false && "Method (strip) Valid only for STRING dtype.");
	return Object();
}


Object Object::replace(string from, string to)
{
	if (type == Dtype::STRING) {
		return Object(get_val<String>().replace(from, to), Dtype::STRING);
	}

	print_info();
	assert(false && "Method (replace) Valid only for STRING dtype.");
	return Object();
}

Object Object::extract_pattern(const string& pattern)
{
	if (type == Dtype::STRING) {
		return get_val<String>().extract_pattern(pattern);
	}

	print_info();
	assert(false && "Method (stripl) Valid only for STRING dtype.");

	return Object();
}


size_t Object::find(string target, int start)
{
	if (type == Dtype::STRING) {
		return get_val<String>().find(target, start);
	}

	print_info();
	assert(false && "Method (find) Valid only for STRING dtype.");
	return size_t();
}

bool Object::contains(string target)
{
	if (type == Dtype::STRING) {
		return get_val<String>().find(target) != -1;
	}

	print_info();
	assert(false && "Method (contains) Valid only for STRING dtype.");
	return false;
}

int Object::count(string target)
{
	if (type == Dtype::STRING) {
		return get_val<String>().count(target);
	}

	print_info();
	assert(false && "Method (count) Valid only for STRING dtype.");
	return 0;
}


Object Object::substr(int start, int end)
{
	if (type == Dtype::STRING) {
		return Object(get_val<String>().substr(start, end), Dtype::STRING);
	}

	print_info();
	assert(false && "Method (substr) Valid only for STRING dtype.");
	return Object();
}

vector<Object> Object::split(string sep, bool enable_numbers)
{
	if (type == Dtype::STRING) {
		return get_val<String>().split(sep, enable_numbers);
	}

	print_info();
	assert(false && "Method (split) Valid only for STRING dtype.");
	return vector<Object>();
}

int Object::len() const
{
	if (secType == Dtype::NUMBER) {

		std::ostringstream oss;
		oss << value_num();
		return (int)(oss.str().length());
	}

	return (int)(get().length());
}


///////////////////////////////////////////////////////


// static generate random Objects /////////////////////

Object Object::rand_num(int max, int min)
{
	std::random_device rd;
	std::mt19937 gen(rd());

	// Define a uniform distribution for integers in the range [min, max]
	std::uniform_int_distribution<int> dist(min, max);

	return Object((float)dist(gen));
}

vector<Object> Object::rand_nums(int size, int max, int min, unsigned int seed)
{
	vector<Object> values(size);

	std::random_device rd;
	std::mt19937 gen(seed == 0 ? rd() : seed);

	// Define a uniform distribution for integers in the range [min, max]
	std::uniform_int_distribution<int> dist(min, max);

	for (int i = 0; i < size; i++)
	{
		values[i] = dist(gen);
	}

	return values;
}

Date Object::rand_date()
{
	std::random_device rd;
	std::mt19937 gen(rd());

	// Define a uniform distribution for integers in the range [min, max]
	std::uniform_int_distribution<int> dist(0, 10000);


	int year = 1900 + dist(gen) % 125;  // Random year between 1900 and 2024
	int month = 1 + dist(gen) % 12;   // Random month between 1 and 12
	int day = 1 + dist(gen) % 31;   // Random day between 1 and 31 (not handling leap years)

	return Date(year, month, day);
}

///////////////////////////////////////////////////////

// converting Objects to another valid Objects Dtype //


inline Object Object::to_type(const Dtype& new_type, const DateFormat dateformat)
{
	
	if (new_type == this->type) return *this;

	if (new_type == Dtype::DATE) {

		assert(type == Dtype::STRING && "Cant Convert this object to Date");

		return Object(Date(*this, dateformat));
	}

	switch (type)
	{
	case Dtype::INT8:   return Object(get_val<int8_t>() , new_type);
	case Dtype::INT16:  return Object(get_val<int16_t>(), new_type);
	case Dtype::INT32:  return Object(get_val<int32_t>(), new_type);
	case Dtype::INT64:  return Object(get_val<int64_t>(), new_type);
	case Dtype::FLOAT:  return Object(get_val<float>()  , new_type);
	case Dtype::DOUBLE: return Object(get_val<double>() , new_type);
	case Dtype::DATE: {

		if (Object::get_sec_type(new_type) == Dtype::DATE_ATTR) {
			return get_val<Date>().get_attr(new_type);
		}
		break;
	}
	case Dtype::STRING: {

		try
		{
			return Object(stoi(get_val<String>().get()), new_type);
		}
		catch (const std::exception&)
		{
			print_info();
			cout << "Can't Converting The string to Dtype: " << new_type << endl;
			assert(false);
			return Object();
		}
		break;
	}
	case Dtype::NA: return Object();

	default:
		if (secType == Dtype::DATE_ATTR) {
			return Object(get_val<uint32_t>(), new_type);
		}
		break;
	}

	return Object();
}

void Object::to_type(vector<Object>& values, Dtype new_type)
{
	for (Object& obj : values) {
		obj = obj.to_type(new_type);
	}
}

inline Object Object::to_str()
{
	return to_type(Dtype::STRING);
}

Object Object::to_int8 ()
{
	return to_type(Dtype::INT8);
}
Object Object::to_int16()
{
	return to_type(Dtype::INT16);
}
Object Object::to_int32()
{
	return to_type(Dtype::INT32);
}
Object Object::to_int64()
{
	return to_type(Dtype::INT64);
}

Object Object::to_float ()
{
	return to_type(Dtype::FLOAT);
}
Object Object::to_double()
{
	return to_type(Dtype::DOUBLE);
}

Object Object::to_date_year ()
{
	return to_type(Dtype::DATE_YEAR);
}
Object Object::to_date_month()
{
	return to_type(Dtype::DATE_MONTH);
}
Object Object::to_date_day  ()
{
	return to_type(Dtype::DATE_DAY);
}

Object Object::to_date(DateFormat dateformat)
{
	return to_type(Dtype::DATE, dateformat);
}

Object Object::optimize_mem()
{
	return to_type(get_optimized_type(*this));
}

Dtype Object::optimize_mem(vector<Object>& values, bool with_mixed_type)
{
	Dtype highest_type = Dtype::NA;

	if (with_mixed_type) {

		for (Object& obj : values) {
			obj = obj.optimize_mem();
			if (highest_type < obj.type) {
				highest_type = obj.type;
			}
		}
	}
	else {
		highest_type = get_highest_type(values);
		to_type(values, highest_type);
	}

	return highest_type;
}

vector<Object> Object::from_vector_string(vector<string>& values)
{
	CONVERT_VALUES_TO_OBJECTS(values, Dtype::STRING);
}
vector<Object> Object::from_vector_double(vector<double>& values)
{
	CONVERT_VALUES_TO_OBJECTS(values, Dtype::DOUBLE);
}
vector<Object> Object::from_vector_int   (vector<int   >& values)
{
	CONVERT_VALUES_TO_OBJECTS(values, Dtype::INT32);
}
vector<Object> Object::from_set_string   (set   <string>& values)
{
	CONVERT_VALUES_TO_OBJECTS(values, Dtype::STRING);
}


///////////////////////////////////////////////////////


// dealing with Object dtype //////////////////////////


string Object::type_str(Dtype type)
{
	return dtypeMap.at(type);
}

Dtype Object::get_type(const std::string& str)
{
	if (str.empty()) {
		return Dtype::NA;
	}

	size_t i = 0;
	bool hasDecimalPoint = false;
	bool isNumber = true;  // Flag to determine if it's a valid number

	// Check for a leading '+' or '-'
	if (str[0] == '+' || str[0] == '-') i++;

	for (; i < str.size(); i++) {
		if (str[i] == '.') {
			// If there's more than one decimal point, it's invalid
			if (hasDecimalPoint) {
				isNumber = false;
				break;
			}
			hasDecimalPoint = true;  // Mark that we've found a decimal point
		}
		else if (!std::isdigit(str[i])) {
			// If a non-digit character is found (not a decimal point), it's invalid
			isNumber = false;
			break;
		}
	}

	if (!isNumber) {
		return Dtype::STRING;
	}
	else if (hasDecimalPoint) {
		return Dtype::FLOAT;
	}
	else {
		return Dtype::INT32;
	}
}

Dtype Object::get_sec_type(Dtype type)
{

	if (
		type == Dtype::INT8 ||
		type == Dtype::INT16 ||
		type == Dtype::INT32 ||
		type == Dtype::INT64 ||
		type == Dtype::FLOAT ||
		type == Dtype::DOUBLE
		) {
		return Dtype::NUMBER;
	}

	if (type == Dtype::STRING) {
		return Dtype::STRING;
	}

	if (type == Dtype::DATE) {
		return Dtype::DATE;
	}

	if (
		type == Dtype::DATE_DAY ||
		type == Dtype::DATE_YEAR ||
		type == Dtype::DATE_MONTH
		) {
		return Dtype::DATE_ATTR;
	}

	return Dtype::NA;
}


bool Object::is_num(Dtype type)
{
	if (
		type == Dtype::INT8 ||
		type == Dtype::INT16 ||
		type == Dtype::INT32 ||
		type == Dtype::INT64 ||

		type == Dtype::FLOAT ||
		type == Dtype::DOUBLE
		) {
		return true;
	}
	return false;
}

bool Object::is_num() const
{
	return is_num(type);
}


Dtype Object::get_optimized_type(const Object& invalue)
{
	if (invalue.type == Dtype::NA) return Dtype::NA;
	double temp_val;

	if (invalue.type == Dtype::STRING) {
		string temp_val_str = invalue.get_val<String>().get();
		Dtype temp_val_type = Object::get_type(temp_val_str);

		if (temp_val_type == Dtype::STRING ||
			get_sec_type(temp_val_type) != Dtype::NUMBER) return Dtype::STRING;

		temp_val = stod(temp_val_str);
	}

	else if (invalue.secType == Dtype::NUMBER) {
		temp_val = invalue.value_num();
	}
	else return invalue.type;

	bool is_int = (static_cast<int>(temp_val)) == temp_val;

	if (is_int)
	{
		if (temp_val >= INT8_MIN && temp_val <= INT8_MAX) {
			return Dtype::INT8;
		}
		else if (temp_val >= INT16_MIN && temp_val <= INT16_MAX) {
			return Dtype::INT16;
		}
		else if (temp_val >= INT32_MIN && temp_val <= INT32_MAX) {
			return  Dtype::INT32;
		}
		else {
			return Dtype::INT64;
		}
	}
	else {
		if (temp_val >= FLT_MIN && temp_val <= FLT_MAX) {
			return Dtype::FLOAT;
		}
		else {
			return Dtype::DOUBLE;
		}
	}
	return Dtype::NA;
}

Dtype Object::get_highest_type(vector<Object>& values)
{
	Dtype highest_type = Dtype::NA;
	for (size_t i = 0; i < values.size(); i++)
	{
		Dtype curr_type = Object::get_optimized_type(values[i]);
		if (highest_type < curr_type) {
			highest_type = curr_type;
		}
	}
	return highest_type;
}


///////////////////////////////////////////////////////


// memory size ////////////////////////////////////////

inline size_t Object::get_dtype_mem_size(Dtype dtype) const
{
	switch (dtype)
	{
	case Dtype::STRING: return 56;

	case Dtype::DATE:
	case Dtype::INT64:
	case Dtype::DOUBLE: return 24;

	case Dtype::FLOAT:
	case Dtype::INT32:
	case Dtype::DATE_YEAR:
	case Dtype::DATE_MONTH:
	case Dtype::DATE_DAY: return 20;

	case Dtype::INT16: return 18;

	case Dtype::INT8: return 17;

	case Dtype::NA: return 16;
	}
	return 0;
}

size_t Object::get_mem_size() const
{
	return get_dtype_mem_size(this->type);
}

size_t Object::get_mem_size(vector<Object> values)
{
	size_t total_size = sizeof(values);
	for (size_t i = 0; i < values.size(); i++)
	{
		total_size += values[i].get_mem_size();
	}

	return total_size;
}


///////////////////////////////////////////////////////


// print methods /////////////////////////////////////

void Object::print_info() const
{
	string str = get();
	if (str.length() > 30) {

		str = str.substr(0, 30) + "... (len:" + to_string(str.length()) + ")";
	}

	str = (this->type == Dtype::STRING ? "\"" + str + "\"" : str);

	cout << endl;
	cout << "Object {" << endl;
	cout << " value: " << str << endl;
	cout << " dtype: " << type << endl;
	cout << " sec dtype: " << secType<< endl;
	cout << " size : " << get_mem_size() << " B" << endl;
	cout << "}" << endl;
}

void Object::print_infos(vector<Object> values)
{
	for (Object& obj : values) obj.print_info();
}

ostream& operator<<(ostream& os, const Object& obj)
{
	switch (obj.type)
	{
	case Dtype::INT8:
	{
		os << obj.get_val<int8_t>(); break;
	}

	case Dtype::INT16:
	{
		os << obj.get_val<int16_t>(); break;
	}

	case Dtype::INT32:
	{
		os << obj.get_val<int32_t>(); break;
	}

	case Dtype::INT64:
	{
		os << obj.get_val<int64_t>(); break;
	}

	case Dtype::FLOAT:
	{
		os << obj.get_val<float>(); break;
	}

	case Dtype::DOUBLE:
	{
		os << obj.get_val<double>(); break;
	}

	case Dtype::DATE:
	{
		os << obj.get_val<Date>(); break;
	}

	case Dtype::STRING:
	{
		os << obj.get_val<String>().get(); break;
	}
	case Dtype::NA: {
		os << "NULL"; break;
	}

	default:
		if (obj.secType == Dtype::DATE_ATTR) {
			os << obj.get_val<uint32_t>();
		}
	}
	return os;
}

///////////////////////////////////////////////////////

// Arithmetic Operators ////////////////////////////////

template<typename Func>
inline Object Object::arithmetic_operation(
	const Object& other, 
	Func operation, 
	const char operation_char) const
{
	ASSERT_TYPE_MISMATCH((*this), other, operation_char);

	Dtype new_type = std::max(type, other.type);

	switch (type)
	{
	case Dtype::NA:
		return Object();
		break;
	case Dtype::STRING: {

		switch (other.type)
		{
		case Dtype::STRING:return Object(operation(get_val<String>(), other.get_val<String>()) ,new_type);

		case Dtype::INT8:  return Object(operation(get_val<String>(), (const float)other.get_val<int8_t> ()), new_type);
		case Dtype::INT16: return Object(operation(get_val<String>(), (const float)other.get_val<int16_t>()), new_type);
		case Dtype::INT32: return Object(operation(get_val<String>(), (const float)other.get_val<int32_t>()), new_type);
		case Dtype::INT64: return Object(operation(get_val<String>(), (const float)other.get_val<int64_t>()), new_type);
		case Dtype::FLOAT: return Object(operation(get_val<String>(), (const float)other.get_val<float>()  ), new_type);
		case Dtype::DOUBLE:return Object(operation(get_val<String>(), (const float)other.get_val<double>() ), new_type);
		}
		break;
	}
	case Dtype::INT8: {
		switch (other.type)
		{
		case Dtype::STRING:return Object(operation(other.get_val<String>(), (const float)get_val<int8_t>()), new_type);

		case Dtype::INT8:  return Object(operation(get_val<int8_t>(), other.get_val<int8_t> ()), new_type);
		case Dtype::INT16: return Object(operation(get_val<int8_t>(), other.get_val<int16_t>()), new_type);
		case Dtype::INT32: return Object(operation(get_val<int8_t>(), other.get_val<int32_t>()), new_type);
		case Dtype::INT64: return Object(operation(get_val<int8_t>(), other.get_val<int64_t>()), new_type);
		case Dtype::FLOAT: return Object(operation(get_val<int8_t>(), other.get_val<float>  ()), new_type);
		case Dtype::DOUBLE:return Object(operation(get_val<int8_t>(), other.get_val<double> ()), new_type);
		}
		break;
	}
	case Dtype::INT16: {
		switch (other.type)
		{
		case Dtype::STRING:return Object(operation(other.get_val<String>(), (const float)get_val<int16_t>()), new_type);

		case Dtype::INT8:  return Object(operation(get_val<int16_t>(), other.get_val<int8_t> ()), new_type);
		case Dtype::INT16: return Object(operation(get_val<int16_t>(), other.get_val<int16_t>()), new_type);
		case Dtype::INT32: return Object(operation(get_val<int16_t>(), other.get_val<int32_t>()), new_type);
		case Dtype::INT64: return Object(operation(get_val<int16_t>(), other.get_val<int64_t>()), new_type);
		case Dtype::FLOAT: return Object(operation(get_val<int16_t>(), other.get_val<float>  ()), new_type);
		case Dtype::DOUBLE:return Object(operation(get_val<int16_t>(), other.get_val<double> ()), new_type);
		}
		break;
	}

	case Dtype::INT32: {
		switch (other.type)
		{
		case Dtype::STRING:return Object(operation(other.get_val<String>(), (const float)get_val<int32_t>()), new_type);

		case Dtype::INT8:  return Object(operation(get_val<int32_t>(), other.get_val<int8_t> ()), new_type);
		case Dtype::INT16: return Object(operation(get_val<int32_t>(), other.get_val<int16_t>()), new_type);
		case Dtype::INT32: return Object(operation(get_val<int32_t>(), other.get_val<int32_t>()), new_type);
		case Dtype::INT64: return Object(operation(get_val<int32_t>(), other.get_val<int64_t>()), new_type);
		case Dtype::FLOAT: return Object(operation(get_val<int32_t>(), other.get_val<float>  ()), new_type);
		case Dtype::DOUBLE:return Object(operation(get_val<int32_t>(), other.get_val<double> ()), new_type);
		}
		break;
	}
	case Dtype::INT64: {
		switch (other.type)
		{
		case Dtype::STRING:return Object(operation(other.get_val<String>(), (const float)get_val<int64_t>()), new_type);

		case Dtype::INT8:  return Object(operation(get_val<int64_t>(), other.get_val<int8_t> ()), new_type);
		case Dtype::INT16: return Object(operation(get_val<int64_t>(), other.get_val<int16_t>()), new_type);
		case Dtype::INT32: return Object(operation(get_val<int64_t>(), other.get_val<int32_t>()), new_type);
		case Dtype::INT64: return Object(operation(get_val<int64_t>(), other.get_val<int64_t>()), new_type);
		case Dtype::FLOAT: return Object(operation(get_val<int64_t>(), other.get_val<float>  ()), new_type);
		case Dtype::DOUBLE:return Object(operation(get_val<int64_t>(), other.get_val<double> ()), new_type);
		}
		break;
	}
	case Dtype::FLOAT: {
		switch (other.type)
		{
		case Dtype::STRING:return Object(operation(other.get_val<String>(), get_val<float>()), new_type);

		case Dtype::INT8:  return Object(operation(get_val<float>(), other.get_val<int8_t> ()), new_type);
		case Dtype::INT16: return Object(operation(get_val<float>(), other.get_val<int16_t>()), new_type);
		case Dtype::INT32: return Object(operation(get_val<float>(), other.get_val<int32_t>()), new_type);
		case Dtype::INT64: return Object(operation(get_val<float>(), other.get_val<int64_t>()), new_type);
		case Dtype::FLOAT: return Object(operation(get_val<float>(), other.get_val<float>  ()), new_type);
		case Dtype::DOUBLE:return Object(operation(get_val<float>(), other.get_val<double> ()), new_type);
		}
		break;
	}
	case Dtype::DOUBLE: {
		switch (other.type)
		{
		case Dtype::STRING:return Object(operation(other.get_val<String>(), (const float)get_val<double>()), new_type);

		case Dtype::INT8:  return Object(operation(get_val<double>(), other.get_val<int8_t> ()), new_type);
		case Dtype::INT16: return Object(operation(get_val<double>(), other.get_val<int16_t>()), new_type);
		case Dtype::INT32: return Object(operation(get_val<double>(), other.get_val<int32_t>()), new_type);
		case Dtype::INT64: return Object(operation(get_val<double>(), other.get_val<int64_t>()), new_type);
		case Dtype::FLOAT: return Object(operation(get_val<double>(), other.get_val<float>  ()), new_type);
		case Dtype::DOUBLE:return Object(operation(get_val<double>(), other.get_val<double> ()), new_type);
		}
		break;
	}

	case Dtype::DATE:{
		assert(false && "The Date Object Not Handled");
		break;
	}
	default: {
		if (secType == Dtype::DATE_ATTR) {
			return  Object(operation(get_val<uint32_t>(), other.get_val<uint32_t>()), new_type);
		}
		break;
	}

	}
	return Object();
}


Object Object::operator +(const Object& other) const
{
	return arithmetic_operation(other, std::plus<>{}, '+');
}
Object Object::operator -(const Object& other) const
{
	return arithmetic_operation(other, std::minus<>{}, '-');
}
Object Object::operator /(const Object& other) const
{
	return arithmetic_operation(other, std::divides<>{}, '/');
}
Object Object::operator *(const Object& other) const
{
	return arithmetic_operation(other, std::multiplies<>{}, '*');
}

void Object::operator +=(const Object& other)
{
	*this = arithmetic_operation(other, std::plus<>{}, '+');
}
void Object::operator -=(const Object& other)
{
	*this = arithmetic_operation(other, std::minus<>{}, '-');
}
void Object::operator /=(const Object& other)
{
	*this = arithmetic_operation(other, std::divides<>{}, '/');
}
void Object::operator *=(const Object& other)
{
	*this = arithmetic_operation(other, std::multiplies<>{}, '*');
}


Object Object::operator +(const double& other) const
{
	return arithmetic_operation(Object(other, Dtype::DOUBLE), std::plus<>{}, '+');
}
Object Object::operator -(const double& other) const
{
	return arithmetic_operation(Object(other, Dtype::DOUBLE), std::minus<>{}, '-');

}
Object Object::operator /(const double& other) const
{
	return arithmetic_operation(Object(other, Dtype::DOUBLE), std::divides<>{}, '/');
}
Object Object::operator *(const double& other) const
{
	return arithmetic_operation(Object(other), std::multiplies<>{}, '*');
}


void Object::operator +=(const double& other)
{
	*this = arithmetic_operation(Object(other, Dtype::DOUBLE), std::plus<>{}, '+');
}
void Object::operator -=(const double& other)
{
	*this = arithmetic_operation(Object(other, Dtype::DOUBLE), std::minus<>{}, '-');
}
void Object::operator /=(const double& other)
{
	*this = arithmetic_operation(Object(other, Dtype::DOUBLE), std::divides<>{}, '/');
}
void Object::operator *=(const double& other) {
	*this = arithmetic_operation(Object(other, Dtype::DOUBLE), std::multiplies<>{}, '*');
}

///////////////////////////////////////////////////////


// Comparisons Operators ///////////////////////////////


inline int16_t Object::compareTo(const Object& other) const
{

	if      (this->type == Dtype::NA && other.type != Dtype::NA) return -1;
	else if (this->type != Dtype::NA && other.type == Dtype::NA) return  1;
	else if (this->type == Dtype::NA && other.type == Dtype::NA) return  0;

	bool date_case = (secType == Dtype::DATE_ATTR || secType == Dtype::DATE)
		&& (other.secType == Dtype::DATE_ATTR || other.secType == Dtype::DATE);

	bool valid_comparison = (secType == other.secType) || date_case;

	if (!valid_comparison) {

		cout << "Warning Comparing: "
			<< "Compare "
			<< type << "(" << *this << ")"
			<< " to "
			<< other.type << "(" << other << ")" << endl;
		assert(0);


		return 1;
	}


	switch (type)
	{
	case Dtype::STRING: COMPARE_OBJECTS(this, other, String, String);
	
	case Dtype::INT8: {
		switch (other.type)
		{
			case Dtype::INT8  : COMPARE_OBJECTS(this, other, int8_t, int8_t);
			case Dtype::INT16 : COMPARE_OBJECTS(this, other, int8_t, int16_t);
			case Dtype::INT32 : COMPARE_OBJECTS(this, other, int8_t, int32_t);
			case Dtype::INT64 : COMPARE_OBJECTS(this, other, int8_t, int64_t);
			case Dtype::FLOAT :	COMPARE_OBJECTS(this, other, int8_t, float);
			case Dtype::DOUBLE: COMPARE_OBJECTS(this, other, int8_t, double);
		}
		break;
	}
	case Dtype::INT16: {
		switch (other.type)
		{
		case Dtype::INT8:   COMPARE_OBJECTS(this, other, int16_t, int8_t);
		case Dtype::INT16:  COMPARE_OBJECTS(this, other, int16_t, int16_t);
		case Dtype::INT32:  COMPARE_OBJECTS(this, other, int16_t, int32_t);
		case Dtype::INT64:  COMPARE_OBJECTS(this, other, int16_t, int64_t);
		case Dtype::FLOAT:	COMPARE_OBJECTS(this, other, int16_t, float);
		case Dtype::DOUBLE: COMPARE_OBJECTS(this, other, int16_t, double);
		}
		break;
	}
	case Dtype::INT32: {
		switch (other.type)
		{
		case Dtype::INT8:   COMPARE_OBJECTS(this, other, int32_t, int8_t);
		case Dtype::INT16:  COMPARE_OBJECTS(this, other, int32_t, int16_t);
		case Dtype::INT32:  COMPARE_OBJECTS(this, other, int32_t, int32_t);
		case Dtype::INT64:  COMPARE_OBJECTS(this, other, int32_t, int64_t);
		case Dtype::FLOAT:	COMPARE_OBJECTS(this, other, int32_t, float);
		case Dtype::DOUBLE: COMPARE_OBJECTS(this, other, int32_t, double);
		}
		break;
	}
	case Dtype::INT64: {
		switch (other.type)
		{
		case Dtype::INT8:   COMPARE_OBJECTS(this, other, int64_t, int8_t);
		case Dtype::INT16:  COMPARE_OBJECTS(this, other, int64_t, int16_t);
		case Dtype::INT32:  COMPARE_OBJECTS(this, other, int64_t, int32_t);
		case Dtype::INT64:  COMPARE_OBJECTS(this, other, int64_t, int64_t);
		case Dtype::FLOAT:	COMPARE_OBJECTS(this, other, int64_t, float);
		case Dtype::DOUBLE: COMPARE_OBJECTS(this, other, int64_t, double);
		}
		break;
	}
	case Dtype::FLOAT: {
		switch (other.type)
		{
		case Dtype::INT8:   COMPARE_OBJECTS(this, other, float, int8_t);
		case Dtype::INT16:  COMPARE_OBJECTS(this, other, float, int16_t);
		case Dtype::INT32:  COMPARE_OBJECTS(this, other, float, int32_t);
		case Dtype::INT64:  COMPARE_OBJECTS(this, other, float, int64_t);
		case Dtype::FLOAT:	COMPARE_OBJECTS(this, other, float, float);
		case Dtype::DOUBLE: COMPARE_OBJECTS(this, other, float, double);
		}
		break;
	}
	case Dtype::DOUBLE: {
		switch (other.type)
		{
		case Dtype::INT8:   COMPARE_OBJECTS(this, other, double, int8_t);
		case Dtype::INT16:  COMPARE_OBJECTS(this, other, double, int16_t);
		case Dtype::INT32:  COMPARE_OBJECTS(this, other, double, int32_t);
		case Dtype::INT64:  COMPARE_OBJECTS(this, other, double, int64_t);
		case Dtype::FLOAT:	COMPARE_OBJECTS(this, other, double, float);
		case Dtype::DOUBLE: COMPARE_OBJECTS(this, other, double, double);
		}
		break;
	}
	case Dtype::DATE: {

		COMPARE_OBJECTS(this, other, Date, Date);
	}

	default:
		if (secType == Dtype::DATE_ATTR && other.secType == Dtype::DATE) {

			auto* thisvalue = &get_val<uint32_t>();
			uint32_t othervalue = other.get_val<Date>().get_attr_num(this->type);

			return *thisvalue < othervalue ? -1 : *thisvalue > othervalue ? 1 : 0;
		}
		if (secType == other.secType || secType == Dtype::DATE_ATTR) {

			COMPARE_OBJECTS(this, other, uint32_t, uint32_t);
		}
	};


	return 0;
}

bool Object::operator <  (const Object& other) const
{
	return compareTo(other) == -1;
}
bool Object::operator >  (const Object& other) const
{
	return compareTo(other) == 1;
}
bool Object::operator <= (const Object& other) const
{
	if (type == Dtype::NA) {
		return other.type == Dtype::NA;
	}
	else if (other.type == Dtype::NA) {
		return type == Dtype::NA;
	}

	int8_t compare_res = compareTo(other);
	return compare_res == 0 || compare_res == -1;
}
bool Object::operator >= (const Object& other) const
{

	if (type == Dtype::NA) {
		return other.type == Dtype::NA;
	}
	else if (other.type == Dtype::NA) {
		return type == Dtype::NA;
	}

	int8_t compare_res = compareTo(other);

	return compare_res == 0 || compare_res == 1;
}
bool Object::operator == (const Object& other) const
{
	return compareTo(other) == 0;
}
bool Object::operator != (const Object& other) const
{
	if (type == Dtype::NA) {
		return other.type != Dtype::NA;
	}
	else if (other.type == Dtype::NA) {
		return type != Dtype::NA;
	}

	return compareTo(other) != 0;
}

Object Object::operator%(const Object& other) const {
	// Handle NA cases
	if (type == Dtype::NA || other.type == Dtype::NA) {
		return Object(); // Return NA Object
	}

	// Check if types are numeric
	if (!this->is_num() || !other.is_num()) {
		throw std::invalid_argument("Modulo operation only supported for numeric types");
	}

	// Prevent division by zero
	if (other.value_num() == 0) {
		throw std::domain_error("Modulo by zero");
	}

	Object result;
	// Handle different numeric types
	switch (type) {
	case Dtype::INT8:
	case Dtype::INT16:
	case Dtype::INT32:
	case Dtype::INT64: {

		Object result;
		// Convert both operands to the larger type
		int64_t lhs = (int64_t)value_num();
		int64_t rhs = (int64_t)other.value_num();

		result.type = std::max(type, other.type);
		result.secType = Object::get_sec_type(result.type);

		switch (result.type) {
		case Dtype::INT8:   result.setVal(static_cast<int8_t>(lhs % rhs)); break;
		case Dtype::INT16:  result.setVal(static_cast<int16_t>(lhs % rhs)); break;
		case Dtype::INT32:  result.setVal(static_cast<int32_t>(lhs % rhs)); break;
		case Dtype::INT64:  result.setVal(lhs % rhs); break;
		default: break;
		}
		return result;
	}

	case Dtype::FLOAT:
	case Dtype::DOUBLE: {
		Object result;
		// Convert both operands to double for floating-point modulo
		double lhs = value_num();
		double rhs = other.value_num();

		result.type = std::max(type, other.type);
		result.secType = Object::get_sec_type(result.type);

		double mod = std::fmod(lhs, rhs);

		if (result.type == Dtype::FLOAT) {
			result.setVal(static_cast<float>(mod));
		}
		else {
			result.setVal(mod);
		}
		return result;
	}

	default:
		throw std::invalid_argument("Unsupported type for modulo operation");
	}

	return result;
}

///////////////////////////////////////////////////////


Object& Object::operator =(const Object& other)
{
	if (this->type != other.type) {
		copyFrom(other);
	}
	else {
		switch (type)
		{
		case Dtype::INT8:
			get_val<int8_t>() = other.get_val<int8_t>();
			break;

		case Dtype::INT16:
			get_val<int16_t>() = other.get_val<int16_t>();
			break;

		case Dtype::INT32:
			get_val<int32_t>() = other.get_val<int32_t>();
			break;

		case Dtype::INT64:
			get_val<int64_t>() = other.get_val<int64_t>();
			break;

		case Dtype::FLOAT:
			get_val<float>() = other.get_val<float>();
			break;

		case Dtype::DOUBLE:
			get_val<double>() = other.get_val<double>();
			break;

		case Dtype::DATE:
			get_val<Date>() = other.get_val<Date>();
			break;

		case Dtype::DATE_YEAR:
		case Dtype::DATE_MONTH:
		case Dtype::DATE_DAY:
			get_val<uint32_t>() = other.get_val<uint32_t>();
			break;

		case Dtype::STRING:
			get_val<String>() = other.get_val<String>();
			break;
		}
	}

	return *this;
}

// 1909
// 1923 -> +52%
// 4043
// 4537
// 4870