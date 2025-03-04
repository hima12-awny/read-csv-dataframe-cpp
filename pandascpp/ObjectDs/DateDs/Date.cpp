#include "Date.h"
#include "../object.h"
#include <chrono>
#include<iomanip>


char Date::find_delimiter(string datestr)
{
	for (char letter : datestr) {
		if (!std::isdigit(letter)) return letter;
	}
	return -1;
}
Date Date::parseDateFromString(string dateStr, DateFormat dateFormat)
{
	unsigned  int year_;
	unsigned char month_;
	unsigned char day_;

	std::istringstream iss(dateStr);

	char delimiter = find_delimiter(dateStr);
	string token;

	switch (dateFormat)
	{
	case DateFormat::YYYY_MM_DD:
	{
		getline(iss, token, delimiter);
		year_ = stoul(token);

		getline(iss, token, delimiter);
		month_ = static_cast<unsigned char>(stoul(token));

		getline(iss, token, delimiter);
		day_ = static_cast<unsigned char>(stoul(token));
		break;
	}
	case DateFormat::YYYY_DD_MM:
	{
		getline(iss, token, delimiter);
		year_ = stoul(token);

		getline(iss, token, delimiter);
		day_ = static_cast<unsigned char>(stoul(token));

		getline(iss, token, delimiter);
		month_ = static_cast<unsigned char>(stoul(token));
		break;
	}

	case DateFormat::DD_MM_YYYY:
	{
		getline(iss, token, delimiter);
		day_ = static_cast<unsigned char>(stoul(token));

		getline(iss, token, delimiter);
		month_ = static_cast<unsigned char>(stoul(token));

		getline(iss, token, delimiter);
		year_ = stoul(token);

		break;
	}
	case DateFormat::MM_DD_YYYY:
	{
		getline(iss, token, delimiter);
		month_ = static_cast<unsigned char>(stoul(token));

		getline(iss, token, delimiter);
		day_ = static_cast<unsigned char>(stoul(token));

		getline(iss, token, delimiter);
		year_ = stoul(token);
		break;
	}

	default:
		year_ = 0;
		month_ = 0;
		day_ = 0;
	}


	return Date(year_, month_, day_);
}
vector<Object> Date::validate_date_str(Object date_str)
{
	string str_ = date_str.get();

	char delimiter = find_delimiter(str_);

	if (delimiter == -1) {

		date_str.print_info();
		assert(0 && "Can't to Parse This String to Date");
	}

	int delimiter_counter = 0;
	int str_len = (int)str_.length();

	bool is_my_delimiter;

	for (size_t i = 0; i < str_len; i++)
	{
		is_my_delimiter = str_[i] == delimiter;

		if (is_my_delimiter) {
			if (delimiter_counter == 2) {

				cout << "Can't to Parse This String to Date " << endl <<
					"Because the first Detected delimiter(" << delimiter << ") " <<
					"appears more the 2 times" << endl;

				assert(0);
			}
			++delimiter_counter;
		}

		if (!std::isalnum(str_[i]) && !is_my_delimiter) {

			date_str.print_info();
			cout << "Can't to Parse This String to Date " << endl <<
				"Because there is (" << str_[i] << ") Not Number" <<
				" or Not First Detected delimiter(" << delimiter << ")" << endl;

			assert(0);
		}
	}


	vector<Object> attrs = date_str.split(string(1, delimiter), true);
	int above_31_counter = 0, above_12_counter = 0;

	for (size_t i = 0; i < 3; i++)
	{
		if (attrs[i].secType != Dtype::NUMBER) {

			date_str.print_info();
			cout << "Can't to Parse This String to Date " << endl <<
				"Because there is (" << attrs[i] << ") Not Number\n";

			assert(0);
		}
		if (attrs[i] > 31) {

			if (above_31_counter == 1) {

				date_str.print_info();
				cout << "Can't to Parse This String to Date " << endl <<
					"Because there is NUMBER(" << attrs[i] << ") " << 
					"that also bigger than 31 And Year That only Number can be.\n";

				assert(0);
			}
			++above_31_counter;
		}
		if (attrs[i] > 12) {

			if (above_12_counter == 2) {

				date_str.print_info();
				cout << "Can't to Parse This String to Date " << endl <<
					"Because there are Two Numbers "<<
					"that also bigger than 12 And Year and Month That only Number can be.\n";

				assert(0);
			}
			++above_12_counter;
		}
	}
	return attrs;
}

Date::Date() {
	year = 0;
	month = 0;
	day = 0;
}
Date::Date(const char date_str[])
{
	Object temp = Object(date_str, Dtype::STRING);
	*this = Date(temp, DateFormat::AUTO);
}
Date::Date(Object obj)
{
	if (obj.type == Dtype::DATE) {
		*this = obj.get_val<Date>().copy();
	}
	else {

		DateFormat extracted_format = Date::extractDateFormate(obj);
		*this = Date(obj, extracted_format);
	}

}

Date::Date(string& datestr, DateFormat dateformat)
{
	DateFormat extracted_format = dateformat == DateFormat::AUTO ? Date::extractDateFormate(Object(datestr)) : dateformat;

	*this = parseDateFromString(datestr, extracted_format);
}

Date::Date(const Object& datestr, DateFormat dateformat)
{
	DateFormat extracted_format = dateformat == DateFormat::AUTO ? Date::extractDateFormate(datestr) : dateformat;

	*this = parseDateFromString(datestr.get_val<String>().get(), extracted_format);
}

Date::Date(unsigned int year, unsigned char month, unsigned char day)
{
	this->year = year;
	this->month = month;
	this->day = day;
}



Object Date::get_year () const
{
	return Object(year, Dtype::DATE_YEAR);
}
Object Date::get_month() const
{
	return Object(month, Dtype::DATE_MONTH);
}
Object Date::get_day  () const
{
	return Object(day, Dtype::DATE_DAY);
}

uint32_t Date::get_attr_num(Dtype attr_type)
{
	switch (attr_type)
	{
	case Dtype::DATE_MONTH: return this->month;
	case Dtype::DATE_DAY: return this->day;
	case Dtype::DATE_YEAR: return this->year;
	default:
		return 0;
		break;
	}
}

Object Date::get_attr(Dtype attr_type)
{
	switch (attr_type)
	{
	case Dtype::DATE_MONTH: return Object(this->month, attr_type);
	case Dtype::DATE_DAY: return Object(this->day, attr_type);
	case Dtype::DATE_YEAR: return Object(this->year, attr_type);
	default:
		return Object();
		break;
	}
}

unsigned int  Date::get_year_num ()
{
	return year;
}
unsigned char Date::get_month_num()
{
	return month;

}
unsigned char Date::get_day_num  ()
{
	return day;
}


string Date::get() const
{
	string str_year  = to_string(year);
	string str_month = to_string(month);
	string str_day   = to_string(day);

	int len_y = (4 - (int)str_year.length());

	for (int i = 0; i < len_y; i++)
	{
		str_year = "0" + str_year;
	}

	if (str_month.length() == 1) {
		str_month = "0" + str_month;
	}

	if (str_day.length() == 1) {
		str_day = "0" + str_day;
	}

	return str_year + "-" + str_month + "-" + str_day;
}


DateFormat Date::extractDateFormate(Object date_str)
{
	vector<Object> attrs = validate_date_str(date_str);

	int8_t year_index = 0, month_index = 1;

	// get year
	for (int8_t i = 0; i < 3; i++)
	{
		if (attrs[i] > 31) {
			year_index = i;
			break;
		}
	}

	// get month
	for (int8_t i = 0; i < 3; i++)
	{
		if (attrs[i] <= 12) {
			month_index = i;
			break;
		}
	}

	if (year_index == 0 && month_index == 1)
		return DateFormat::YYYY_MM_DD;

	else if (year_index == 0 && month_index == 2)
		return DateFormat::YYYY_DD_MM;

	else if (year_index == 2 && month_index == 0)
		return DateFormat::MM_DD_YYYY;
	else
		return DateFormat::DD_MM_YYYY;
	
}

Date Date::now()
{
	time_t currentTime = time(NULL);
	tm localTime;
	localtime_s(&localTime, &currentTime);

	int currentY = localTime.tm_year + 1900;
	int currentM = localTime.tm_mon + 1;
	int currentD = localTime.tm_mday;

	return Date(currentY, currentM, currentD);
}

Date Date::copy()
{
	return *this;
}


bool Date::operator < (const Date& other)  const {

	if (year != other.year) return year < other.year;
	if (month != other.month) return month < other.month;
	return day < other.day;
}
bool Date::operator > (const Date& other)  const {

	if (year != other.year) return year > other.year;
	if (month != other.month) return month > other.month;
	return day > other.day;
}

bool Date::operator <= (const Date& other) const {

	if (year != other.year) return year <= other.year;
	if (month != other.month) return month <= other.month;
	return day <= other.day;
}
bool Date::operator >= (const Date& other) const {

	if (year != other.year) return year >= other.year;
	if (month != other.month) return month >= other.month;
	return day >= other.day;
}

bool Date::operator == (const Date& other) const {

	return year == other.year && month == other.month && day == other.day;
}
bool Date::operator != (const Date& other) const {

	return !(*this == other);
}


bool Date::operator < (const Object& other)  const
{

	switch (other.type)
	{
	case Dtype::DATE:		return *this < other.get_val<Date>();
	case Dtype::DATE_YEAR:  return year  < other.get_val<uint32_t>();
	case Dtype::DATE_MONTH: return month < other.get_val<uint32_t>();
	case Dtype::DATE_DAY:	return day	 < other.get_val<uint32_t>();

	default:

		string operation = " == ";
		cout << "Comparision Error:: " <<
			Dtype::DATE << "(" << *this << ")"
			+ operation
			<< other.type << "(" << other << ")" << endl;
		assert(0);
		return 0;
		break;
	}
	return 0;

}
bool Date::operator > (const Object& other)  const
{
	switch (other.type)
	{
	case Dtype::DATE:		return *this > other.get_val<Date>();
	case Dtype::DATE_YEAR:  return year  > other.get_val<uint32_t>();
	case Dtype::DATE_MONTH: return month > other.get_val<uint32_t>();
	case Dtype::DATE_DAY:	return day > other.get_val<uint32_t>();

	default:

		string operation = " > ";
		cout << "Comparision Error:: " <<
			Dtype::DATE << "(" << *this << ")"
			+ operation
			<< other.type << "(" << other << ")" << endl;
		assert(0);
		return 0;
		break;
	}
	return 0;

}

bool Date::operator <= (const Object& other)  const
{
	switch (other.type)
	{
	case Dtype::DATE:		return *this <= other.get_val<Date>();
	case Dtype::DATE_YEAR:  return year  <= other.get_val<uint32_t>();
	case Dtype::DATE_MONTH: return month <= other.get_val<uint32_t>();
	case Dtype::DATE_DAY:	return day <= other.get_val<uint32_t>();

	default:

		string operation = " <= ";
		cout << "Comparision Error:: " <<
			Dtype::DATE << "(" << *this << ")"
			+ operation
			<< other.type << "(" << other << ")" << endl;
		assert(0);
		break;
		return 0;

	}
	return 0;

}
bool Date::operator >= (const Object& other)  const
{
	switch (other.type)
	{
	case Dtype::DATE:		return *this >= other.get_val<Date>();
	case Dtype::DATE_YEAR:  return year  >= other.get_val<uint32_t>();
	case Dtype::DATE_MONTH: return month >= other.get_val<uint32_t>();
	case Dtype::DATE_DAY:	return day >= other.get_val<uint32_t>();

	default:


		string operation = " >= ";
		cout << "Comparision Error:: " <<
			Dtype::DATE << "(" << *this << ")"
			+ operation
			<< other.type << "(" << other << ")" << endl;

		assert(0);		
		break;
		return 0;

	}
	return 0;

}

bool Date::operator == (const Object& other)  const
{
	switch (other.type)
	{
	case Dtype::DATE:		return *this == other.get_val<Date>();
	case Dtype::DATE_YEAR:  return year  == other.get_val<uint32_t>();
	case Dtype::DATE_MONTH: return month == other.get_val<uint32_t>();
	case Dtype::DATE_DAY:	return day == other.get_val<uint32_t>();

	default:


		string operation = " == ";
		cout << "Comparision Error:: " <<
			Dtype::DATE << "(" << *this << ")"
			+ operation
			<< other.type << "(" << other << ")" << endl;
		assert(0);

		break;
		return 0;

	}
	return 0;

}

bool Date::operator!=(const Object& other) const
{
	switch (other.type)
	{
	case Dtype::DATE:		return *this != other.get_val<Date>();
	case Dtype::DATE_YEAR:  return year  != other.get_val<uint32_t>();
	case Dtype::DATE_MONTH: return month != other.get_val<uint32_t>();
	case Dtype::DATE_DAY:	return day != other.get_val<uint32_t>();

	default:


		string operation = " != ";
		cout << "Comparision Error:: " <<
			Dtype::DATE << "(" << *this << ")"
			+ operation
			<< other.type << "(" << other << ")" << endl;
		assert(0);

		break;
		return 0;

	}
	return 0;
}

Date& Date::operator = (const Date& other)
{
	if (this != &other) {
		day   = other.day;
		month = other.month;
		year  = other.year;
	}
	return *this;
}
