#pragma once
#include "../columnDs/column.h"
class Table
{

public:
	vector<column> columnsobj;
	Table(vector<column> cols);

	void printBorder();
	void printHeader();

	void printRow(int idx);

	void print(int from=-1, int to=-1);

};

