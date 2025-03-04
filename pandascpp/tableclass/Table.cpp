#include "Table.h"

Table::Table(vector<column> colsinput) {

	columnsobj = colsinput;
	for (column& col : columnsobj) {
		col.calc_max_str_val_len();
	}
}

void Table::printBorder() {

	cout << "+";
	for (column col : columnsobj)
		cout << " " << setfill('-') << setw(col.maxLenStr + 2) << " +";

	cout << "\n";
}

void Table::printHeader() {
	cout << "\n";
	printBorder();

	cout << "+";
	for (auto col : columnsobj) col.printAt(-1);

	cout << "\n";
	printBorder();
}

void Table::printRow(int idx) {

	cout << "|";
	for (auto col : columnsobj) {
		col.printAt(idx);
	}
	cout << "\n";
}

void Table::print(int from, int to) {

	int size = columnsobj[0].size;

	printHeader();

	if (to == -1 && from != -1) {
		printRow(from);
	}

	else if (from == -1 && to == -1)
		for (int idx = 0; idx < size; idx++)  printRow(idx);

	else
		for (int idx = from; idx < to; idx++) printRow(idx);

	printBorder();
	cout << "\n";
}