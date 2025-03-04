#include "Tools.h"

Tools::Tools() {

}

vector<Object> Tools::gen_seq_obj(int from, int to)
{
	int size = to - from;
	vector<Object> tempVec(size);

	for (int i = 0; i < size; i++) tempVec[i] = Object(from++, Dtype::INT32);
	return tempVec;
}

vector<int> Tools::gen_seq_int(int from, int to)
{
	int i = 0;
	vector<int> tempVec(to - from);
	while (from!=to) tempVec[i++] = from++;
	return std::move(tempVec);
}

void Tools::printthis(vector<int> arr)
{
	cout << endl;
	for (int i = 0; i < arr.size(); i++)
	{
		cout << i << " : " << arr[i] << endl;
	}
	cout << endl;
}

void Tools::printthis(vector<Object> arr)
{
	cout << endl;
	for (int i = 0; i < arr.size(); i++)
	{
		cout << i << " : " << arr[i].get() << endl;
	}
	cout << endl;
}

void Tools::printSubArrayBoundris(SubArrayBoundris sap)
{
	cout << "{low: " << sap.low << ", high: " << sap.high << ", sum: " << sap.sum << "}\n";
}

double  Tools::speedup_ratio_percent(double old_time, double new_time) {

	return ((old_time - new_time) / old_time) * 100.0;
}

double Tools::timesFaster(double oldTime, double newTime) {
	if (oldTime <= 0 || newTime <= 0) {
		std::cerr << "Error: Speed values must be greater than zero." << std::endl;
		return 0.0;
	}
	return oldTime / newTime;
	;
}



int Tools::randimized_partition(vector<Object>& arr, int start, int end_)
{
	int random_index = start + rand() % (end_ - start +1);
	std::swap(arr[start], arr[random_index]);

	Object pivot = arr[start];

	int left = start + 1,
		right = end_;

	while (left <= right) {

		while (left <= right &&  arr[left] <= pivot) ++left;
		while (left <= right && arr[right] >= pivot) --right;

		if (left < right) {
			std::swap(arr[left], arr[right]);
		}

	}
	std::swap(arr[start], arr[right]);

	return right;
}
