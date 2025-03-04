#pragma once
#include <chrono>

#include "../ObjectDs/Object.h"
#include <unordered_set>

using namespace std;


struct SubArrayBoundris {
    int low, high;
    long double sum;
};

class Tools
{
public:
	Tools();

    static vector<Object> gen_seq_obj(int from, int to);
    static vector<int> gen_seq_int(int from, int to);
    static void printthis(vector<int> arr);
    static void printthis(vector<Object> arr);
    static void printSubArrayBoundris(SubArrayBoundris sap);

    template<typename F>
    static float timeIt(F&& func) {

        auto start = std::chrono::high_resolution_clock::now();

        func();

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        return (float)(duration.count());
    }
    static double speedup_ratio_percent(double old_time, double new_time);
     
    static double timesFaster(double oldTime, double newTime);
    

    static int randimized_partition(vector<Object>& arr, int start, int end);


    static vector<int> rand_vec_nums(int size, int max, int min, unsigned int seed=0, bool with_replacement=1)
    {
        vector<int> values(size);

        std::random_device rd;
        std::mt19937 gen(seed == 0 ? rd() : seed);

        std::uniform_int_distribution<int> dist(min, max);

        if (with_replacement) {
            for (int i = 0; i < size; i++)
            {
                values[i] = dist(gen);
            }
        }
        else {
            unordered_set<int> used_num;

            int i = 0;
            int rand_num;

            while (i < size) {
                rand_num = dist(gen);

                if (used_num.find(rand_num) == used_num.end()) {
                    values[i++] = rand_num;
                    used_num.insert(rand_num);
                }
            }
        }
        

        return values;
    }
   
};

