#include <thread>
#include <vector>
#include <filesystem>

#include <gtest/gtest.h>

#include "map_reduce.h"
#include "mapper.h"
#include "reducer.h"

using namespace mare_nostrum;

TEST(MapReduce, TestMapReduce) {
    MapReduce obj;
    obj.setInputFiles("../static/data.txt");
    obj.setMaxSimultaneousWorkers(std::thread::hardware_concurrency());
    obj.setNumReducers(4);
    obj.setTmpDir("../tmp/");
    std::function<std::vector<std::pair<std::string, int>>(const std::string &)> mapper = Mapper();
    obj.setMapper(mapper);
    std::function<std::vector<std::pair<std::string, int>>
            (const std::vector<std::pair<std::string, std::vector<int>>> &)> reducer = Reducer();
    obj.setReducer(reducer);
    obj.start();
}