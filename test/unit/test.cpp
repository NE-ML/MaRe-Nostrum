#include <thread>
#include <vector>
#include <string>
#include <filesystem>

#include <gtest/gtest.h>

#include "map_reduce_test.h"
#include "mapper.h"
#include "reducer.h"

using namespace mare_nostrum;

TEST(MapReduce, TestMapReduce) {
    MapReduce obj;
    obj.setInputFiles("./test/static/data.txt");
    obj.setMaxSimultaneousWorkers(std::thread::hardware_concurrency());
    obj.setNumReducers(4);
    obj.setTmpDir("./test/static/tmp/");
    std::function<std::vector<std::pair<std::string, int>>(const std::string &)> mapper = Mapper();
    obj.setMapper(mapper);
    std::function<std::vector<std::pair<std::string, int>>
            (const std::vector<std::pair<std::string, std::vector<int>>> &)> reducer = Reducer();
    obj.setReducer(reducer);
    obj.start();
}

TEST(MapReduce, TestMapper) {
    Mapper mapper;
    std::string str = "Serious inside else memory if six field live on traditional measure example sense peace economy travel work special total financial role\n";
    auto _mapper = mapper(str);
    std::string result = "serious"
    "inside"
    "else"
    "memory"
    "if"
    "six"
    "field"
    "live"
    "on"
    "traditional"
    "measure"
    "example"
    "sense"
    "peace"
    "economy"
    "travel"
    "work"
    "special"
    "total"
    "financial"
    "role";
    int i = 0;
    for (auto line: _mapper) {
        EXPECT_EQ(line.first, result[i++]);
        EXPECT_EQ(line.second, '1');
    }
}

TEST(MapReduce, TestReducer) {
    Reducer reducer;
    Mapper mapper;
    std::string str = "Serious inside else memory if six field live on traditional measure example sense peace economy travel work special total financial role\n";
    auto _mapper = mapper(str);
    auto _reducer = reducer(mapper);
    std::string result = "serious"
                         "inside"
                         "else"
                         "memory"
                         "if"
                         "six"
                         "field"
                         "live"
                         "on"
                         "traditional"
                         "measure"
                         "example"
                         "sense"
                         "peace"
                         "economy"
                         "travel"
                         "work"
                         "special"
                         "total"
                         "financial"
                         "role";
    int i = 0;
    for (auto line: _reducer) {
        EXPECT_EQ(line.first, result[i++]);
        EXPECT_EQ(line.second, '1');
    }
}