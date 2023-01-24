#include <thread>
#include <iostream>
#include <filesystem>
#include "map_reduce.h"
#include "mapper.h"
#include "reducer.h"

using namespace mare_nostrum;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " <input_file>" << std::endl;
        return 1;
    }
    MapReduce obj;

    try {
        obj.setInputFiles("data/" + std::string(argv[1]));
        obj.setMaxSimultaneousWorkers(std::thread::hardware_concurrency());
        obj.setNumReducers(4);
        obj.setTmpDir("../tmp/");
        std::function<std::vector<std::pair<std::string, int>>(const std::string &)> mapper = Mapper();
        obj.setMapper(mapper);
        std::function<std::vector<std::pair<std::string, int>>
                (const std::vector<std::pair<std::string, std::vector<int>>> &)> reducer = Reducer();
        obj.setReducer(reducer);
        obj.start();
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    return 0;
}
