#include "map_reduce.h"
#include "mapper.h"
#include "reducer.h"
#include <thread>

using namespace mare_nostrum;

int main() {
    MapReduce obj;
    obj.setInputFiles("./data/text.txt");
    obj.setMaxSimultaneousWorkers(std::thread::hardware_concurrency());
    obj.setNumReducers(4);
    obj.setTmpDir("../tmp/");
    std::function<std::vector<std::pair<std::string, int>>(const std::string &)> mapper = Mapper();
    obj.setMapper(mapper);
    std::function<std::vector<std::pair<std::string, int>>
                    (const std::vector<std::pair<std::string, std::vector<int>>> &)> reducer = Reducer();
    obj.setReducer(reducer);
    obj.start();
//    return 0;
}
