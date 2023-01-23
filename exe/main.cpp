#include "map_reduce.h"
#include "mapper.h"
#include "reducer.h"
#include <thread>

using namespace mare_nostrum;

// 1. Число мапперов задается размером входных данных
// Каждый маппер должен подготовить сортировнный список ключей и значений,
// которые он предоставляет редьюсеру
// 2. Число редьюсеров задается пользователем
// надо склеить списки, предоставляемые редьюсеру

//Хадуп сам отвечает за пересылку данных с маппера на нужный редьюсер, группировку по ключу.
//В редьюсере нужно только написать логику преобразования списка значений.
int main() {
    MapReduce obj;
    obj.setInputFiles("../data/test.txt");
    obj.setMaxSimultaneousWorkers(std::thread::hardware_concurrency());
    obj.setNumReducers(4);
    obj.setTmpDir("/tmp/");
    std::function<std::vector<std::pair<std::string, int>>(const std::string &)> mapper = Mapper();
    obj.setMapper(mapper);
    std::function<std::vector<std::pair<std::string, int>>
                    (const std::vector<std::pair<std::string, std::vector<int>>> &)> reducer = Reducer();
    obj.setReducer(reducer);
    obj.start();
//    Job j;
//    j.setInputFiles(/* список файлов */);
//    j.setMaxSimultaneouslyWorkers(/* N, сколько потоков одновременно */);
//    j.setNumReducers(/* N */);
//    j.setTmpDir(/* Где джоба может размещать временные данные,
//                   по умлочанию - /tmp */);
//    j.setOutputDir(/* Куда выводить данные, не должна существовать */);
//
//    j.setMapper(/* функтор (string, string) -> vector<(string, string)> */);
//    j.setReducer(/* функтор (string, iterable<(string, string)>) ->
//                                       vector<(string, string) */);
//
//    j.start();
//
//    return 0;
}
