#include <unistd.h>
#include <sys/mman.h>
#include <thread>
#include "map_reduce.h"

using namespace mare_nostrum;

// 1. Число мапперов задается размером входных данных
// 2. Число редьюсеров задается пользователем

int main() {
    MapReduce obj;
    obj.setInputFiles("../data/file.txt");
    obj.setMaxSimultaneousWorkers(std::thread::hardware_concurrency());
    obj.setTmpDir("/tmp/");
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
