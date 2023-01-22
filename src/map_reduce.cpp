#include <iostream>
#include <vector>
#include <thread>
#include <filesystem>
#include <fstream>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include "map_reduce.h"

namespace mare_nostrum {
    void MapReduce::setInputFiles(const std::string &input_file) {
        input_file_ = input_file;
    }

    void MapReduce::setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers) {
        max_simultaneous_workers_ = max_simultaneous_workers;
    }

    void MapReduce::setNumReducers(std::size_t num_reducers) {
        num_reducers_ = num_reducers;
    }

    void MapReduce::setTmpDir(const std::string &tmp_dir = "/tmp") {
        tmp_dir_ = tmp_dir;
    }

    void MapReduce::setOutputDir(const std::string &output_dir) {
        output_dir_ = output_dir;
    }

    MapReduce::MapReduce() {

    }

    MapReduce::~MapReduce() {

    }
    //    Делает mmap входного файла, расчитывает сплиты (например, по 32МБ).
    //    (!) Сплит может проходить только через символ ‘\n’.
    //    Если “разрез” по размеру проходит не по ‘\n’,
    //    нужно захватить все до этого символа, соответственно,
    //    следующий сплит начать позже.
    void MapReduce::start() {
        if (!std::filesystem::create_directory(tmp_dir_)) {
            std::cerr << "Error creating temporary directory.\n";
        }
        auto file_size = std::filesystem::file_size(input_file_);

        int input_split = (int) (file_size / block_size);
        if (file_size % block_size != 0) {
            ++input_split;
        }

        std::vector<std::thread> threads(max_simultaneous_workers_);
        std::vector<int> mapper_status(max_simultaneous_workers_, FREE);
        int descriptor = open(input_file_.c_str(), O_RDONLY);

        for (size_t i = 0; i < input_split; ++i) {
            int index = GetFreeMapperIndex(mapper_status);

            mapper_status[index] = BUSY;

            threads[index] = std::thread(&MapReduce::Map, this, descriptor, index, i);

            threads[index].join();
        }
    }

    // Get split from file and pass it to Mapper
    void MapReduce::Map(const int descriptor, const int mapper_index, const int current_split) {
        std::string split("");
        off_t off = current_split * BLOCK_SIZE;
        off_t pa_off = off & ~(sysconf(_SC_PAGE_SIZE) - 1);
        char* src = (char*)mmap(NULL, BLOCK_SIZE + off - pa_off, PROT_READ, MAP_SHARED, descriptor, pa_off);
        std::string dst(src + off, BLOCK_SIZE);

        // if no '\n' at the end of block, write data until new sym == '\n'
        if (dst[BLOCK_SIZE - 1] != '\n') {
            int i = 0;
            do {
                dst += src[BLOCK_SIZE + off + i];
                ++i;
            } while (src[BLOCK_SIZE + off + i] != '\n');
        }
        std::vector<std::pair<std::string, int>> mapper_result = (*mapper_)(dst);

        std::cout << "_____" << std::endl;
        std::cout << "Mapper[" << mapper_index << "]" << std::endl;
        std::cout << dst << std::endl;
        for (int i = 0; i < mapper_result.size(); ++i) {
            std::cout << mapper_result[i].first << ": " << mapper_result[i].second << std::endl;
        }

        return;
    }

    int MapReduce::GetFreeMapperIndex(const std::vector<int> &mapper_status) {
        for (int i = 0; i < mapper_status.size(); ++i) {
            if (mapper_status[i] == FREE) {
                return i;
            }
        }
        return -1;
    }

    void MapReduce::setMapper(
            std::function<std::vector<std::pair<std::string, int>>(const std::string &)> &mapper) {
        mapper_ = &mapper;
    }
}  // namespace mare_nostrum
