#include <iostream>
#include <vector>
#include <thread>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include "map_reduce.h"

namespace mare_nostrum {
    void MapReduce::setInputFiles(const std::string &input_file) {
        input_file_ = input_file;
        file_size_ = std::filesystem::file_size(input_file_);
    }

    void MapReduce::setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers) {
        max_simultaneous_workers_ = max_simultaneous_workers;
        mapper_status.resize(max_simultaneous_workers_, FREE);
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

        std::vector<std::thread> threads(max_simultaneous_workers_);

        int descriptor = open(input_file_.c_str(), O_RDONLY);
        int offset = 0;

        while (offset < file_size_) {
            std::string split = GetSplit(descriptor, offset);

            int index = GetFreeMapperIndex(mapper_status);
            if (mapper_status[index] == DONE) {
                threads[index].join();
            }

            mapper_status[index] = BUSY;
            threads[index] = std::thread(&MapReduce::Map, this, split, index);
        }

        for (int i = 0; i < max_simultaneous_workers_; ++i) {
            if (mapper_status[i] == BUSY || mapper_status[i] == DONE) {
                threads[i].join();
            }
        }

        threads.clear();
    }

    std::string MapReduce::GetSplit(const int descriptor, int &offset) {
//        off_t off = current_split * BLOCK_SIZE;
//        off_t off = BLOCK_SIZE;
//        off_t pa_off = off & ~(sysconf(_SC_PAGE_SIZE) - 1);
//        char* src = (char*)mmap(NULL, BLOCK_SIZE + off - pa_off, PROT_READ, MAP_SHARED, descriptor, pa_off);
        char* src = (char*) mmap(NULL, 1, PROT_READ, MAP_SHARED, descriptor, 0);
        if (offset + BLOCK_SIZE > file_size_) {
            std::string dst(src + offset, file_size_);
            offset += file_size_;
            return dst;
        }

        std::string dst(src + offset, BLOCK_SIZE);
        if (dst[BLOCK_SIZE - 1] != '\n') {
            int i = 0;
            do {
                dst += src[offset + BLOCK_SIZE + i];
                ++i;
            } while (src[offset + BLOCK_SIZE + i] != '\n' && src[offset + BLOCK_SIZE + i] != NULL);
            offset += BLOCK_SIZE + i + 1;
        }
        return dst;
    }

    // Get split from file and pass it to Mapper
    void MapReduce::Map(const std::string &split, const int mapper_index) {
        t_lock.lock();
        mapper_result.push_back((*mapper_)(split));
        t_lock.unlock();
        mapper_status[mapper_index] = DONE;
        return;
    }

    int MapReduce::GetFreeMapperIndex(const std::vector<int> &mapper_status) {
        for (int i = 0; i < mapper_status.size(); ++i) {
            if (mapper_status[i] == FREE || mapper_status[i] == DONE) {
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
