#include <iostream>
#include <vector>
#include <thread>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <algorithm>
#include <fstream>
#include "map_reduce.h"

namespace mare_nostrum {
    void mapReduce::setInputFiles(const std::string &input_file) {
        input_file_ = input_file;
        file_size_ = std::filesystem::file_size(input_file_);
    }

    void mapReduce::setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers) {
        max_simultaneous_workers_ = max_simultaneous_workers;
        mapper_status.resize(max_simultaneous_workers_, FREE);
        mapped_data_for_reducer.resize(max_simultaneous_workers_);
    }

    void mapReduce::setNumReducers(std::size_t num_reducers) {
        num_reducers_ = num_reducers;
        reducer_chars.resize(num_reducers_);
        for (auto & i : mapped_data_for_reducer) {
            i.resize(num_reducers_);
        }
        calculateRangeOfKeysForReducers();
    }

    void mapReduce::setTmpDir(const std::string &tmp_dir = "/tmp") {
        tmp_dir_ = tmp_dir;
    }

    void mapReduce::setOutputDir(const std::string &output_dir) {
        output_dir_ = output_dir;
    }

    mapReduce::mapReduce() {

    }

    mapReduce::~mapReduce() {

    }

    void mapReduce::start() {
        if (!std::filesystem::create_directory(tmp_dir_)) {
            std::cerr << "Error creating temporary directory.\n";
        }

        std::vector<std::thread> threads(max_simultaneous_workers_);

        int descriptor = open(input_file_.c_str(), O_RDONLY);
        int offset = 0;

        while (offset < file_size_) {
            std::string split = getSplit(descriptor, offset);

            int index = getFreeMapperIndex(mapper_status);
            if (mapper_status[index] == DONE) {
                threads[index].join();
            }

            mapper_status[index] = BUSY;
            threads[index] = std::thread(&mapReduce::map, this, split, index);
        }

        for (int i = 0; i < max_simultaneous_workers_; ++i) {
            if (mapper_status[i] == BUSY || mapper_status[i] == DONE) {
                threads[i].join();
            }
        }

        threads.clear();
   }

    std::string mapReduce::getSplit(const int descriptor, int &offset) const {
//        off_t off = current_split * BLOCK_SIZE;
//        off_t off = BLOCK_SIZE;
//        off_t pa_off = off & ~(sysconf(_SC_PAGE_SIZE) - 1);
//        char* src = (char*)mmap(NULL, BLOCK_SIZE + off - pa_off, PROT_READ, MAP_SHARED, descriptor, pa_off);
        char* src = (char*) mmap(nullptr, BLOCK_SIZE, PROT_READ, MAP_SHARED, descriptor, 0);
        if (offset + BLOCK_SIZE > file_size_) {
            std::string dst(src + offset, file_size_);
            offset += (int)file_size_;
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

    void mapReduce::reduce(const int reducer_index) {
        // Сделать merge списков и передать в user reducer
        std::vector<std::pair<std::string, std::vector<int>>> merged_data;
        for (int mapper_i = 0; mapper_i < max_simultaneous_workers_; ++mapper_i) {
            // Из каждого маппера достать данные для текущего редьюсера и добавить в merged_data
            // если K - новый, то добавить пару
            // иначе - добавить V к существующему K
            for (auto &pair: mapped_data_for_reducer[mapper_i][reducer_index]) {
                bool same = false;
                for (auto &pair_in_merged_list: merged_data) {
                    if (pair_in_merged_list.first == pair.first) {
                        pair_in_merged_list.second.push_back(pair.second);
                        same = true;
                        break;
                    }
                }
                if (!same) {
                    merged_data.emplace_back(pair.first, std::vector<int>(pair.second));
                }
            }
        }

        std::vector<std::pair<std::string, int>> reduce_result = (*reducer_)(merged_data);

        std::ofstream file (tmp_dir_ + std::to_string(reducer_index) + ".txt");
        for (const auto& pair: reduce_result) {
            file << pair.first << ": " << pair.second << "\n";
        }
        file.close();
   }

    void mapReduce::map(const std::string &split, int mapper_index) {
        map_type map_result = (*mapper_)(split);
        std::sort(map_result.begin(), map_result.end(),
                  [](auto &left, auto &right) {
                      return left.first.compare(right.first) < 0;
                  });

        // 1. определить какие значения какой редьюсер принимает
        // 2. Разделить результат для каждого маппера

        for (int reducer_i = 0, range_begin = 0; reducer_i < num_reducers_; ++reducer_i) {
            // Узнать, какое количество пар в диапазоне редьюсера
            auto range_end = std::count_if(map_result.begin(), map_result.end(),
                       [this, reducer_i] (auto &pair) {
                          for (char & i : reducer_chars[reducer_i]) {
                              if (pair.first[0] == i) {
                                  return true;
                              }
                          }
                          return false;
                       });
            // Пары из полученного диапазона закинуть в вектор
            t_lock.lock();
            mapped_data_for_reducer[mapper_index][reducer_i] =
                    map_type(map_result.begin() + range_begin, map_result.begin() + range_end);
            t_lock.unlock();
        }

        mapper_status[mapper_index] = DONE;
   }

    int mapReduce::getFreeMapperIndex(const std::vector<int> &mapper_status) {
        for (int i = 0; i < mapper_status.size(); ++i) {
            if (mapper_status[i] == FREE || mapper_status[i] == DONE) {
                return i;
            }
        }
        return -1;
    }

    void mapReduce::setMapper(
            std::function<std::vector<std::pair<std::string, int>>(const std::string &)> &mapper) {
        mapper_ = &mapper;
    }

    void mapReduce::setReducer(std::function<std::vector<std::pair<std::string, int>>(const std::vector<std::pair<std::string, std::vector<int>>> &)> &reducer) {
        reducer_ = &reducer;
    }

    void mapReduce::calculateRangeOfKeysForReducers() {
        for (size_t i = 0; i < num_reducers_; ++i) {
            size_t range_begin = i * 26 / num_reducers_;
            size_t range_end = (i + 1) * 26 / num_reducers_ - 1;
            for (size_t j = range_begin; j <= range_end; ++j) {
                reducer_chars[i].push_back(char(97 + j));
            }
        }
    }

    mapReduce::mapReduce(std::size_t split_size) {

    }
}  // namespace mare_nostrum
