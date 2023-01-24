#include <iostream>
#include <vector>
#include <thread>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <algorithm>
#include <fstream>
#include <unistd.h>
#include "map_reduce.h"

namespace mare_nostrum {
    void MapReduce::setInputFiles(const std::string &input_file) {
        input_file_ = input_file;
        file_size_ = std::filesystem::file_size(input_file_);
    }

    void MapReduce::setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers) {
        max_simultaneous_workers_ = max_simultaneous_workers;
        mapper_status.resize(max_simultaneous_workers_, FREE);
//        mapped_data_for_reducer.resize(max_simultaneous_workers_);
    }

    void MapReduce::setNumReducers(std::size_t num_reducers) {
        num_reducers_ = num_reducers;
        reducer_chars.resize(num_reducers_);
//        for (int i = 0; i < mapped_data_for_reducer.size(); ++i) {
//            mapped_data_for_reducer[i].resize(num_reducers_);
//        }
        CalculateRangeOfKeysForReducers();
    }

    void MapReduce::setTmpDir(const std::string &tmp_dir = "/tmp/") {
        tmp_dir_ = tmp_dir;
    }

    void MapReduce::setOutputDir(const std::string &output_dir) {
        output_dir_ = output_dir;
    }

    void MapReduce::start() {
        if (!std::filesystem::create_directory(tmp_dir_)) {
            std::cerr << "Error creating temporary directory.\n";
        }

        std::vector<std::thread> threads(max_simultaneous_workers_);

        int descriptor = open(input_file_.c_str(), O_RDONLY);
        int offset = 0;
        int current_split = 0;
        while (offset < file_size_) {
            char* split = GetSplit(descriptor, offset, current_split++);
            int index = -1;
            do {
                index = GetFreeMapperIndex();
            } while(index == -1);
            if (mapper_status[index] == DONE) {
                threads[index].join();
            }

            mapper_status[index] = BUSY;
            threads[index] = std::thread(&MapReduce::Map, this, split, current_split-1);
        }

        for (int i = 0; i < max_simultaneous_workers_; ++i) {
            if (mapper_status[i] == BUSY || mapper_status[i] == DONE) {
                threads[i].join();
            }
        }

        threads.clear();


        for (int reducer_i = 0; reducer_i < num_reducers_; ++reducer_i) {
            threads[reducer_i] = std::thread(&MapReduce::Reduce, this, reducer_i);
        }
        for (int reducer_i = 0; reducer_i < num_reducers_; ++reducer_i) {
            threads[reducer_i].join();
        }
    }

    char *MapReduce::GetSplit(const int descriptor, int &offset, const int current_split) const {
        off_t off = current_split * BLOCK_SIZE;
        off_t pa_off = off & ~(sysconf(_SC_PAGE_SIZE) - 1);
        char* src = reinterpret_cast<char*>(mmap(nullptr, BLOCK_SIZE + off, PROT_READ, MAP_SHARED, descriptor, pa_off));

        char* dst = nullptr;
        if (pa_off + BLOCK_SIZE + offset > file_size_) {
            dst = (char*) malloc(file_size_);
            memcpy(dst, src + offset, file_size_ - pa_off);
            offset += file_size_;
            return dst;
        }

        if (src[BLOCK_SIZE - 1] != '\n') {
            munmap(src, BLOCK_SIZE);
            src = reinterpret_cast<char*>(mmap(nullptr, BLOCK_SIZE + off + 1024, PROT_READ, MAP_SHARED, descriptor, pa_off));
            int i = 0;
            do {
                ++i;
            } while (src[offset + BLOCK_SIZE + i] != '\n' && src[offset + BLOCK_SIZE + i] != NULL);
            dst = (char*) malloc(BLOCK_SIZE + i);
            memcpy(dst, src + offset, BLOCK_SIZE + i);
            if (src[offset + BLOCK_SIZE + i] == NULL) {
                offset += BLOCK_SIZE;
            }
            offset += i + 1;
        }
        return dst;
    }

    void MapReduce::Reduce(const int reducer_index) {
        std::vector<std::pair<std::string, std::vector<int>>> merged_data;
        for (int mapper_i = 0; mapper_i < mapped_data_for_reducer.size(); ++mapper_i) {
            for (auto &pair: mapped_data_for_reducer[mapper_i][reducer_index]) {
                bool exist = false;
                for (auto &pair_in_merged_list: merged_data) {
                    if (pair_in_merged_list.first == pair.first) {
                        pair_in_merged_list.second.push_back(pair.second);
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    merged_data.emplace_back(pair.first, std::initializer_list<int>{pair.second});
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

    void MapReduce::Map(const std::string &split, const int mapper_index) {
        map_type map_result = (*mapper_)(split);
        std::sort(map_result.begin(), map_result.end(),
                  [](auto &left, auto &right) {
                      return left.first.compare(right.first) < 0;
                  });

//        t_lock.lock();
//        if (mapped_data_for_reducer.size() == mapper_index) {
//            mapped_data_for_reducer.resize(mapped_data_for_reducer.size() * 2);
//        }
//        t_lock.unlock();
        std::vector<std::vector<std::pair<std::string, int>>> mapped_splitted_data;

        long range_begin = 0;
        for (int reducer_i = 0; reducer_i < num_reducers_; ++reducer_i) {
            auto range_end = std::count_if(map_result.begin(), map_result.end(),
                       [this, reducer_i] (auto &pair) {
                          for (char & sym : reducer_chars[reducer_i]) {
                              if (pair.first[0] == sym) {
                                  return true;
                              }
                          }
                          return false;
                       });
            range_end += range_begin;
//            t_lock.lock();
            mapped_splitted_data.emplace_back(map_result.begin() + range_begin, map_result.begin() + range_end);
//            mapped_data_for_reducer[mapper_index].emplace_back(map_result.begin() + range_begin, map_result.begin() + range_end);
//            mapped_data_for_reducer[mapper_index][reducer_i] =
//                    map_type(map_result.begin() + range_begin, map_result.begin() + range_end);
//            t_lock.unlock();
            range_begin = range_end;
        }
        std::cout << mapper_index << " - " << map_result[3].first << ": " << map_result[3].second << std::endl;

        t_lock.lock();
        mapped_data_for_reducer.emplace_back(mapped_splitted_data);
        t_lock.unlock();

        mapper_status[mapper_index] = DONE;
    }

    int MapReduce::GetFreeMapperIndex() {
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

    void MapReduce::setReducer(std::function<std::vector<std::pair<std::string, int>>(const std::vector<std::pair<std::string, std::vector<int>>> &)> &reducer) {
        reducer_ = &reducer;
    }

    void MapReduce::CalculateRangeOfKeysForReducers() {
        std::vector<char> alphabet { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
        const int alphabet_size = 26;
        std::vector<int> reducer_sizes(num_reducers_, alphabet_size / num_reducers_);
        for (int i = 0; i < alphabet_size % num_reducers_; ++i) {
            ++(reducer_sizes[i]);
        }

        int k = 0;
        for (int i = 0; i < reducer_sizes.size(); ++i) {
            for (int j = 0; j < reducer_sizes[i]; ++j) {
                reducer_chars[i].push_back(alphabet[k++]);
            }
        }
    }
}  // namespace mare_nostrum
