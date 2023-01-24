#include <iostream>
#include <vector>
#include <thread>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <algorithm>
#include <fstream>

#include <cstring>

#include "map_reduce.h"

namespace mare_nostrum {
    void MapReduce::setInputFiles(const std::string &input_file) {
        input_file_ = input_file;
        file_size_ = std::filesystem::file_size(input_file_);
    }

    void MapReduce::setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers) {
        max_simultaneous_workers_ = max_simultaneous_workers;
        mapper_status.resize(max_simultaneous_workers_, mapperStatus::FREE);
//        mapped_data_for_reducer.resize(max_simultaneous_workers_);
    }

    void MapReduce::setNumReducers(std::size_t num_reducers) {
        num_reducers_ = num_reducers;
        reducer_chars.resize(num_reducers_);
        calculateRangeOfKeysForReducers();
    }

    void MapReduce::setTmpDir(const std::string &tmp_dir = "/tmp/") {
        tmp_dir_ = tmp_dir;
    }

    void MapReduce::setOutputDir(const std::string &output_dir) {
        output_dir_ = output_dir;
    }

    void MapReduce::start() {
        if (!std::filesystem::create_directory(tmp_dir_)) {
            std::cerr << "Error creating temporary directory. It possibly exists\n";
        }

        std::vector<std::thread> threads(max_simultaneous_workers_);

        int descriptor = open(input_file_.c_str(), O_RDONLY);
        if (descriptor == -1) {
            std::cerr << "Error opening file.\n";
        }

        char *mapped_data = reinterpret_cast<char *>(mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, descriptor,
                                                          0));  // map the current pages

        int offset = 0;
        int current_split = 0;
        while (offset < file_size_) {
            auto split = getSplit(mapped_data, offset, current_split++);
            int index = -1;
            do {
                index = getFreeMapperIndex();
            } while (index == -1);
            if (mapper_status[index] == mapperStatus::DONE) {
                threads[index].join();
            }

            mapper_status[index] = mapperStatus::BUSY;
            threads[index] = std::thread(&MapReduce::map, this, split, current_split - 1);
        }

        for (int i = 0; i < max_simultaneous_workers_; ++i) {
            if (mapper_status[i] == mapperStatus::BUSY || mapper_status[i] == mapperStatus::DONE) {
                threads[i].join();
            }
        }
        munmap(mapped_data, file_size_);
        close(descriptor);
        threads.clear();


        for (int reducer_i = 0; reducer_i < num_reducers_; ++reducer_i) {
            threads[reducer_i] = std::thread(&MapReduce::reduce, this, reducer_i);
        }
        for (int reducer_i = 0; reducer_i < num_reducers_; ++reducer_i) {
            threads[reducer_i].join();
        }
    }

    std::string MapReduce::getSplit(const char *mapped_data, int &offset, const int current_split) const {


        if (offset + BLOCK_SIZE > file_size_) {
            // last split: start of split + BLOCK_SIZE may go beyond the end of the file
            std::string dst = std::string(mapped_data + offset,
                                          file_size_ - offset);  // Added - offset to avoid copying the same data twice
            offset += BLOCK_SIZE;
            return dst;
        }

        if (mapped_data[(current_split + 1) * BLOCK_SIZE - 1] != '\n') {
            // if the last character of the current page is not a newline, we need to read the next page
            // remap the current block with an extra page
            int i = 0;
            for (; mapped_data[offset + BLOCK_SIZE + i] != '\n' &&
                   mapped_data[offset + BLOCK_SIZE + i] != NULL &&
                   offset + BLOCK_SIZE + i < file_size_; ++i) {
                // while not a newline or end of file
                // we increase i until we find a newline or the end of the file
            }
            auto dst = std::string(mapped_data + offset, BLOCK_SIZE + i + 1);
            offset += BLOCK_SIZE + i + 1;
            return dst;
        }
        return std::string(mapped_data + offset, BLOCK_SIZE);
    }

    void MapReduce::reduce(const int reducer_index) {
        std::vector<std::pair<std::string, std::vector<int>>> merged_data;
        for (const auto & mapper_i : mapped_data_for_reducer) {
            for (const auto &pair : mapper_i[reducer_index]) {
                bool exist = false;
                for (auto &pair_in_merged_list : merged_data) {
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

        std::vector<std::pair<std::string, int>> reduce_result = reducer_(merged_data);

        std::ofstream file(tmp_dir_ + std::to_string(reducer_index) + ".txt");
        for (const auto &pair : reduce_result) {
            file << pair.first << ": " << pair.second << "\n";
        }
        file.close();
    }

    void MapReduce::map(const std::string &split, int mapper_index) {
        map_type map_result = mapper_(split);
        std::sort(map_result.begin(), map_result.end(),
                  [](auto &left, auto &right) {
                      return left.first.compare(right.first) < 0;
                  });
        std::vector<std::vector<std::pair<std::string, int>>> mapped_splitted_data;

        long range_begin = 0;
        for (int reducer_i = 0; reducer_i < num_reducers_; ++reducer_i) {
            auto range_end = std::count_if(map_result.begin(), map_result.end(),
                                           [this, reducer_i](auto &pair) {
                                               for (char &sym : reducer_chars[reducer_i]) {
                                                   if (pair.first[0] == sym) {
                                                       return true;
                                                   }
                                               }
                                               return false;
                                           });
            range_end += range_begin;
            mapped_splitted_data.emplace_back(map_result.begin() + range_begin, map_result.begin() + range_end);
            range_begin = range_end;
        }
        {
            std::lock_guard<std::mutex> lock(t_lock);
            std::cout << mapper_index << " - " << map_result[3].first << ": " << map_result[3].second << std::endl;
            mapped_data_for_reducer.emplace_back(mapped_splitted_data);
        }

        mapper_status[mapper_index] = mapperStatus::DONE;
    }

    int MapReduce::getFreeMapperIndex() {
        for (int i = 0; i < mapper_status.size(); ++i) {
            if (mapper_status[i] == mapperStatus::FREE || mapper_status[i] == mapperStatus::DONE) {
                return i;
            }
        }
        return -1;
    }

    void MapReduce::setMapper(
            std::function<std::vector<std::pair<std::string, int>>(const std::string &)> &mapper) {
        mapper_ = mapper;
    }

    void MapReduce::setReducer(std::function<std::vector<std::pair<std::string, int>>(
            const std::vector<std::pair<std::string, std::vector<int>>> &)> &reducer) {
        reducer_ = reducer;
    }

    void MapReduce::calculateRangeOfKeysForReducers() {
        std::vector<char> alphabet{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
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
