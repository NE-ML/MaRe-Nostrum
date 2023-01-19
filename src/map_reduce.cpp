#include <iostream>
#include <filesystem>
#include <fstream>
#include "map_reduce.h"

namespace mare_nostrum {
    void MapReduce::setInputFiles(const std::vector<std::string> &input_files) {
        input_files_ = input_files;
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

    void MapReduce::setMapper(IMapper &mapper) {
        mapper_ = &mapper;
    }

    void MapReduce::start() {
        if (!std::filesystem::create_directory(tmp_dir_)) {
            std::cerr << "Error creating temporary directory.\n";
        }
        mergeFiles();

    }

    void MapReduce::mergeFiles() {
        std::ofstream big_file;
        big_file.open(tmp_dir_ + big_file_);
        for (const auto &input_file_name: input_files_) {
            std::ifstream file;
            file.open(input_file_name);
            std::string line;
            while (std::getline(file, line)) {
                big_file << line << std::endl;
            }
            file.close();
        }
        big_file.close();
    }
}  // namespace mare_nostrum
