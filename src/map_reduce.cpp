//
// Created by Alexander on 17.01.2023.
//
#include "../include/map_reduce.h"
// #include "map_reduce.h"

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


}