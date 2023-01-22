#ifndef MARE_NOSTRUM_MAP_REDUCE_H
#define MARE_NOSTRUM_MAP_REDUCE_H

#include <vector>
#include <string>
#include <functional>
#include <utility>  // std::pair

#define BLOCK_SIZE 10

namespace mare_nostrum {
    class MapReduce {
    public:
        enum mapperStatus {
            FREE,
            BUSY
        };

        MapReduce();

        explicit MapReduce(std::size_t split_size);

        ~MapReduce();

        void setInputFiles(const std::string &input_file);

        void setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers);

        void setNumReducers(std::size_t num_reducers);

        void setOutputDir(const std::string &output_dir);

        void setTmpDir(const std::string &tmp_dir);

        // would be fine to replace with std::string_view
        void setMapper(std::function<std::vector<std::pair<std::string, int>>(const std::string &)> &mapper);

        // would be fine to rewrite as template function with std::string_view and Iterable instead of std::vector
        void setReducer(std::function<std::vector<std::string, std::string>(const std::string &,
                                                                            const std::vector<std::string> &)> reducer);

        void mergeFiles();

        void start();

    private:
        // YOUR CODE HERE
        std::function<std::vector<std::pair<std::string, int>>
                      (const std::string &)>* mapper_;
        std::string tmp_dir_;
        std::string output_dir_;
        std::string big_file_ = "big_file.txt";
        std::string input_file_;
        std::size_t max_simultaneous_workers_;
        std::size_t num_reducers_;
        const int block_size = BLOCK_SIZE;

        int GetFreeMapperIndex(const std::vector<int> &mapper_status);

        void Map(const int descriptor, const int mapper_index, const int current_split);
    };
}

#endif  // MARE_NOSTRUM_MAP_REDUCE_H
