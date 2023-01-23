#ifndef MARE_NOSTRUM_MAP_REDUCE_H
#define MARE_NOSTRUM_MAP_REDUCE_H

#include <vector>
#include <string>
#include <functional>
#include <thread>
#include <utility>  // std::pair

#define BLOCK_SIZE 131072

namespace mare_nostrum {
    class mapReduce {
    public:
        using map_type = std::vector<std::pair<std::string, int>>;

        enum mapperStatus {
            FREE,
            BUSY,
            DONE
        };

        mapReduce();

        explicit mapReduce(std::size_t split_size);

        ~mapReduce();

        void setInputFiles(const std::string &input_file);

        void setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers);

        void setNumReducers(std::size_t num_reducers);

        void setOutputDir(const std::string &output_dir);

        void setTmpDir(const std::string &tmp_dir);

        // would be fine to replace with std::string_view
        void setMapper(std::function<std::vector<std::pair<std::string, int>>(const std::string &)> &mapper);

        // would be fine to rewrite as template function with std::string_view and Iterable instead of std::vector
        void setReducer(std::function<std::vector<std::pair<std::string, int>>
                                      (const std::vector<std::pair<std::string, std::vector<int>>> &)> &reducer);

        void start();

    private:
        // YOUR CODE HERE
        std::function<std::vector<std::pair<std::string, int>>
                      (const std::string &)>* mapper_{};
        std::function<std::vector<std::pair<std::string, int>>
                      (const std::vector<std::pair<std::string, std::vector<int>>> &)>* reducer_{};
        uintmax_t file_size_{};
        std::string tmp_dir_;
        std::string output_dir_;
        std::string big_file_ = "big_file.txt";
        std::string input_file_;
        std::vector<int> mapper_status;                             // Current mapper state
        std::size_t max_simultaneous_workers_{};
        std::vector<std::vector<char>> reducer_chars;               // Range of chars for each reducer
        std::vector<std::vector<std::pair<std::string, int>>> mapper_result;
        std::vector<std::vector<std::vector<std::pair<std::string, int>>>> mapped_data_for_reducer;
        std::size_t num_reducers_{};      // Number of reducers
        std::mutex t_lock;

        static int getFreeMapperIndex(const std::vector<int> &mapper_status);

        std::string getSplit(int descriptor, int &offset) const;

        void map(const std::string &split, int mapper_index);

        void reduce(int reducer_index);

        void calculateRangeOfKeysForReducers();
    };
}

#endif  // MARE_NOSTRUM_MAP_REDUCE_H
