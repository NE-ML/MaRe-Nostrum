#ifndef MARE_NOSTRUM_MAP_REDUCE_H
#define MARE_NOSTRUM_MAP_REDUCE_H

#include <vector>
#include <string>
#include <functional>
#include <utility>  // std::pair

namespace mare_nostrum {
    class MapReduce {
    public:
        struct IMapper {
            virtual std::vector<std::pair<std::string, std::string>>
            operator()(const std::string&) = 0;
        };

        struct IReducer {
            virtual std::vector<std::pair<std::string, std::string>>
            operator()(std::vector<std::pair<std::string, std::vector<std::string>>>) = 0;
        };

        MapReduce();

        explicit MapReduce(std::size_t split_size);

        ~MapReduce();

        void setInputFiles(const std::vector<std::string> &input_files);

        void setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers);

        void setNumReducers(std::size_t num_reducers);

        void setOutputDir(const std::string &output_dir);

        void setTmpDir(const std::string &tmp_dir);

        // would be fine to replace with std::string_view
        void setMapper(IMapper &mapper);

        // would be fine to rewrite as template function with std::string_view and Iterable instead of std::vector
        void setReducer(std::function<std::vector<std::string, std::string>(const std::string &,
                                                                            const std::vector<std::string> &)> reducer);

        void mergeFiles();

        void start();

    private:
        // YOUR CODE HERE
        IMapper* mapper_;
        std::string tmp_dir_;
        std::string output_dir_;
        std::string big_file_ = "big_file.txt";
        std::vector<std::string> input_files_;
        std::size_t max_simultaneous_workers_;
        std::size_t num_reducers_;
    };
}

#endif  // MARE_NOSTRUM_MAP_REDUCE_H
