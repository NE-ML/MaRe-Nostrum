#ifndef MARE_NOSTRUM_MAP_REDUCE_H
#define MARE_NOSTRUM_MAP_REDUCE_H

#include <vector>
#include <string>
#include <functional>
#include <utility>  // std::pair

namespace mare_nostrum {
    class MapReduce {
    public:
        MapReduce();

        explicit MapReduce(std::size_t split_size);

        ~MapReduce();

        void setInputFiles(const std::vector<std::string> &input_files);

        void setMaxSimultaneousWorkers(std::size_t max_simultaneous_workers);

        void setNumReducers(std::size_t num_reducers);

        void setOutputDir(const std::string &output_dir);

        void setTmpDir(const std::string &tmp_dir);

        // would be fine to replace with std::string_view
        void setMapper(std::function<std::vector<std::pair<std::string, std::string>>
                (const std::string &, const std::string &)> mapper);

        // would be fine to rewrite as template function with std::string_view and Iterable instead of std::vector
        void setReducer(std::function<std::vector<std::string, std::string>(const std::string &,
                                                                            const std::vector<std::string> &)> reducer);

        void start();

    private:
        // YOUR CODE HERE
    };
}

#endif  // MARE_NOSTRUM_MAP_REDUCE_H
