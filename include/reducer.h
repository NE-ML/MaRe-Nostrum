#ifndef MARE_NOSTRUM_REDUCER_H
#define MARE_NOSTRUM_REDUCER_H

#include <vector>
#include <utility>
#include <string>

struct Reducer {
    std::vector<std::pair<std::string, int>>
    operator()(const std::vector<std::pair<std::string, std::vector<int>>>&);
};

#endif//MARE_NOSTRUM_REDUCER_H
