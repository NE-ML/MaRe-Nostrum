#ifndef MAPPER_H
#define MAPPER_H

#include <vector>
#include <utility>
#include <string>

struct Mapper {
    std::vector<std::pair<std::string, int>>
    operator()(const std::string& str);
};

#endif//MAPPER_H
