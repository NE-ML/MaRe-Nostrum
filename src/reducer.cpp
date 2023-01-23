//
// Created by Alexander on 23.01.2023.
//
#include <numeric>
#include "reducer.h"

std::vector<std::pair<std::string, int>> Reducer::operator()
        (const std::vector<std::pair<std::string, std::vector<int>>> & merged_data) {
    std::vector<std::pair<std::string, int>> result;
    for (const auto &pair: merged_data) {
        int sum = std::accumulate(pair.second.begin(), pair.second.end(), 0);
        result.emplace_back(pair.first, sum);
    }
    return result;
}