//
// Created by Alexander on 17.01.2023.
//
#include "mapper.h"

std::vector<std::pair<std::string, int>> Mapper::operator()(const std::string &str) {
    std::vector<std::pair<std::string, int>> map_result;

    for (int i = 0; i < str.size(); ++i) {
        // Begin of word
        if (str[i] != ' ' && str[i] != '\n') {
            int word_len = 0;
            while (str[i + word_len] != ' ' && str[i + word_len] != '\n' && i + word_len < str.size()) {
                ++word_len;
            }
            std::string word = str.substr(i, word_len);
            i += word_len;
            bool unique = true;
            // If word is NOT new
            for (int k = 0; k < map_result.size(); ++k) {
                if (word == map_result[k].first) {
                    ++(map_result[k].second);
                    unique = false;
                    break;
                }
            }
            if (unique) {
                map_result.push_back(std::pair<std::string, int>(word, 1));
            }
        }
    }

    return map_result;
}
