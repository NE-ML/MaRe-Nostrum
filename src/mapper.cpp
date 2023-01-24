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
            for (auto &pair: map_result) {
                if (word == pair.first) {
                    ++(pair.second);
                    unique = false;
                    break;
                }
            }
            if (unique) {
                map_result.emplace_back(word, 1);
            }
        }
    }

    return map_result;
}
