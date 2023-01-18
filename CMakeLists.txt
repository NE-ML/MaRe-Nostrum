cmake_minimum_required(VERSION 3.18)
project(MaRe-Nostrum)

set(CMAKE_CXX_STANDARD 20)

add_subdirectory(src)

set(EXE
        exe/main.cpp)

add_executable(${PROJECT_NAME}
        ${EXE})

target_link_libraries(${PROJECT_NAME} PRIVATE mapReduce_lib)
