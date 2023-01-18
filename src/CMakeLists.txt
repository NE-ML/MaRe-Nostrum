project(mapReduce_library)

set(LIB_SRC
        map_reduce.cpp
        map_reduce_mapper.cpp
        map_reduce_reducer.cpp)

add_library(mapReduce_lib STATIC ${LIB_SRC})

message(STATUS ${CMAKE_SOURCE_DIR})
message(STATUS ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mapReduce_lib PUBLIC ${CMAKE_SOURCE_DIR}/include)
