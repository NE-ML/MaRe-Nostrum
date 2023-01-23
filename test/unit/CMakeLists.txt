project(unit_tests)
enable_testing()

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

set(TEST_SOURCES
        test.cpp
        )
message("GTEST_LIBRARY - ${GTEST_LIBRARY}")
message("GTEST_MAIN_LIBRARY - ${GTEST_MAIN_LIBRARY}")
set(TEST_LIBRARIES
        ${GTEST_LIBRARY}
        ${GTEST_MAIN_LIBRARY}
        GTest::GTest
        Threads::Threads
        mapReduce_lib
        )


add_executable(${PROJECT_NAME} ${TEST_SOURCES})


target_include_directories(${PROJECT_NAME} PUBLIC ${GTEST_INCLUDE_DIR})
target_link_libraries(${PROJECT_NAME}
        ${TEST_LIBRARIES}
        )