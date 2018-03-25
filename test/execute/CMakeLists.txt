# build service
set(SOURCE_FILES ExecutorTest.cpp ${PROJECT_SOURCE_DIR}/src/Executor.cpp)

add_executable(runExecuteTests ${SOURCE_FILES} ${BACKWARD_ENABLE})
target_link_libraries(runExecuteTests Execute gtest gmock gmock_main)

add_backward(runExecuteTests)
add_test(runExecuteTests runExecuteTests)
