cmake_minimum_required(VERSION 2.8.12)
project(lindorm-tsdb-contest-cpp)

# We use g++-12 to compile, so that we recommend you to use the same compiler to test for better compatibility.
message("Your OS: ${CMAKE_HOST_SYSTEM_NAME}")
if (CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
    set(CMAKE_C_COMPILER "gcc-12")
    set(CMAKE_CXX_COMPILER "g++-12")
elseif (MAKE_HOST_SYSTEM_NAME MATCHES "Windows")
    # Add w64devkit\bin to PATH.
else ()
    set(CMAKE_C_COMPILER "gcc")
    set(CMAKE_CXX_COMPILER "g++")
endif ()

# # set(CMAKE_CXX_STANDARD 17)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
# # set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3") 
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g") 
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")


find_package(OpenMP)
if(OpenMP_CXX_FOUND)
  set(CMAKE_CXX_FLAGS "-pthread -ldl -lrt ${OpenMP_CXX_FLAGS} ${CMAKE_CXX_FLAGS} ")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()


    



include_directories(${PROJECT_SOURCE_DIR}/include)

file(GLOB_RECURSE sources_c "${PROJECT_SOURCE_DIR}/source/*.c")
file(GLOB_RECURSE sources_cpp "${PROJECT_SOURCE_DIR}/source/*.cpp")

add_library(yourDbLib ${sources_c} ${sources_cpp})

add_executable(testMyDb ${PROJECT_SOURCE_DIR}/TestMyDb.cpp)
target_link_libraries(testMyDb yourDbLib)

add_executable(perfTest ${PROJECT_SOURCE_DIR}/perfTest.cpp)
target_link_libraries(perfTest yourDbLib)
