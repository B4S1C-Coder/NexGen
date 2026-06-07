cmake_minimum_required(VERSION 3.15)
project(llama_cpp_tests)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(VENDOR_DIR ${CMAKE_SOURCE_DIR}/../vendor)
set(LLAMA_DIR ${VENDOR_DIR}/llama.cpp)
set(LLAMA_BUILD_BIN ${LLAMA_DIR}/build/bin)

add_library(ggml        SHARED IMPORTED)
add_library(ggml_base   SHARED IMPORTED)
add_library(ggml_cpu    SHARED IMPORTED)
add_library(ggml_cuda   SHARED IMPORTED)
add_library(llama       SHARED IMPORTED)
add_library(llama_common SHARED IMPORTED)

set_target_properties(ggml        PROPERTIES IMPORTED_LOCATION ${LLAMA_BUILD_BIN}/libggml.so)
set_target_properties(ggml_base   PROPERTIES IMPORTED_LOCATION ${LLAMA_BUILD_BIN}/libggml-base.so)
set_target_properties(ggml_cpu    PROPERTIES IMPORTED_LOCATION ${LLAMA_BUILD_BIN}/libggml-cpu.so)
set_target_properties(ggml_cuda   PROPERTIES IMPORTED_LOCATION ${LLAMA_BUILD_BIN}/libggml-cuda.so)
set_target_properties(llama       PROPERTIES IMPORTED_LOCATION ${LLAMA_BUILD_BIN}/libllama.so)
set_target_properties(llama_common PROPERTIES IMPORTED_LOCATION ${LLAMA_BUILD_BIN}/libllama-common.so)

add_library(llama_bundle INTERFACE)
target_link_libraries(llama_bundle INTERFACE
  llama ggml ggml_base ggml_cpu ggml_cuda llama_common
)

target_include_directories(llama_bundle INTERFACE
  ${LLAMA_DIR}/include
  ${LLAMA_DIR}/ggml/include
)

add_executable(test_gpu src/test_gpu.cc)
target_link_libraries(test_gpu PRIVATE llama_bundle)