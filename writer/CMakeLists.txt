message(STATUS "exp_chronos_plugin plugin enabled")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library( exp_chronos_plugin
             exp_chronos_plugin.cpp )

target_link_libraries(exp_chronos_plugin scylla-cpp-driver Boost::thread)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(exp_chronos_plugin PRIVATE -Wall -Wextra -Wno-unused-parameter -Wno-deprecated-declarations -fcolor-diagnostics)
endif()

chronicle_receiver_additional_plugin(exp_chronos_plugin)
