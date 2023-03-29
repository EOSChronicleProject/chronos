message(STATUS "exp_chronos_plugin plugin enabled")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library( exp_chronos_plugin
             exp_chronos_plugin.cpp )

chronicle_receiver_additional_plugin(exp_chronos_plugin)
