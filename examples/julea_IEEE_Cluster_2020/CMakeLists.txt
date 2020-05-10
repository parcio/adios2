#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

add_executable(SHOWBP_FORMAT ShowBPFormat.cpp)
target_link_libraries(SHOWBP_FORMAT adios2)

add_executable(NC_TO_BP NC_to_BP.cpp)
target_link_libraries(NC_TO_BP adios2)
target_link_libraries(NC_TO_BP PkgConfig::NETCDF)

# add_executable(QUERY_JULEA JuleaQueryApplication.cpp)
# target_link_libraries(QUERY_JULEA adios2)
# target_link_libraries(QUERY_JULEA PkgConfig::JULEA)

set(SOURCES QueryBenchmark.cpp AdiosQuery.h AdiosQuery.cpp JuleaQuery.h JuleaQuery.cpp)
add_executable(QUERY_BENCHMARK ${SOURCES})
target_link_libraries(QUERY_BENCHMARK adios2)
target_link_libraries(QUERY_BENCHMARK PkgConfig::JULEA)
