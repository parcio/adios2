#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

add_executable(SHOWBP_FORMAT ShowBPFormat.cpp)
target_link_libraries(SHOWBP_FORMAT adios2)

set(NC_SOURCES NC_to_BP.cpp NetCDFQuery.cpp NetCDFQuery.h)
add_executable(NC_TO_BP ${NC_SOURCES})
target_link_libraries(NC_TO_BP adios2)
target_link_libraries(NC_TO_BP PkgConfig::NETCDF)

# add_executable(QUERY_JULEA JuleaQueryApplication.cpp)
# target_link_libraries(QUERY_JULEA adios2)
# target_link_libraries(QUERY_JULEA PkgConfig::JULEA)

set(Q_SOURCES QueryBenchmark.cpp AdiosQuery.h AdiosQuery.cpp JuleaQuery.h JuleaQuery.cpp NetCDFQuery.cpp NetCDFQuery.h)
# set(Q_SOURCES QueryBenchmark.cpp AdiosQuery.h AdiosQuery.cpp JuleaQuery.h JuleaQuery.cpp)
add_executable(QUERY_BENCHMARK ${Q_SOURCES})
target_link_libraries(QUERY_BENCHMARK adios2)
target_link_libraries(QUERY_BENCHMARK PkgConfig::JULEA)
target_link_libraries(QUERY_BENCHMARK PkgConfig::NETCDF)
