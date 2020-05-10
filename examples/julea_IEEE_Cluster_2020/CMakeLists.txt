#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

add_executable(SHOWBP_FORMAT ShowBPFormat.cpp)
target_link_libraries(SHOWBP_FORMAT adios2)

set(NC_SOURCES NC_to_BP.cpp NetCDFRead.cpp NetCDFRead.h)
add_executable(NC_TO_BP ${NC_SOURCES})
target_link_libraries(NC_TO_BP adios2)
target_link_libraries(NC_TO_BP PkgConfig::NETCDF)

# add_executable(Read_JULEA JuleaReadApplication.cpp)
# target_link_libraries(Read_JULEA adios2)
# target_link_libraries(Read_JULEA PkgConfig::JULEA)

set(BENCHMARK_SOURCES Benchmark.cpp AdiosRead.h AdiosRead.cpp JuleaRead.h JuleaRead.cpp NetCDFRead.cpp NetCDFRead.h)
# set(Q_SOURCES ReadBenchmark.cpp AdiosRead.h AdiosRead.cpp JuleaRead.h JuleaRead.cpp)
add_executable(BENCHMARK ${BENCHMARK_SOURCES})
target_link_libraries(BENCHMARK adios2)
target_link_libraries(BENCHMARK PkgConfig::JULEA)
target_link_libraries(BENCHMARK PkgConfig::NETCDF)
