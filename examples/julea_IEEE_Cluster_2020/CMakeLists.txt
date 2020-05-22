#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

add_executable(SHOWBP_FORMAT ShowBPFormat.cpp)
target_link_libraries(SHOWBP_FORMAT adios2)

set(NC_SOURCES NC_to_BP.cpp
	benchmarkSource/NetCDFRead.cpp benchmarkSource/NetCDFRead.h)
add_executable(NC_TO_BP ${NC_SOURCES})
target_link_libraries(NC_TO_BP adios2)
target_link_libraries(NC_TO_BP PkgConfig::NETCDF)

set(BENCHMARK_SOURCES Benchmark.cpp
	benchmarkSource/AdiosRead.h benchmarkSource/AdiosRead.cpp benchmarkSource/AdiosMinMax.cpp
	benchmarkSource/JuleaRead.h benchmarkSource/JuleaRead.cpp
	benchmarkSource/NetCDFRead.cpp benchmarkSource/NetCDFRead.h)
add_executable(BENCHMARK ${BENCHMARK_SOURCES})
target_link_libraries(BENCHMARK adios2)
target_link_libraries(BENCHMARK PkgConfig::JULEA)
target_link_libraries(BENCHMARK PkgConfig::NETCDF)
