#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

add_executable(BITTE_BITTE JuleaEngineTest.cpp)
target_link_libraries(BITTE_BITTE adios2)
target_link_libraries(BITTE_BITTE PkgConfig::JULEA)

add_executable(SIMPLE_STEPS SimpleStepTest.cpp)
target_link_libraries(SIMPLE_STEPS adios2)

add_executable(WRITE_STEPS localArray_write.cpp)
target_link_libraries(WRITE_STEPS adios2)

add_executable(SHOWBP_FORMAT ShowBPFormat.cpp)
target_link_libraries(SHOWBP_FORMAT adios2)

add_executable(NC_TO_BP NC_to_BP.cpp)
target_link_libraries(NC_TO_BP adios2)
target_link_libraries(NC_TO_BP PkgConfig::NETCDF)

if (ADIOS2_HAVE_MPI)
    target_link_libraries(WRITE_STEPS MPI::MPI_C)
endif()
