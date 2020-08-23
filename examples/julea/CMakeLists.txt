#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

add_executable(BITTE_BITTE JuleaEngineTest.cpp)
target_link_libraries(BITTE_BITTE adios2)
target_link_libraries(BITTE_BITTE PkgConfig::JULEA)

add_executable(SIMPLE_STEPS SimpleStepTest.cpp)
target_link_libraries(SIMPLE_STEPS adios2)

add_executable(HELLO_MPI mpi-hello_world.cpp)
target_link_libraries(HELLO_MPI adios2)

add_executable(WRITE_STEPS_MPI mpi-steps-blocks.cpp)
target_link_libraries(WRITE_STEPS_MPI adios2)

add_executable(WRITE_STEPS localArray_write.cpp)
target_link_libraries(WRITE_STEPS adios2)

add_executable(LOCAL_VS_GLOBAL local-vs-global-arrays.cpp)
target_link_libraries(LOCAL_VS_GLOBAL adios2)


if (ADIOS2_HAVE_MPI)
    target_link_libraries(WRITE_STEPS MPI::MPI_C)
endif()
