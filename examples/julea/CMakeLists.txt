#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

if(ADIOS2_HAVE_JULEA)
    add_executable(BITTE_BITTE JuleaEngineTest.cpp)
    target_link_libraries(BITTE_BITTE adios2)
    target_link_libraries(BITTE_BITTE PkgConfig::JULEA)

    add_executable(DAI_TestExample JuleaDAITest.cpp)
    target_link_libraries(DAI_TestExample adios2)
    target_link_libraries(DAI_TestExample PkgConfig::JULEA)

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

    add_executable(GLOBAL_ARRAY_WRITE globalArray_write.cpp)
    target_link_libraries(GLOBAL_ARRAY_WRITE adios2)
endif()

if (ADIOS2_HAVE_MPI AND ADIOS2_HAVE_JULEA )
	add_executable(GLOBAL_ARRAY_WRITE_mpi globalArray_write.cpp)
    target_link_libraries(GLOBAL_ARRAY_WRITE_mpi adios2::cxx11_mpi MPI::MPI_C)
    target_link_libraries(WRITE_STEPS MPI::MPI_C)
endif()
