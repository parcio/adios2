#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #
#Distributed under the OSI - approved Apache License, Version 2.0. See
#accompanying file Copyright.txt for details.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- #

add_executable(thesis_eval_heatTransfer_queryDAI queryJulea.cpp JuleaQueryPrintDataStep.h JuleaQuerySettings.cpp)
    target_link_libraries(
        thesis_eval_heatTransfer_queryDAI adios2::cxx11_mpi MPI::MPI_C)
        target_link_libraries(
            thesis_eval_heatTransfer_queryDAI PkgConfig::JULEA)
