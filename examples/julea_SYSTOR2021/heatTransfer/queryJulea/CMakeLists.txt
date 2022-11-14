#------------------------------------------------------------------------------#
# Distributed under the OSI-approved Apache License, Version 2.0.  See
# accompanying file Copyright.txt for details.
#------------------------------------------------------------------------------#

add_executable(julea_heatTransfer_queryJulea
  queryJulea.cpp 
  JuleaQueryPrintDataStep.h 
  JuleaQuerySettings.cpp
)
target_link_libraries(julea_heatTransfer_queryJulea adios2::cxx11_mpi MPI::MPI_C)
target_link_libraries(julea_heatTransfer_queryJulea PkgConfig::JULEA)
