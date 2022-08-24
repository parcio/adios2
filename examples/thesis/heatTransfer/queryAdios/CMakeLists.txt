#------------------------------------------------------------------------------#
# Distributed under the OSI-approved Apache License, Version 2.0.  See
# accompanying file Copyright.txt for details.
#------------------------------------------------------------------------------#

add_executable(
  # julea_heatTransfer_queryAdios
  thesis_eval_heatTransfer_queryAdios
  queryAdios.cpp 
  AdiosQueryPrintDataStep.h 
  AdiosQuerySettings.cpp
)
target_link_libraries(thesis_eval_heatTransfer_queryAdios adios2::cxx11_mpi MPI::MPI_C)
