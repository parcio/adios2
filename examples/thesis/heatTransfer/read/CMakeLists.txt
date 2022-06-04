#------------------------------------------------------------------------------#
# Distributed under the OSI-approved Apache License, Version 2.0.  See
# accompanying file Copyright.txt for details.
#------------------------------------------------------------------------------#

add_executable(thesis_eval_heatTransfer_read 
  heatRead.cpp 
  PrintDataStep.h 
  ReadSettings.cpp
)
target_link_libraries(thesis_eval_heatTransfer_read adios2::cxx11_mpi MPI::MPI_C)
