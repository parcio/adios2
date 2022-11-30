#------------------------------------------------------------------------------#
# Distributed under the OSI-approved Apache License, Version 2.0.  See
# accompanying file Copyright.txt for details.
#------------------------------------------------------------------------------#

add_subdirectory(basics)
add_subdirectory(hello)
add_subdirectory(query)
add_subdirectory(useCases)
add_subdirectory(inlineMWE)

if(ADIOS2_HAVE_MPI)
  add_subdirectory(heatTransfer)
endif()

if(ADIOS2_HAVE_JULEA)
  add_subdirectory(julea)
  add_subdirectory(julea_SYSTOR2021)
  add_subdirectory(thesis)
endif()

if(ADIOS2_HAVE_NetCDF AND ADIOS2_HAVE_JULEA)
  add_subdirectory(importNC)
endif()


add_subdirectory(importNC)
if(ADIOS2_BUILD_EXAMPLES_EXPERIMENTAL)
  add_subdirectory(experimental)
endif()
