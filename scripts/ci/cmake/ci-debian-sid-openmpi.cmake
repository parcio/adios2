# Client maintainer: chuck.atkins@kitware.com

set(dashboard_cache "
ADIOS2_USE_EXTERNAL_DEPENDENCIES:BOOL=ON
ADIOS2_USE_EXTERNAL_EVPATH:BOOL=OFF
ADIOS2_USE_BZip2:BOOL=ON
ADIOS2_USE_Blosc:BOOL=ON
ADIOS2_USE_Fortran:BOOL=ON
ADIOS2_USE_HDF5:BOOL=ON
ADIOS2_USE_MPI:BOOL=ON
ADIOS2_USE_PNG:BOOL=ON
ADIOS2_USE_Python:BOOL=ON
ADIOS2_USE_SSC:BOOL=ON
ADIOS2_USE_SST:BOOL=ON
ADIOS2_USE_ZeroMQ:BOOL=ON
ADIOS2_LIBRARY_SUFFIX:STRING=_openmpi
ADIOS2_EXECUTABLE_SUFFIX:STRING=.openmpi

MPIEXEC_EXTRA_FLAGS:STRING=--allow-run-as-root --oversubscribe
")

set(CTEST_TEST_ARGS
  PARALLEL_LEVEL 1
)
set(CTEST_CMAKE_GENERATOR "Ninja")

set(ADIOS_TEST_REPEAT 0)
list(APPEND CTEST_UPDATE_NOTES_FILES "${CMAKE_CURRENT_LIST_FILE}")
include(${CMAKE_CURRENT_LIST_DIR}/ci-common.cmake)
