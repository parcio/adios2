if (ADIOS2_HAVE_NetCDF AND ADIOS2_HAVE_JULEA )
    set(NC_SOURCES ./import_nc/import_nc.cpp 
    # ./import_nc/nc_read.cpp
    ./import_nc/benchmark_source/nc_read.cpp ./import_nc/benchmark_source/nc_read.h)
    add_executable(import_nc ${NC_SOURCES})
    target_link_libraries(import_nc adios2_core adios2sys)
    target_link_libraries(import_nc PkgConfig::NetCDF)
endif()
