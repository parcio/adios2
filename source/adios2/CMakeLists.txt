#------------------------------------------------------------------------------#
# Distributed under the OSI-approved Apache License, Version 2.0.  See
# accompanying file Copyright.txt for details.
#------------------------------------------------------------------------------#

add_library(taustubs
  toolkit/profiling/taustubs/tautimer.cpp
  toolkit/profiling/taustubs/tautimer.hpp
  toolkit/profiling/taustubs/taustubs.h
)

target_include_directories(taustubs PRIVATE
      $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/source>
)
set_property(TARGET taustubs PROPERTY OUTPUT_NAME adios2${ADIOS2_LIBRARY_SUFFIX}_taustubs)

target_link_libraries(taustubs PRIVATE ${CMAKE_DL_LIBS})

add_library(adios2_core
  common/ADIOSTypes.cpp

  core/Attribute.cpp
  core/AttributeBase.cpp
  core/ADIOS.cpp
  core/Engine.cpp
  core/IO.cpp core/IO.tcc
  core/Operator.cpp
  core/Stream.cpp core/Stream.tcc
  core/Variable.cpp core/Variable.tcc
  core/VariableBase.cpp
  core/VariableCompound.cpp core/VariableCompound.tcc
  core/Group.cpp core/Group.tcc

#operator callback
  operator/callback/Signature1.cpp
  operator/callback/Signature2.cpp

#helper
  helper/adiosComm.h  helper/adiosComm.cpp
  helper/adiosCommDummy.h  helper/adiosCommDummy.cpp
  helper/adiosDynamicBinder.h  helper/adiosDynamicBinder.cpp
  helper/adiosMath.cpp
  helper/adiosMemory.cpp
  helper/adiosNetwork.cpp
  helper/adiosString.cpp helper/adiosString.tcc
  helper/adiosSystem.cpp
  helper/adiosType.cpp
  helper/adiosXML.cpp
  helper/adiosXMLUtil.cpp
  helper/adiosYAML.cpp

#engine derived classes
  engine/bp3/BP3Reader.cpp engine/bp3/BP3Reader.tcc
  engine/bp3/BP3Writer.cpp engine/bp3/BP3Writer.tcc

  engine/bp4/BP4Reader.cpp engine/bp4/BP4Reader.tcc
  engine/bp4/BP4Writer.cpp engine/bp4/BP4Writer.tcc

  engine/skeleton/SkeletonReader.cpp engine/skeleton/SkeletonReader.tcc
  engine/skeleton/SkeletonWriter.cpp engine/skeleton/SkeletonWriter.tcc

  engine/inline/InlineReader.cpp engine/inline/InlineReader.tcc
  engine/inline/InlineWriter.cpp engine/inline/InlineWriter.tcc

  engine/null/NullEngine.cpp

  engine/nullcore/NullCoreWriter.cpp engine/nullcore/NullCoreWriter.tcc
#toolkit
  toolkit/format/buffer/Buffer.cpp
  toolkit/format/buffer/heap/BufferSTL.cpp

  toolkit/format/bp/BPBase.cpp toolkit/format/bp/BPBase.tcc
  toolkit/format/bp/BPSerializer.cpp toolkit/format/bp/BPSerializer.tcc

  toolkit/format/bp/bp3/BP3Base.cpp
  toolkit/format/bp/bp3/BP3Serializer.cpp toolkit/format/bp/bp3/BP3Serializer.tcc
  toolkit/format/bp/bp3/BP3Deserializer.cpp toolkit/format/bp/bp3/BP3Deserializer.tcc

  toolkit/format/bp/bp4/BP4Base.cpp
  toolkit/format/bp/bp4/BP4Serializer.cpp toolkit/format/bp/bp4/BP4Serializer.tcc
  toolkit/format/bp/bp4/BP4Deserializer.cpp toolkit/format/bp/bp4/BP4Deserializer.tcc

  toolkit/format/bp/bpOperation/BPOperation.cpp
  toolkit/format/bp/bpOperation/BPOperation.tcc
  toolkit/format/bp/bpOperation/compress/BPZFP.cpp
  toolkit/format/bp/bpOperation/compress/BPZFP.tcc
  toolkit/format/bp/bpOperation/compress/BPSZ.cpp
  toolkit/format/bp/bpOperation/compress/BPMGARD.cpp
  toolkit/format/bp/bpOperation/compress/BPPNG.cpp
  toolkit/format/bp/bpOperation/compress/BPBZIP2.cpp
  toolkit/format/bp/bpOperation/compress/BPBZIP2.tcc
  toolkit/format/bp/bpOperation/compress/BPBlosc.cpp

  toolkit/profiling/iochrono/Timer.cpp
  toolkit/profiling/iochrono/IOChrono.cpp

  toolkit/query/Query.cpp
  toolkit/query/Worker.cpp
  toolkit/query/XmlWorker.cpp
  toolkit/query/BlockIndex.cpp

  toolkit/transport/Transport.cpp
  toolkit/transport/file/FileStdio.cpp
  toolkit/transport/file/FileFStream.cpp
  toolkit/transport/null/NullTransport.cpp

  toolkit/transportman/TransportMan.cpp

  toolkit/aggregator/mpi/MPIAggregator.cpp
  toolkit/aggregator/mpi/MPIChain.cpp

  toolkit/burstbuffer/FileDrainer.cpp
  toolkit/burstbuffer/FileDrainerSingleThread.cpp
)
set_property(TARGET adios2_core PROPERTY EXPORT_NAME core)
set_property(TARGET adios2_core PROPERTY OUTPUT_NAME adios2${ADIOS2_LIBRARY_SUFFIX}_core)

target_include_directories(adios2_core
  PUBLIC
    $<BUILD_INTERFACE:${ADIOS2_SOURCE_DIR}/source>
    $<BUILD_INTERFACE:${ADIOS2_BINARY_DIR}/source>
)
target_link_libraries(adios2_core PRIVATE adios2sys_interface adios2::thirdparty::pugixml taustubs adios2::thirdparty::yaml-cpp)
target_link_libraries(adios2_core PUBLIC ${CMAKE_THREAD_LIBS_INIT})
target_compile_features(adios2_core PUBLIC "$<BUILD_INTERFACE:${ADIOS2_CXX11_FEATURES}>")

if(UNIX)
  target_sources(adios2_core PRIVATE toolkit/transport/file/FilePOSIX.cpp)
endif()

if(ADIOS2_HAVE_MPI)
  add_library(adios2_core_mpi
    core/IOMPI.cpp
    helper/adiosCommMPI.h  helper/adiosCommMPI.cpp
    helper/adiosMpiHandshake.h  helper/adiosMpiHandshake.cpp
    engine/insitumpi/InSituMPIWriter.cpp engine/insitumpi/InSituMPIWriter.tcc
    engine/insitumpi/InSituMPIReader.cpp engine/insitumpi/InSituMPIReader.tcc
    engine/insitumpi/InSituMPIFunctions.cpp engine/insitumpi/InSituMPISchedules.cpp
  )
  set_property(TARGET adios2_core_mpi PROPERTY EXPORT_NAME core_mpi)
  set_property(TARGET adios2_core_mpi PROPERTY OUTPUT_NAME adios2${ADIOS2_LIBRARY_SUFFIX}_core_mpi)
  target_link_libraries(adios2_core_mpi PUBLIC adios2_core MPI::MPI_C PRIVATE taustubs)
  target_compile_definitions(adios2_core_mpi PUBLIC "$<BUILD_INTERFACE:ADIOS2_USE_MPI>")
endif()

if(ADIOS2_HAVE_SysVShMem)
  target_sources(adios2_core PRIVATE toolkit/format/buffer/ipc/BufferSystemV.cpp)
  target_sources(adios2_core PRIVATE toolkit/transport/shm/ShmSystemV.cpp)
endif()

if(ADIOS2_HAVE_ZeroMQ)
    target_sources(adios2_core PRIVATE
        toolkit/zmq/zmqreqrep/ZmqReqRep.cpp
        toolkit/zmq/zmqpubsub/ZmqPubSub.cpp
        )
    target_link_libraries(adios2_core PRIVATE ZeroMQ::ZMQ)
endif()

if(ADIOS2_HAVE_JULEA)
  target_sources(adios2_core PRIVATE
      # engine/julea-kv-old/JuleaKVReader.cpp
      # engine/julea-kv-old/JuleaKVReader.tcc
      # engine/julea-kv-old/JuleaKVWriter.cpp
      # engine/julea-kv-old/JuleaKVWriter.tcc
      # engine/julea-kv-old/JuleaInteractionWriter.cpp
      # engine/julea-kv-old/JuleaInteractionReader.cpp
      # engine/julea-kv-old/JuleaFormatWriter.cpp
      # engine/julea-kv-old/JuleaFormatReader.cpp

      # engine/julea-kv/JuleaClientLogic-legacy.cpp
      # engine/julea-kv/JuleaFormatWriter-legacy.cpp

      engine/julea-db/JuleaDBReader.cpp
      engine/julea-db/JuleaDBReader.tcc
      engine/julea-db/JuleaDBWriter.cpp
      engine/julea-db/JuleaDBWriter.tcc
      engine/julea-db/JuleaDBInteractionWriter.cpp
      engine/julea-db/JuleaDBInteractionReader.cpp

      engine/julea-db-dai/JuleaDBDAIReader.cpp
      engine/julea-db-dai/JuleaDBDAIReader.tcc
      engine/julea-db-dai/JuleaDBDAIWriter.cpp
      engine/julea-db-dai/JuleaDBDAIWriter.tcc

      engine/julea-kv/JuleaKVReader.cpp
      engine/julea-kv/JuleaKVReader.tcc
      engine/julea-kv/JuleaKVWriter.cpp
      engine/julea-kv/JuleaKVWriter.tcc

      # engine/julea-db-do/JuleaDB-DO-Reader.cpp
      # engine/julea-db-do/JuleaDB-DO-Reader.tcc
      # engine/julea-db-do/JuleaDB-DO-Writer.cpp
      # engine/julea-db-do/JuleaDB-DO-Writer.tcc
      # engine/julea-db-do/JuleaDB-DO-InteractionWriter.cpp
      # engine/julea-db-do/JuleaDB-DO-InteractionReader.cpp

      toolkit/interop/julea/JuleaInteraction.cpp
      toolkit/interop/julea/JuleaInteraction.tcc
      toolkit/interop/julea/JuleaDAI.cpp
      toolkit/interop/julea/JuleaCDO.cpp
      toolkit/interop/julea/Database/JuleaDBInteractionReader.cpp
      toolkit/interop/julea/Database/JuleaDBInteractionReader.tcc
      toolkit/interop/julea/Database/JuleaDBInteractionWriter.cpp
      toolkit/interop/julea/Database/JuleaDBInteractionWriter.tcc
      toolkit/interop/julea/KeyValue/JuleaKVInteractionReader.cpp
      toolkit/interop/julea/KeyValue/JuleaKVInteractionWriter.cpp
  )
  target_include_directories(adios2_core PRIVATE
    ${JULEA_INCLUDE_DIRS}
    ${MPI_INCLUDE_DIRS}
  )
  target_link_libraries(adios2_core PRIVATE
    PkgConfig::JULEA
    MPI::MPI_C
    )
endif()


#if (ADIOS2_HAVE_JSON)
if (ADIOS2_HAVE_DataMan)
    target_sources(adios2_core PRIVATE
	toolkit/query/JsonWorker.cpp
   	)
    target_link_libraries(adios2_core PRIVATE adios2::thirdparty::nlohmann_json)
endif()

if(ADIOS2_HAVE_DataMan OR ADIOS2_HAVE_Table)
    target_sources(adios2_core PRIVATE
        toolkit/format/dataman/DataManSerializer.cpp
        toolkit/format/dataman/DataManSerializer.tcc
    )
    target_link_libraries(adios2_core PRIVATE adios2::thirdparty::nlohmann_json)
endif()


if(ADIOS2_HAVE_DataMan)
    target_sources(adios2_core PRIVATE
        engine/dataman/DataManMonitor.cpp
        engine/dataman/DataManReader.cpp
        engine/dataman/DataManReader.tcc
        engine/dataman/DataManWriter.cpp
        engine/dataman/DataManWriter.tcc
        )
endif()

if(ADIOS2_HAVE_SSC)
    target_sources(adios2_core_mpi PRIVATE
        engine/ssc/SscReader.cpp
        engine/ssc/SscReader.tcc
        engine/ssc/SscWriter.cpp
        engine/ssc/SscWriter.tcc
        engine/ssc/SscHelper.cpp
        )
    target_link_libraries(adios2_core_mpi PRIVATE adios2::thirdparty::nlohmann_json)
    # SSC also activates code in main core library that uses nlohmann_json.
    target_link_libraries(adios2_core PRIVATE adios2::thirdparty::nlohmann_json)
endif()

if(ADIOS2_HAVE_Table)
    target_sources(adios2_core PRIVATE
        engine/table/TableWriter.cpp
        engine/table/TableWriter.tcc
        )
endif()

if(ADIOS2_HAVE_SST)
  add_subdirectory(toolkit/sst)
  target_sources(adios2_core PRIVATE
    engine/sst/SstReader.cpp
    engine/sst/SstWriter.cpp
    engine/sst/SstParamParser.cpp
  )
  target_link_libraries(adios2_core PRIVATE sst)
endif()

if(ADIOS2_HAVE_DataSpaces)
  target_sources(adios2_core_mpi PRIVATE
     toolkit/dataspaces/ds_writer.c
     engine/dataspaces/DataSpacesWriter.cpp engine/dataspaces/DataSpacesWriter.tcc
     engine/dataspaces/DataSpacesReader.cpp engine/dataspaces/DataSpacesReader.tcc
  )
  target_link_libraries(adios2_core_mpi PRIVATE DataSpaces::DataSpaces)
endif()

if(ADIOS2_HAVE_Blosc)
  target_sources(adios2_core PRIVATE operator/compress/CompressBlosc.cpp)
  target_link_libraries(adios2_core PRIVATE Blosc::Blosc)
endif()

if(ADIOS2_HAVE_BZip2)
  target_sources(adios2_core PRIVATE operator/compress/CompressBZIP2.cpp)
  target_link_libraries(adios2_core PRIVATE BZip2::BZip2)
endif()

if(ADIOS2_HAVE_ZFP)
  target_sources(adios2_core PRIVATE operator/compress/CompressZFP.cpp)
  target_link_libraries(adios2_core PRIVATE zfp::zfp)
endif()

if(ADIOS2_HAVE_SZ)
  target_sources(adios2_core PRIVATE operator/compress/CompressSZ.cpp)
  target_link_libraries(adios2_core PRIVATE SZ::SZ)
endif()

if(ADIOS2_HAVE_MGARD)
  target_sources(adios2_core PRIVATE operator/compress/CompressMGARD.cpp)
  target_link_libraries(adios2_core PRIVATE MGARD::MGARD)
endif()

if(ADIOS2_HAVE_PNG)
  target_sources(adios2_core PRIVATE operator/compress/CompressPNG.cpp)
  target_link_libraries(adios2_core PRIVATE PNG::PNG)
endif()

if(ADIOS2_HAVE_HDF5)
  add_library(adios2_hdf5 OBJECT
    core/IOHDF5.cpp
    engine/hdf5/HDF5ReaderP.cpp
    engine/hdf5/HDF5WriterP.cpp
    toolkit/interop/hdf5/HDF5Common.cpp toolkit/interop/hdf5/HDF5Common.tcc
    )
  if(NOT HDF5_VERSION VERSION_LESS 1.11)
    target_sources(adios2_hdf5 PRIVATE
      engine/mixer/HDFMixer.cpp
      engine/mixer/HDFMixer.tcc
      engine/mixer/HDFMixerWriter.cpp
      )
  endif()
  target_link_libraries(adios2_core PRIVATE adios2_hdf5)
  set_property(TARGET adios2_hdf5 PROPERTY EXPORT_NAME hdf5)
  set_property(TARGET adios2_hdf5 PROPERTY OUTPUT_NAME adios2${ADIOS2_LIBRARY_SUFFIX}_hdf5)

  set(adios2_hdf5_objs adios2_hdf5)
  if(HDF5_IS_PARALLEL)
    list(APPEND adios2_hdf5_objs adios2_hdf5_mpi)
    set_property(SOURCE core/IOMPI.cpp APPEND PROPERTY COMPILE_DEFINITIONS ADIOS2_HAVE_HDF5_PARALLEL)
    add_library(adios2_hdf5_mpi OBJECT
      toolkit/interop/hdf5/HDF5CommonMPI.cpp
      )
    target_compile_definitions(adios2_hdf5_mpi PRIVATE ADIOS2_USE_MPI)
    target_link_libraries(adios2_core_mpi PRIVATE adios2_hdf5_mpi)
    set_property(TARGET adios2_hdf5_mpi PROPERTY EXPORT_NAME hdf5_mpi)
    set_property(TARGET adios2_hdf5_mpi PROPERTY OUTPUT_NAME adios2${ADIOS2_LIBRARY_SUFFIX}_hdf5_mpi)
  endif()

  foreach(lib IN LISTS adios2_hdf5_objs)
    target_link_libraries(${lib} PRIVATE ${HDF5_C_LIBRARIES})
    if(HDF5_IS_PARALLEL)
      target_link_libraries(${lib} PRIVATE $<BUILD_INTERFACE:MPI::MPI_C>)
    endif()
    if(BUILD_SHARED_LIBS)
      set_property(TARGET ${lib} PROPERTY POSITION_INDEPENDENT_CODE 1)
    else()
      # When this is included in a static library we must export the
      # target so that consumers know the link dependencies.
      install(TARGETS ${lib} EXPORT adios2Exports)
    endif()
    target_include_directories(${lib} PRIVATE
      ${ADIOS2_SOURCE_DIR}/source
      ${ADIOS2_BINARY_DIR}/source
      )

    if(HDF5_C_INCLUDE_DIRS)
      target_include_directories(${lib} PRIVATE ${HDF5_C_INCLUDE_DIRS})
    else()
      target_include_directories(${lib} PRIVATE ${HDF5_INCLUDE_DIRS})
    endif()

    if(CMAKE_VERSION VERSION_LESS 3.14)
      # CMake < 3.14 forgets to propagate private dependencies of object
      # libraries.  Propagate them the same way CMake >= 3.14 do.
      # FIXME: Drop this workaround when we require CMake 3.14.
      get_property(deps TARGET ${lib} PROPERTY LINK_LIBRARIES)
      foreach(dep ${deps})
        target_link_libraries(${lib} INTERFACE "$<LINK_ONLY:${dep}>")
      endforeach()
    endif()
  endforeach()

endif()

if(ADIOS2_HAVE_IME)
  target_sources(adios2_core PRIVATE toolkit/transport/file/FileIME.cpp)
  target_link_libraries(adios2_core PRIVATE IME::IME)
endif()

if(ADIOS2_HAVE_MPI)
  set(maybe_adios2_c_mpi adios2_c_mpi)
  set(maybe_adios2_cxx11_mpi adios2_cxx11_mpi)
  set(maybe_adios2_core_mpi adios2_core_mpi)
else()
  set(maybe_adios2_c_mpi)
  set(maybe_adios2_cxx11_mpi)
  set(maybe_adios2_core_mpi)
endif()

# The main "adios2" target provides all interfaces.
add_library(adios2 INTERFACE)
target_link_libraries(adios2 INTERFACE
  ${maybe_adios2_c_mpi} adios2_c
  ${maybe_adios2_cxx11_mpi} adios2_cxx11
  )

# Set library version information
set_target_properties(
  adios2_core ${maybe_adios2_core_mpi}
  PROPERTIES
  VERSION ${ADIOS2_LIBRARY_VERSION}
  SOVERSION ${ADIOS2_VERSION_MAJOR}
)

install(FILES common/ADIOSMacros.h common/ADIOSTypes.h common/ADIOSTypes.inl
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/adios2/common COMPONENT adios2_core-development
)

# Library installation
install(TARGETS adios2_core ${maybe_adios2_core_mpi} taustubs EXPORT adios2Exports
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT adios2_core-runtime
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT adios2_core-libraries NAMELINK_COMPONENT adios2_core-development
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT adios2_core-development
)
