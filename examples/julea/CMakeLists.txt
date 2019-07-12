#------------------------------------------------------------------------------#
# Distributed under the OSI-approved Apache License, Version 2.0.  See
# accompanying file Copyright.txt for details.
#------------------------------------------------------------------------------#

add_executable(BITTE_BITTE JuleaEngineTest.cpp)

target_link_libraries(BITTE_BITTE adios2)
#target_link_libraries(BITTE_BITTE /home/kira/Dokumente/julea/build/lib/libjulea.so)
target_link_libraries(BITTE_BITTE /home/elentari/Documents/julea/build/lib/libjulea.so)

