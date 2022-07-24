/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaKVInteractionWriter.h"
#include "JuleaKVInteractionWriter.tcc"
#include "adios2/toolkit/interop/julea/JuleaCDO.h"
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"
// #include "JuleaDBDAIInteractionReader.h"
// #include "JuleaMetadata.h"

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
#include <julea-db.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace interop
{

JuleaKVInteractionWriter::JuleaKVInteractionWriter(helper::Comm const &comm)
: JuleaInteraction(std::move(comm))
{
    // std::cout << "This is the constructor of the writer" << std::endl;
}

// TODO: needed?
void JuleaKVInteractionWriter::InitKV(std::string projectNamespace,
                                      bool isOriginalFormat)
{
    // std::cout << "--- InitKV ---" << std::endl;
}
//    template void JuleaKVInteractionWriter::AppendMinMaxToBSON(core::Variable<T> &variable,bson_t *bsonMetadata);\ 

#define declare_template_instantiation(T)                                      \
    template void JuleaKVInteractionWriter::ParseVariableToBSON(               \
        core::Variable<T> &variable, bson_t *bsonMetadata);                    \
    template void JuleaKVInteractionWriter::ParseBlockToBSON(                  \
        core::Variable<T> &variable, bson_t *bsonMetadata);                    \
    template void JuleaKVInteractionWriter::PutVariableMetadataToJulea(        \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName,                 \
        size_t currStep, size_t block, bool original);                         \
    template void JuleaKVInteractionWriter::PutBlockMetadataToJulea(           \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName, size_t step,    \
        size_t block, const typename core::Variable<T>::Info &blockInfo,       \
        T &blockMin, T &blockMax, T &blockMean, T &blockSum, T &blockVar,      \
        uint32_t &entryID, bool original);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios2
