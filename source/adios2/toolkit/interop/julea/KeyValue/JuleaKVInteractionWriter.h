/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONWRITER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONWRITER_H_

// #include "JuleaMetadata.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop
// namespace!
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

#include "adios2/common/ADIOSMacros.h"
#include "adios2/common/ADIOSTypes.h"
#include "adios2/core/IO.h" // for CreateVar
#include "adios2/core/Variable.h"

#include <julea.h>

#include <string>

#include <stdexcept> // for Intel Compiler

namespace adios2
{
namespace interop
{

class JuleaKVInteractionWriter : public JuleaInteraction
{

public:
    JuleaKVInteractionWriter(helper::Comm const &comm);
    ~JuleaKVInteractionWriter() = default;

    /** --- Variables --- */
    void InitKV(std::string projectNamespace, bool isOriginalFormat);
    // void InitTagTables(std::string projectNamespace);

    /**
     *  Writes name of a variable to Julea KV. Also checks if name is
     * already in kv.
     */
    void PutVarNameToJulea(std::string const projectNamespace,
                           std::string const fileName,
                           std::string const varName);

    /**
     * Put the metadata for a specific block in a specific step to JULEA
     * database.
     */
    template <class T>
    void PutVariableMetadataToJulea(core::Variable<T> &variable,
                                    const std::string projectNamespace,
                                    const std::string fileName,
                                    const std::string varName, size_t currStep,
                                    size_t block, bool original);
    /**
     * Put the metadata for a specific block in a specific step to JULEA
     * database.
     */
    template <class T>
    void PutBlockMetadataToJulea(
        core::Variable<T> &variable, const std::string projectNamespace,
        const std::string fileName, const std::string varName, size_t step,
        size_t block, const typename core::Variable<T>::Info &blockInfo,
        T &blockMin, T &blockMax, T &blockMean, T &blockSum, T &blockVar,
        uint32_t &entryID, bool original);

    template <class T>
    void ParseVariableToBSON(core::Variable<T> &variable, bson_t *bsonMetadata,
                             size_t currentStep);

    template <class T>
    void ParseBlockToBSON(core::Variable<T> &variable, bson_t *bsonMetadata,
                          T blockMin, T blockMax);

    template <class T>
    void AppendMinMaxValueToBSON(core::Variable<T> &variable,
                                 bson_t *bsonMetadata);

private:
}; // end namespace JuleaKVInteractionWriter

// extern template void JuleaKVInteractionWriter::AppendMinMaxToBSON(core::Variable<T> &variable,bson_t *bsonMetadata);\

#define declare_template_instantiation(T)                                      \
    extern template void JuleaKVInteractionWriter::ParseVariableToBSON(        \
        core::Variable<T> &variable, bson_t *bsonMetadata,                     \
        size_t currentStep);                                                   \
    extern template void JuleaKVInteractionWriter::ParseBlockToBSON(           \
        core::Variable<T> &variable, bson_t *bsonMetadata, T blockMin,         \
        T blockMax);                                                           \
    extern template void JuleaKVInteractionWriter::PutVariableMetadataToJulea( \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName,                 \
        size_t currStep, size_t block, bool original);                         \
    extern template void JuleaKVInteractionWriter::PutBlockMetadataToJulea(    \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName, size_t step,    \
        size_t block, const typename core::Variable<T>::Info &blockInfo,       \
        T &blockMin, T &blockMax, T &blockMean, T &blockSum, T &blockVar,      \
        uint32_t &entryID, bool original);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONWRITER_H_ */
