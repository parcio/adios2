/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONWRITER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONWRITER_H_

#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

// #include "JuleaDBDAIWriter.h"
// #include "JuleaMetadata.h"

namespace adios2
{
namespace interop
{

class JuleaDBInteractionWriter : public JuleaInteraction
{

public:
    JuleaDBInteractionWriter(helper::Comm const &comm);
    ~JuleaDBInteractionWriter() = default;
    /** --- Variables --- */
    void InitDBSchemas();

    /**
     * Put the variable metadata that does not change from block to block into
     * the JULEA key-value store
     * @param nameSpace file name
     * @param buffer    buffer of serialized metadata
     * @param bufferLen length of buffer
     * @param varName   variable name = key for the kv store
     */
    template <class T>
    void PutVariableMetadataToJulea(core::Variable<T> &variable,
                                    const std::string nameSpace,
                                    const std::string varName, size_t currStep,
                                    size_t block);
    /**
     * Put the metadata for a specific block in a specific step to JULEA
     * key-value store.
     * @param nameSpace   file name
     * @param varName     variableName; is part of the kv-namespace
     * @param buffer      buffer of serialized metadata
     * @param bufferLen   length of buffer
     * @param stepBlockID key for the kv-store: currentStep_currentBlock
     */
    template <class T>
    void PutBlockMetadataToJulea(
        core::Variable<T> &variable, const std::string nameSpace,
        const std::string varName, size_t step, size_t block,
        const typename core::Variable<T>::Info &blockInfo, T &blockMin,
        T &blockMax, T &blockMean, uint32_t &entryID);

    // /**
    //  * Store variable data in JULEA object store. The key is:
    //  * currentStep_currentBlock. Variable name is in the kv-namespace.
    //  * @param variable      variable
    //  * @param data data     pointer
    //  * @param nameSpace     file name
    //  * @param currentStep   current step (part of key)
    //  * @param blockID       current block (part of key)
    //  */
    // template <class T>
    // void DBPutVariableDataToJulea(Variable<T> &variable, const T *data,
    //                               const std::string nameSpace, size_t
    //                               currentStep, size_t blockID);

    // template <class T>
    // void DAIDBPutVariableDataToJulea(Variable<T> &variable, const T *data,
    //                               const std::string nameSpace, uint32_t
    //                               entryID);

    /** --- Attributes --- */
    // template <class T>
    // void DAIDBPutAttributeMetadataToJulea(Attribute<T> &attribute,
    //                                    bson_t *bsonMetadata,
    //                                    const std::string nameSpace);
    // template <class T>
    // void DAIDBPutAttributeMetadataToJuleaSmall(Attribute<T> &attribute,
    //                                         bson_t *bsonMetadata,
    //                                         const std::string nameSpace);

    // #define declare_template_instantiation(T)                                      \
//     extern template void JuleaSerializer::Write(core::Variable<T> &variable,   \
//                                                 const T *value);
    // ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
    // #undef declare_template_instantiation

    // TODO: THIS WORKS WHY NOT MINE?
    // #define declare_template_instantiation(T) \
//     extern template void HDF5Common::Write(core::Variable<T> &variable, \
//                                            const T *value);
    // ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
    // #undef declare_template_instantiation

    // #define declare_template_instantiation(T)                                      \
//     extern template void JuleaDBInteractionWriter::DAIDBPutVariableDataToJulea(                             \
//         core::Variable<T> &variable, const T *data, const std::string nameSpace,     \
//         uint32_t entryID);                                                     \
//     extern template void JuleaDBInteractionWriter::DAIDBPutVariableMetadataToJulea(                         \
//         core::Variable<T> &variable, const std::string nameSpace,                    \
//         const std::string varName, size_t currStep, size_t block);             \
//     extern template void JuleaDBInteractionWriter::DAIDBPutBlockMetadataToJulea(                            \
//         core::Variable<T> &variable, const std::string nameSpace,                    \
//         const std::string varName, size_t step, size_t block,                  \
//         const typename Variable<T>::Info &blockInfo, , T &blockMin,            \
//         T &blockMax, T &blockMean, uint32_t &entryID);
    //     ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
    // #undef declare_template_instantiation

    // extern template void DAIDBPutAttributeDataToJulea(                            \
    //     Attribute<T> &attribute, const std::string nameSpace);                 \
    // extern template void DAIDBPutAttributeDataToJuleaSmall(                       \
    //     Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    // extern template void DAIDBPutAttributeMetadataToJulea(                        \
    //     Attribute<T> &attribute, bson_t *bsonMetadata,                         \
    //     const std::string nameSpace);                                          \
    // extern template void DAIDBPutAttributeMetadataToJuleaSmall(                   \
    //     Attribute<T> &attribute, bson_t *bsonMetadata,                         \
    //     const std::string nameSpace);                                          \
    // ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
    // #undef declare_template_instantiation

}; // end namespace JuleaDBInteractionWriter

// TODO: check whether there is any parameter missing; also, maybe blockInfo
// does not need to be passed?

#define declare_template_instantiation(T)                                      \
    extern template void JuleaDBInteractionWriter::PutVariableMetadataToJulea( \
        core::Variable<T> &variable, const std::string nameSpace,              \
        const std::string varName, size_t currStep, size_t block);             \
    extern template void JuleaDBInteractionWriter::PutBlockMetadataToJulea(    \
        core::Variable<T> &variable, const std::string nameSpace,              \
        const std::string varName, size_t step, size_t block,                  \
        const typename core::Variable<T>::Info &blockInfo, T &blockMin,        \
        T &blockMax, T &blockMean, uint32_t &entryID);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios2

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONWRITER_H_ */
