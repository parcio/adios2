/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEADBINTERACTION_H_
#define ADIOS2_ENGINE_JULEADBINTERACTION_H_

#include "JuleaDBWriter.h"
// #include "JuleaMetadata.h"

namespace adios2
{
namespace core
{
namespace engine
{

/** --- Variables --- */
void InitDBSchemas();

/**
 * Put the variable metadata that does not change from block to block into the
 * JULEA key-value store
 * @param nameSpace file name
 * @param buffer    buffer of serialized metadata
 * @param bufferLen length of buffer
 * @param varName   variable name = key for the kv store
 */
template <class T>
void DBPutVariableMetadataToJulea(Variable<T> &variable,
                                  const std::string nameSpace,
                                  const std::string varName, size_t currStep);
/**
 * Put the metadata for a specific block in a specific step to JULEA key-value
 * store.
 * @param nameSpace   file name
 * @param varName     variableName; is part of the kv-namespace
 * @param buffer      buffer of serialized metadata
 * @param bufferLen   length of buffer
 * @param stepBlockID key for the kv-store: currentStep_currentBlock
 */
template <class T>
void DBPutBlockMetadataToJulea(Variable<T> &variable,
                               const std::string nameSpace,
                               const std::string varName, size_t step,
                               size_t block,
                               const typename Variable<T>::Info &blockInfo);

/**
 * Store variable data in JULEA object store. The key is:
 * currentStep_currentBlock. Variable name is in the kv-namespace.
 * @param variable      variable
 * @param data data     pointer
 * @param nameSpace     file name
 * @param currentStep   current step (part of key)
 * @param blockID       current block (part of key)
 */
template <class T>
void DBPutVariableDataToJulea(Variable<T> &variable, const T *data,
                              const std::string nameSpace, size_t currentStep,
                              size_t blockID);

/** --- Attributes --- */
template <class T>
void DBPutAttributeDataToJulea(Attribute<T> &attribute,
                               const std::string nameSpace);

template <class T>
void DBPutAttributeMetadataToJulea(Attribute<T> &attribute,
                                   bson_t *bsonMetadata,
                                   const std::string nameSpace);

template <class T>
void DBPutAttributeDataToJuleaSmall(Attribute<T> &attribute, const T *data,
                                    const std::string nameSpace);
template <class T>
void DBPutAttributeMetadataToJuleaSmall(Attribute<T> &attribute,
                                        bson_t *bsonMetadata,
                                        const std::string nameSpace);

#define declare_template_instantiation(T)                                      \
    extern template void DBPutVariableDataToJulea(                             \
        Variable<T> &variable, const T *data, const std::string nameSpace,     \
        size_t currentStep, size_t blockID);                                   \
    extern template void DBPutVariableMetadataToJulea(                         \
        Variable<T> &variable, const std::string nameSpace,                    \
        const std::string varName, size_t currStep);                           \
    extern template void DBPutBlockMetadataToJulea(                            \
        Variable<T> &variable, const std::string nameSpace,                    \
        const std::string varName, size_t step, size_t block,                  \
        const typename Variable<T>::Info &blockInfo);                          \
                                                                               \
    extern template void DBPutAttributeDataToJulea(                            \
        Attribute<T> &attribute, const std::string nameSpace);                 \
    extern template void DBPutAttributeDataToJuleaSmall(                       \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    extern template void DBPutAttributeMetadataToJulea(                        \
        Attribute<T> &attribute, bson_t *bsonMetadata,                         \
        const std::string nameSpace);                                          \
    extern template void DBPutAttributeMetadataToJuleaSmall(                   \
        Attribute<T> &attribute, bson_t *bsonMetadata,                         \
        const std::string nameSpace);                                          \
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAINTERACTION_H_ */
