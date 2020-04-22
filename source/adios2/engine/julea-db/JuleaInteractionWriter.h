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
/**
 * Stores variable name in a BSON in the key-value store.
 * @param paramName name of attribute/variable
 * @param nameSpace file name
 * @param kvName    name of the key value store (variable_names/attribute_names)
 */
void PutNameToJulea(std::string paramName, std::string nameSpace,
                    std::string kvName);

/** --- Variables --- */

/**
 * Put the variable metadata that does not change from block to block into the
 * JULEA key-value store
 * @param nameSpace file name
 * @param buffer    buffer of serialized metadata
 * @param bufferLen length of buffer
 * @param varName   variable name = key for the kv store
 */
void PutVariableMetadataToJulea(const std::string nameSpace, gpointer buffer,
                                guint32 bufferLen, const std::string varName);
/**
 * Put the metadata for a specific block in a specific step to JULEA key-value
 * store.
 * @param nameSpace   file name
 * @param varName     variableName; is part of the kv-namespace
 * @param buffer      buffer of serialized metadata
 * @param bufferLen   length of buffer
 * @param stepBlockID key for the kv-store: currentStep_currentBlock
 */
void PutBlockMetadataToJulea(const std::string nameSpace,
                             const std::string varName, gpointer &buffer,
                             guint32 bufferLen, const std::string stepBlockID);

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
void PutVariableDataToJulea(Variable<T> &variable, const T *data,
                            const std::string nameSpace, size_t currentStep,
                            size_t blockID);

/** --- Attributes --- */
template <class T>
void PutAttributeDataToJulea(Attribute<T> &attribute,
                             const std::string nameSpace);

template <class T>
void PutAttributeMetadataToJulea(Attribute<T> &attribute, bson_t *bsonMetadata,
                                 const std::string nameSpace);

template <class T>
void PutAttributeDataToJuleaSmall(Attribute<T> &attribute, const T *data,
                                  const std::string nameSpace);
template <class T>
void PutAttributeMetadataToJuleaSmall(Attribute<T> &attribute,
                                      bson_t *bsonMetadata,
                                      const std::string nameSpace);

#define declare_template_instantiation(T)                                      \
    extern template void PutVariableDataToJulea(                               \
        Variable<T> &variable, const T *data, const std::string nameSpace,     \
        size_t currentStep, size_t blockID);                                   \
                                                                               \
    extern template void PutAttributeDataToJulea(Attribute<T> &attribute,      \
                                                 const std::string nameSpace); \
    extern template void PutAttributeDataToJuleaSmall(                         \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    extern template void PutAttributeMetadataToJulea(                          \
        Attribute<T> &attribute, bson_t *bsonMetadata,                         \
        const std::string nameSpace);                                          \
    extern template void PutAttributeMetadataToJuleaSmall(                     \
        Attribute<T> &attribute, bson_t *bsonMetadata,                         \
        const std::string nameSpace);                                          \
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAINTERACTION_H_ */
