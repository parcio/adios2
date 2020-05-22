/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEADBINTERACTIONREADERTEST_H_
#define ADIOS2_ENGINE_JULEADBINTERACTIONREADERTEST_H_

#include "JuleaDBReader.h"
// #include "JuleaMetadata.h"

namespace adios2
{
namespace core
{
namespace engine
{
void setMinMaxValueFields(std::string *minField, std::string *maxField,
                          std::string *valueField, const char *varType);

void DBDefineVariableInInit(core::IO *io, const std::string varName,
                            std::string type, Dims shape, Dims start,
                            Dims count, bool constantDims, bool isLocalValue);

void CheckSchemas();

void InitVariablesFromDB(const std::string nameSpace, core::IO *io,
                         core::Engine &engine);

/**
 * Deserialize the metadata of a single block of a step of a variable.
 * @param variable         variable
 * @param buffer           metadata buffer from JULEA key-value store
 * @param blockID          blockID (0 index)
 * @param info             info struct to store block infos in
 */
// template <class T>
// void DeserializeBlockMetadata(Variable<T> &variable, gpointer buffer,
//                               size_t blockID,
//                               typename core::Variable<T>::Info &info);
template <class T>
void GetCountFromBlockMetadata(const std::string nameSpace,
                               const std::string varName, size_t step,
                               size_t block, Dims *count, size_t entryID,
                               bool isLocalValue, T *value);

template <class T>
std::unique_ptr<typename core::Variable<T>::Info>
DBGetBlockMetadata(const core::Variable<T> &variable,
                   const std::string nameSpace, size_t step, size_t block,
                   size_t entryID);
/* --- Variables --- */

/** Retrieves all variable names from key-value store. They are all stored
 * in one bson. */
void DBGetNamesFromJulea(const std::string nameSpace, bson_t **bsonNames,
                         unsigned int *varCount, bool isVariable);

/** Retrieves the metadata buffer for the variable metadata that do not vary
 * from block to block. The key is the variable name. */
void DBGetVariableMetadataFromJulea(const std::string nameSpace,
                                    const std::string varName, gpointer *buffer,
                                    guint32 *buffer_len);

/** Retrieves the block metadata buffer from the key-value store. The key is:
 * currentStep_currentBlock. The variable name and the nameSpace from the
 * key-value namespace. */
void DBGetBlockMetadataFromJulea(const std::string nameSpace,
                                 const std::string varName, gpointer *buffer,
                                 guint32 *buffer_len,
                                 const std::string stepBlockID);

/** Passing the data pointer from application to the key-value store. Space for
 * the data is allocated in the application. */
template <class T>
void DBGetVariableDataFromJulea(Variable<T> &variable, T *data,
                                const std::string nameSpace,
                                long unsigned int dataSize,
                                const std::string stepBlockID);

#define variable_template_instantiation(T)                                     \
    extern template void GetCountFromBlockMetadata(                            \
        const std::string nameSpace, const std::string varName, size_t step,   \
        size_t block, Dims *count, size_t entryID, bool isLocalValue,          \
        T *value);                                                             \
    extern template std::unique_ptr<typename core::Variable<T>::Info>          \
    DBGetBlockMetadata(const core::Variable<T> &variable,                      \
                       const std::string nameSpace, size_t step, size_t block, \
                       size_t entryID);                                        \
                                                                               \
    extern template void DBGetVariableDataFromJulea(                           \
        Variable<T> &variable, T *data, const std::string nameSpace,           \
        long unsigned int dataSize, const std::string stepBlockID);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

/* --- Attributes --- */
/** Attributes are still implemented using BSON. It was not urgent enough to
 * change anything. */
void DBGetAttributeMetadataFromJulea(const std::string attrName,
                                     const std::string nameSpace,
                                     long unsigned int *dataSize,
                                     size_t *numberElements,
                                     bool *IsSingleValue, int *type);

void DBGetAttributeMetadataFromJulea(const std::string attrName,
                                     const std::string nameSpace,
                                     long unsigned int *completeSize,
                                     size_t *numberElements,
                                     bool *IsSingleValue, int *type,
                                     unsigned long **dataSizes);

void DBGetAttributeBSONFromJulea(const std::string nameSpace,
                                 const std::string varName,
                                 bson_t **bsonMetadata, guint32 *valueLen);
template <class T>
void DBGetAttributeDataFromJulea(const std::string attrName, T *data,
                                 const std::string nameSpace,
                                 long unsigned int dataSize);

void DBGetAttributeStringDataFromJulea(const std::string attrName, char *data,
                                       const std::string nameSpace,
                                       long unsigned int completeSize,
                                       bool IsSingleValue,
                                       size_t numberElements);

#define attribute_template_instantiation(T)                                    \
    extern template void DBGetAttributeDataFromJulea(                          \
        const std::string attrName, T *data, const std::string nameSpace,      \
        long unsigned int dataSize);

ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAINTERACTIONREADERTEST_H_ */
