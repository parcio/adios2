/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAINTERACTIONREADERTEST_H_
#define ADIOS2_ENGINE_JULEAINTERACTIONREADERTEST_H_

#include "JuleaKVReader.h"
#include "JuleaMetadata.h"

namespace adios2
{
namespace core
{
namespace engine
{

/* --- Variables --- */

/** Retrieves all variable names from key-value store. They are all stored in
 * one bson. */
void GetNamesFromJulea(const std::string nameSpace, bson_t **bsonNames,
                       unsigned int *varCount, bool isVariable);

/** Retrieves the metadata buffer for the variable metadata that do not vary
 * from block to block. The key is the variable name. */
void GetVariableMetadataFromJulea(const std::string nameSpace,
                                  const std::string varName, gpointer *buffer,
                                  guint32 *buffer_len);

/** Retrieves the block metadata buffer from the key-value store. The key is:
 * currentStep_currentBlock. The variable name and the nameSpace from the
 * key-value namespace. */
void GetBlockMetadataFromJulea(const std::string nameSpace,
                               const std::string varName, gpointer *buffer,
                               guint32 *buffer_len,
                               const std::string stepBlockID);

/** Passing the data pointer from application to the key-value store. Space for
 * the data is allocated in the application. */
template <class T>
void GetVariableDataFromJulea(Variable<T> &variable, T *data,
                              const std::string nameSpace,
                              long unsigned int dataSize, const std::string stepBlockID);

#define variable_template_instantiation(T)                                     \
    extern template void GetVariableDataFromJulea(                             \
        Variable<T> &variable, T *data, const std::string nameSpace,           \
        long unsigned int dataSize,const std::string stepBlockID);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

/* --- Attributes --- */
/** Attributes are still implemented using BSON. It was not urgent enough to
 * change anything. */
void GetAttributeMetadataFromJulea(const std::string attrName,
                                   const std::string nameSpace,
                                   long unsigned int *dataSize,
                                   size_t *numberElements, bool *IsSingleValue,
                                   int *type);

void GetAttributeMetadataFromJulea(const std::string attrName,
                                   const std::string nameSpace,
                                   long unsigned int *completeSize,
                                   size_t *numberElements, bool *IsSingleValue,
                                   int *type, unsigned long **dataSizes);

void GetAttributeBSONFromJulea(const std::string nameSpace,
                               const std::string varName, bson_t **bsonMetadata,
                               guint32 *valueLen);
template <class T>
void GetAttributeDataFromJulea(const std::string attrName, T *data,
                               const std::string nameSpace,
                               long unsigned int dataSize);

void GetAttributeStringDataFromJulea(const std::string attrName, char *data,
                                     const std::string nameSpace,
                                     long unsigned int completeSize,
                                     bool IsSingleValue, size_t numberElements);

#define attribute_template_instantiation(T)                                    \
    extern template void GetAttributeDataFromJulea(                            \
        const std::string attrName, T *data, const std::string nameSpace,      \
        long unsigned int dataSize);

ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAINTERACTIONREADERTEST_H_ */
