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


/* Variables */
void GetNamesFromJulea(const std::string nameSpace, bson_t **bsonNames,
                       unsigned int *varCount, bool isVariable);


void GetVariableMetadataFromJulea(const std::string nameSpace,
                                  const std::string varName, gpointer *md,
                                  guint32 *buffer_len);

void GetBlockMetadataFromJulea(const std::string nameSpace,
                               const std::string varName, gpointer *md,
                               guint32 *buffer_len,
                               const std::string stepBlockID);

template <class T>
void GetVariableDataFromJulea(Variable<T> &variable, T *data,
                              const std::string nameSpace,
                              long unsigned int dataSize, size_t step,
                              size_t block);

/* Attributes */
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

#define variable_template_instantiation(T)                                     \
    extern template void GetVariableDataFromJulea(                             \
        Variable<T> &variable, T *data, const std::string nameSpace,           \
        long unsigned int dataSize, size_t step, size_t block);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

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
