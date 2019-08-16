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
// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

/* Variables and Attributes */
void GetNamesBSONFromJulea(const std::string nameSpace, bson_t **bsonNames,
                           unsigned int *varCount, const std::string kvName);

/* Variables */
void GetVariableBSONFromJulea(const std::string nameSpace,
                              const std::string varName, bson_t **bsonMetadata);

template <class T>
void GetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata,
                                  const std::string nameSpace,
                                  long unsigned int *dataSize);

template <class T>
void GetVariableDataFromJulea(Variable<T> &variable, T *data,
                              const std::string nameSpace,
                              long unsigned int dataSize);

#define variable_template_instantiation(T)                                     \
    extern template void GetVariableMetadataFromJulea(                         \
        Variable<T> &variable, bson_t *bsonMetadata,                           \
        const std::string nameSpace, long unsigned int *dataSize);             \
    extern template void GetVariableDataFromJulea(                             \
        Variable<T> &variable, T *data, const std::string nameSpace,           \
        long unsigned int dataSize);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

/* Attributes */
void GetAttributeMetadataFromJulea(const std::string attrName,
                                   bson_t *bsonMetadata,
                                   const std::string nameSpace,
                                   long unsigned int *dataSize,
                                   size_t *numberElements, bool *IsSingleValue,
                                   int *type);

void GetAttributeBSONFromJulea(const std::string nameSpace,
                               const std::string varName,
                               bson_t **bsonMetadata);

template <class T>
void GetAttributeDataFromJulea(const std::string attrName, T *data,
                               const std::string nameSpace,
                               long unsigned int dataSize);

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
