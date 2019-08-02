/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAINTERACTIONREAD_H_
#define ADIOS2_ENGINE_JULEAINTERACTIONREAD_H_

#include "JuleaKVReader.h"
#include "JuleaMetadata.h"

// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

/* Variable Functions */
void GetNamesBSONFromJulea(const std::string nameSpace, bson_t *bsonNames,
                           unsigned int *varCount);

void GetVariableBSONFromJulea(const std::string nameSpace,
                              const std::string varName, bson_t *bsonMetadata);

// void ExtractVariableFromBSON(const std::string nameSpace, const std::string
// varName, bson_t *bsonMetadata, int type, Dims shape, Dims start, Dims
// count,bool constantDims);

template <class T>
void GetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata,
                                  const std::string nameSpace);

template <class T>
void GetVariableDataFromJulea(Variable<T> &variable, const T *data,
                              const std::string nameSpace);

/* Attribute Functions */
template <class T>
void GetAllAttributeNamesFromJulea(Variable<T> &variable, bson_t *bsonMetadata,
                                   const std::string nameSpace);

template <class T>
void GetAttributeMetadataFromJulea(Attribute<T> &attribute,
                                   bson_t *bsonMetadata,
                                   const std::string nameSpace);

template <class T>
void GetAttributeDataFromJulea(Attribute<T> &attribute, const T *data,
                               const std::string nameSpace);

// extern template void GetAllVariableNamesFromJulea(const std::string nameSpace, unsigned int *varCount, bson_t *bsonNames);         \

#define declare_template_instantiation(T)                                      \
    extern template void GetVariableDataFromJulea(                             \
        Variable<T> &variable, const T *data, const std::string nameSpace);    \
    extern template void GetVariableMetadataFromJulea(                         \
        Variable<T> &variable, bson_t *bsonMetadata,                           \
        const std::string nameSpace);                                          \
    extern template void GetAllAttributeNamesFromJulea(                        \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    extern template void GetAttributeDataFromJulea(                            \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    extern template void GetAttributeMetadataFromJulea(                        \
        Attribute<T> &attribute, bson_t *bsonMetadata,                         \
        const std::string nameSpace);                                          \
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation
} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAINTERACTIONREAD_H_ */
