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

template <class T>
void GetVariableDataFromJulea(Variable<T> &variable, const T *data,
                            const char *name_space);

template <class T>
void GetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata,
                                const char *name_space);

template <class T>
void GetAttributeDataFromJulea(Attribute<T> &attribute, const T *data,
                             const char *nameSpace);
template <class T>
void GetAttributeMetadataFromJulea(Attribute<T> &attribute, bson_t *bsonMetadata,
                                 const char *nameSpace);

#define declare_template_instantiation(T)                                      \
    extern template void GetVariableDataFromJulea(                               \
        Variable<T> &variable, const T *data, const char *name_space);         \
    extern template void GetVariableMetadataFromJulea(                           \
        Variable<T> &variable, bson_t *bsonMetadata, const char *name_space);  \
    extern template void GetAttributeDataFromJulea(                              \
        Attribute<T> &attribute, const T *data, const char *nameSpace);        \
    extern template void GetAttributeMetadataFromJulea(                          \
        Attribute<T> &attribute, bson_t *bsonMetadata, const char *nameSpace); \
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation
} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAINTERACTIONREAD_H_ */
