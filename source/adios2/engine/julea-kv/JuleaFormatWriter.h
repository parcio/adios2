/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATWRITER_H_
#define ADIOS2_ENGINE_JULEAFORMATWRITER_H_

#include "JuleaMetadata.h"
#include "JuleaKVWriter.h"
// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
void SetMinMax(Variable<T> &variable, const T *data);

template <class T>
void ParseVariableToBSON(Variable<T> &variable, bson_t *bsonMetadata);

/* also sets m_Min, m_Max, m_Value; therefore the data pointer needs to be passed */
template <class T>
void ParseVarTypeToBSON(Variable<T> &variable, const T *data,
                        bson_t *bsonMetadata);

template <class T>
void ParseAttributeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata);

template <class T>
void ParseAttrTypeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata);

#define variable_template_instantiation(T)                                      \
   	SetMinMax(Variable<T> &variable, const T *data);      \
    extern template void ParseVariableToBSON(Variable<T> &variable,            \
                                             bson_t *bsonMetadata);          \
    ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

#define attribute_template_instantiation(T)                                      \
    extern template void ParseAttributeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata);\
    extern template void ParseAttrTypeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata);\
    ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAFORMATWRITER_H_ */
