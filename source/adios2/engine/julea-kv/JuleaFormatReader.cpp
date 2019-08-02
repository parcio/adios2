/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATREADER_
#define ADIOS2_ENGINE_JULEAFORMATREADER_

#include "JuleaFormatReader.h"
#include "JuleaKVReader.h"

#include <bson.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

namespace adios2
{
namespace core
{
namespace engine
{

// template <class T>
// void SetMinMax(Variable<T> &variable, const T *data)
// {
//     T min;
//     T max;

//     auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
//     adios2::helper::GetMinMax(data, number_elements, min, max);
//     variable.m_Min = min;
//     variable.m_Max = max;
// }

// template <class T>
// void ParseAttributeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata)
// {
//     // name is key in kv
//     unsigned int dataSize = -1;

//     bson_append_int64(bsonMetadata, "number_elements", -1,
//                       attribute.m_Elements);
//     bson_append_bool(bsonMetadata, "is_single_value", -1,
//                      attribute.m_IsSingleValue);
//     if (attribute.m_IsSingleValue)
//     {
//         // TODO: check if this is correct
//         dataSize = sizeof(attribute.m_DataSingleValue);
//     }
//     else
//     {
//         dataSize = attribute.m_DataArray.size();
//     }

//     bson_append_int64(bsonMetadata, "data_size", -1, dataSize);
// }



// #define variable_template_instantiation(T)                                     \
//     template void SetMinMax(Variable<T> &variable, const T *data);             \
//     template void ParseVariableToBSON(core::Variable<T> &,                     \
//                                       bson_t *bsonMetadata);

// ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
// #undef variable_template_instantiation

// #define attribute_template_instantiation(T)                                    \
//     template void ParseAttributeToBSON(Attribute<T> &attribute,                \
//                                        bson_t *bsonMetadata);                  \
//     template void ParseAttrTypeToBSON(Attribute<T> &attribute,                 \
//                                       bson_t *bsonMetadata);

// ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
// #undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
#endif /* ADIOS2_ENGINE_JULEAFORMATREADER_ */
