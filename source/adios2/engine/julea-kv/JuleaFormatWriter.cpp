/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATWRITER_
#define ADIOS2_ENGINE_JULEAFORMATWRITER_

#include "JuleaFormatWriter.h"
#include "JuleaKVWriter.h"

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

template <class T>
void SetMinMax(Variable<T> &variable, const T *data)
{
    T min;
    T max;

    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    adios2::helper::GetMinMax(data, number_elements, min, max);
    variable.m_Min = min;
    variable.m_Max = max;
}

template <class T>
void ParseAttributeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata)
{
    // name is key in kv
    unsigned int dataSize = -1;

    bson_append_int64(bsonMetadata, "number_elements", -1,
                      attribute.m_Elements);
    bson_append_bool(bsonMetadata, "is_single_value", -1,
                     attribute.m_IsSingleValue);
    if (attribute.m_IsSingleValue)
    {
        // TODO: check if this is correct
        dataSize = sizeof(attribute.m_DataSingleValue);
    }
    else
    {
        dataSize = attribute.m_DataArray.size();
    }

    bson_append_int64(bsonMetadata, "data_size", -1, dataSize);
}

template <class T>
void ParseAttrTypeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata)
{
    int type = -1;

    if (helper::GetType<T>() == "string")
    {
        type = STRING;
    }
    else if (helper::GetType<T>() == "int8_t")
    {
        type = INT8;
    }
    else if (helper::GetType<T>() == "uint8_t")
    {
        type = UINT8;
    }
    else if (helper::GetType<T>() == "int16_t")
    {
        type = INT16;
    }
    else if (helper::GetType<T>() == "uint16_t")
    {
        type = UINT16;
    }
    else if (helper::GetType<T>() == "int32_t")
    {
        type = INT32;
    }
    else if (helper::GetType<T>() == "uint32_t")
    {
        type = UINT32;
    }
    else if (helper::GetType<T>() == "int64_t")
    {
        type = INT64;
    }
    else if (helper::GetType<T>() == "uint64_t")
    {
        type = UINT64;
    }
    else if (helper::GetType<T>() == "float")
    {
        type = FLOAT;
    }
    else if (helper::GetType<T>() == "double")
    {
        type = DOUBLE;
    }
    else if (helper::GetType<T>() == "long double")
    {
        type = LONG_DOUBLE;
    }

    bson_append_int32(bsonMetadata, "attr_type", -1, type);
    std::cout << "ParseAttrTypeToBSON type :" << attribute.m_Type << std::endl;
}

template <class T>
void ParseVariableToBSON(Variable<T> &variable, bson_t *bsonMetadata)
{
    std::cout << "Test" << std::endl;
    T min;
    T max;

    uint data_size = 0;
    size_t number_elements = 0;
    char *key;

    bson_append_int64(bsonMetadata, "shape_size", -1, variable.m_Shape.size());
    for (guint i = 0; i < variable.m_Shape.size(); i++)
    {
        key = g_strdup_printf("shape_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Shape[i]);
    }
    bson_append_int64(bsonMetadata, "start_size", -1, variable.m_Start.size());
    for (guint i = 0; i < variable.m_Start.size(); i++)
    {
        key = g_strdup_printf("start_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Start[i]);
    }

    bson_append_int64(bsonMetadata, "count_size", -1, variable.m_Count.size());
    for (guint i = 0; i < variable.m_Count.size(); i++)
    {
        key = g_strdup_printf("count_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Count[i]);
    }

    bson_append_int64(bsonMetadata, "memory_start_size", -1,
                      variable.m_MemoryStart.size());
    for (guint i = 0; i < variable.m_MemoryStart.size(); i++)
    {
        key = g_strdup_printf("memory_start_%d", i);
        bson_append_int64(bsonMetadata, key, -1, variable.m_MemoryStart[i]);
    }

    bson_append_int64(bsonMetadata, "memory_count_size", -1,
                      variable.m_MemoryCount.size());
    for (guint i = 0; i < variable.m_MemoryCount.size(); i++)
    {
        key = g_strdup_printf("memory_count_%d", i);
        bson_append_int64(bsonMetadata, key, -1, variable.m_MemoryCount[i]);
    }

    bson_append_int64(bsonMetadata, "steps_start", -1, variable.m_StepsStart);
    bson_append_int64(bsonMetadata, "steps_count", -1, variable.m_StepsCount);

    bson_append_int64(bsonMetadata, "block_id", -1, variable.m_BlockID);
    bson_append_int64(bsonMetadata, "index_start", -1, variable.m_IndexStart);
    bson_append_int64(bsonMetadata, "element_size", -1, variable.m_ElementSize);
    bson_append_int64(bsonMetadata, "available_steps_start", -1,
                      variable.m_AvailableStepsStart);
    bson_append_int64(bsonMetadata, "available_steps_count", -1,
                      variable.m_AvailableStepsCount);

    /* compute data_size; dimension entries !> 0 are ignored ?!*/
    number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    data_size = variable.m_ElementSize * number_elements;
    bson_append_int64(bsonMetadata, "data_size", -1, data_size);

    //  metadata->deferred_counter = variable.m_DeferredCounter; //FIXME:
    //  needed? metadata->is_value = blockInfo.IsValue;
    bson_append_int64(bsonMetadata, "is_single_value", -1,
                      variable.m_SingleValue);
    bson_append_int64(bsonMetadata, "is_constant_dims", -1,
                      variable.IsConstantDims());
    bson_append_int64(bsonMetadata, "is_read_as_joined", -1,
                      variable.m_ReadAsJoined);
    bson_append_int64(bsonMetadata, "is_read_as_local_value", -1,
                      variable.m_ReadAsLocalValue);
    bson_append_int64(bsonMetadata, "is_random_access", -1,
                      variable.m_RandomAccess);
    bson_append_int64(bsonMetadata, "is_first_streaming_step", -1,
                      variable.m_FirstStreamingStep);
}

template <>
void ParseVarTypeToBSON<std::string>(Variable<std::string> &variable,
                                     const std::string *data,
                                     bson_t *bsonMetadata)
{
    // FIXME: set min, max for string?
    bson_append_int32(bsonMetadata, "var_type", -1, STRING);
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);

    std::cout << "ParseVarTypeToBSON String: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<int8_t>(Variable<int8_t> &variable, const int8_t *data,
                                bson_t *bsonMetadata)
{
    bson_append_int32(bsonMetadata, "var_type", -1, INT8);
    bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);

    std::cout << "ParseVarTypeToBSON int8_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<uint8_t>(Variable<uint8_t> &variable,
                                 const uint8_t *data, bson_t *bsonMetadata)
{
    bson_append_int32(bsonMetadata, "var_type", -1, UINT8);
    bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);

    std::cout << "ParseVarTypeToBSON uint8_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<int16_t>(Variable<int16_t> &variable,
                                 const int16_t *data, bson_t *bsonMetadata)
{
    bson_append_int32(bsonMetadata, "var_type", -1, INT16);
    bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON int16_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<uint16_t>(Variable<uint16_t> &variable,
                                  const uint16_t *data, bson_t *bsonMetadata)
{
    bson_append_int32(bsonMetadata, "var_type", -1, UINT16);
    bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON uint16_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<int32_t>(Variable<int32_t> &variable,
                                 const int32_t *data, bson_t *bsonMetadata)
{
    bson_append_int32(bsonMetadata, "var_type", -1, INT32);
    bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON int32_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<uint32_t>(Variable<uint32_t> &variable,
                                  const uint32_t *data, bson_t *bsonMetadata)
{
    bson_append_int32(bsonMetadata, "var_type", -1,
                      UINT32); // FIXME: does int32 suffice?
    bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON uint32_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<int64_t>(Variable<int64_t> &variable,
                                 const int64_t *data, bson_t *bsonMetadata)
{
    bson_append_int64(bsonMetadata, "var_type", -1, INT64);
    bson_append_int64(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int64(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int64(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON int64_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<uint64_t>(Variable<uint64_t> &variable,
                                  const uint64_t *data, bson_t *bsonMetadata)
{
    bson_append_int64(bsonMetadata, "var_type", -1, UINT64);
    bson_append_int64(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_int64(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_int64(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON uint64_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<float>(Variable<float> &variable, const float *data,
                               bson_t *bsonMetadata)
{
    bson_append_double(bsonMetadata, "var_type", -1, FLOAT);
    bson_append_double(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_double(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_double(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON float: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<double>(Variable<double> &variable, const double *data,
                                bson_t *bsonMetadata)
{
    bson_append_double(bsonMetadata, "var_type", -1, DOUBLE);
    bson_append_double(bsonMetadata, "min_value", -1, variable.Min());
    bson_append_double(bsonMetadata, "max_value", -1, variable.Max());
    bson_append_double(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeToBSON double: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<long double>(Variable<long double> &variable,
                                     const long double *data,
                                     bson_t *bsonMetadata)
{
    // FIXME: implement!
    // how to store long double in bson file?
    std::cout << "ParseVarTypeToBSON long double: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<std::complex<float>>(
    Variable<std::complex<float>> &variable, const std::complex<float> *data,
    bson_t *bsonMetadata)
{
    // FIXME: implement!
    // use two doubles? one for imaginary, one for real part?
    std::cout << "ParseVarTypeToBSON std::complex<float>: min = "
              << variable.Min() << std::endl;
}

template <>
void ParseVarTypeToBSON<std::complex<double>>(
    Variable<std::complex<double>> &variable, const std::complex<double> *data,
    bson_t *bsonMetadata)
{
    // FIXME: implement!
    // use two doubles? one for imaginary, one for real part?
    std::cout << "ParseVarTypeToBSON std::complex<double>: min = "
              << variable.Min() << std::endl;
}

#define variable_template_instantiation(T)                                     \
    template void SetMinMax(Variable<T> &variable, const T *data);             \
    template void ParseVariableToBSON(core::Variable<T> &,                     \
                                      bson_t *bsonMetadata);

ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

#define attribute_template_instantiation(T)                                    \
    template void ParseAttributeToBSON(Attribute<T> &attribute,                \
                                       bson_t *bsonMetadata);                  \
    template void ParseAttrTypeToBSON(Attribute<T> &attribute,                 \
                                      bson_t *bsonMetadata);

ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
#endif /* ADIOS_ENGINE_JULEAFORMATWRITER_ */
