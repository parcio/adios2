/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATWRITER_LEGACY_
#define ADIOS2_ENGINE_JULEAFORMATWRITER_LEGACY_

#include "JuleaFormatWriter-legacy.h"
#include "JuleaWriter.h"

// #include <adios2_c.h>
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
void ParseVariableToMetadataStruct(Variable<T> &variable, const T *data,
                                   Metadata *metadata)
{
    T min;
    T max;

    uint data_size = 0;
    size_t number_elements = 0;
    metadata->name = strdup(variable.m_Name.c_str());

    metadata->shape_size = variable.m_Shape.size();
    // std::cout << "++ Julea Writer DEBUG PRINT variable.m_Shape.size():
    //" <<
    // variable.m_Shape.size() << std::endl;
    if (variable.m_Shape.size() > 0)
    {
        metadata->shape = new unsigned long(metadata->shape_size);
        *metadata->shape = variable.m_Shape[0];
    }
    metadata->start_size = variable.m_Start.size();
    // std::cout << "++ Julea Writer DEBUG PRINT variable.m_Start.size():
    //" <<
    // variable.m_Start.size() << std::endl;
    if (variable.m_Start.size() > 0)
    {
        metadata->start = new unsigned long(metadata->start_size);
        *metadata->start = variable.m_Start[0];
    }
    metadata->count_size = variable.m_Count.size();
    // std::cout << "++ Julea Writer DEBUG PRINT variable.m_Count.size():
    // " <<
    // blockInfo.Count.size() << std::endl;
    if (variable.m_Count.size() > 0)
    {
        metadata->count = new unsigned long(metadata->count_size);
        *metadata->count = variable.m_Count[0];
    }
    metadata->memory_start_size = variable.m_MemoryStart.size();
    if (variable.m_MemoryStart.size() > 0)
    {
        metadata->memory_start = new unsigned long(metadata->memory_start_size);
        *metadata->memory_start = variable.m_MemoryStart[0];
    }
    metadata->memory_count_size = variable.m_MemoryCount.size();
    if (variable.m_MemoryCount.size() > 0)
    {
        metadata->memory_count = new unsigned long(metadata->memory_count_size);
        *metadata->memory_count = variable.m_MemoryCount[0];
    }

    metadata->steps_start = variable.m_StepsStart;
    metadata->steps_count = variable.m_StepsCount;
    metadata->block_id = variable.m_BlockID;
    metadata->index_start = variable.m_IndexStart;
    metadata->element_size = variable.m_ElementSize;
    metadata->available_steps_start = variable.m_AvailableStepsStart;
    metadata->available_steps_count = variable.m_AvailableStepsCount;

    /* compute data_size; dimension entries !> 0 are ignored ?!*/
    number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    metadata->data_size = variable.m_ElementSize * number_elements;

    adios2::helper::GetMinMax(data, number_elements, min, max);
    variable.m_Min = min;
    variable.m_Max = max;

    ParseVarTypeToBSON(variable, data, metadata);
    std::cout << "number_elements: " << number_elements << std::endl;
    std::cout << "m_ElementSize: " << variable.m_ElementSize << std::endl;
    std::cout << "variable: " << variable.m_Name << " min: " << min
              << std::endl;
    std::cout << "variable: " << variable.m_Name << " max: " << max
              << std::endl;
    std::cout << "data_size: " << metadata->data_size << std::endl;

    // metadata->deferred_counter = variable.m_DeferredCounter;
    // metadata->is_value = blockInfo.IsValue;
    metadata->is_single_value = variable.m_SingleValue;
    metadata->is_constant_dims = variable.IsConstantDims();
    metadata->is_read_as_joined = variable.m_ReadAsJoined;
    metadata->is_read_as_local_value = variable.m_ReadAsLocalValue;
    metadata->is_random_access = variable.m_RandomAccess;
    metadata->is_first_streaming_step = variable.m_FirstStreamingStep;
}

template <class T>
void ParseVariableToMetadataStruct(Variable<T> &variable,
                                   const typename Variable<T>::Info &blockInfo,
                                   Metadata *metadata)
{
    metadata->name = strdup(variable.m_Name.c_str());

    metadata->shape_size = blockInfo.Shape.size();
    // std::cout << "++ Julea Writer DEBUG PRINT blockInfo.Shape.size(): " <<
    // blockInfo.Shape.size() << std::endl;
    if (blockInfo.Shape.size() > 0)
    {
        metadata->shape = new unsigned long(metadata->shape_size);
        *metadata->shape = blockInfo.Shape[0];
    }
    metadata->start_size = blockInfo.Start.size();
    // std::cout << "++ Julea Writer DEBUG PRINT blockInfo.Start.size(): " <<
    // blockInfo.Start.size() << std::endl;
    if (blockInfo.Start.size() > 0)
    {
        metadata->start = new unsigned long(metadata->start_size);
        *metadata->start = blockInfo.Start[0];
    }
    metadata->count_size = blockInfo.Count.size();
    // std::cout << "++ Julea Writer DEBUG PRINT blockInfo.Count.size(): " <<
    // blockInfo.Count.size() << std::endl;
    if (blockInfo.Count.size() > 0)
    {
        metadata->count = new unsigned long(metadata->count_size);
        *metadata->count = blockInfo.Count[0];
    }
    metadata->memory_start_size = blockInfo.MemoryStart.size();
    if (blockInfo.MemoryStart.size() > 0)
    {
        metadata->memory_start = new unsigned long(metadata->memory_start_size);
        *metadata->memory_start = blockInfo.MemoryStart[0];
    }
    metadata->memory_count_size = blockInfo.MemoryCount.size();
    if (blockInfo.MemoryCount.size() > 0)
    {
        metadata->memory_count = new unsigned long(metadata->memory_count_size);
        *metadata->memory_count = blockInfo.MemoryCount[0];
    }

    metadata->steps_start = blockInfo.StepsStart;
    metadata->steps_count = blockInfo.StepsCount;
    metadata->block_id = blockInfo.BlockID;

    ParseVariableType(variable, blockInfo, metadata);

    size_t valuesSize = adios2::helper::GetTotalSize(variable.m_Count);
    std::cout << "valuesSize" << valuesSize << std::endl;
    T min, max;
    adios2::helper::GetMinMax(blockInfo.Data, valuesSize, min, max);

    blockInfo.Min = min;
    blockInfo.Max = max;

    std::cout << " -------------------------------------------- " << std::endl;
    std::cout << " BlockInfo Min and Max " << std::endl;

    std::cout << "variable: " << variable.m_Name << " min: " << blockInfo.Min
              << std::endl;
    std::cout << "variable: " << variable.m_Name << " max: " << blockInfo.Max
              << std::endl;

    /* compute data_size; dimension entries !> 0 are ignored */
    size_t numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    metadata->data_size = variable.m_ElementSize * numberElements;

    // metadata->is_value = blockInfo.IsValue;//TODO: currently not part of
    // metadata struct
    // TODO: operations vector
    // TODO: bufferP
    // TODO: buffer vector
    // TODO: selection type
}

template <class T>
void ParseVariableToBSON(const typename Variable<T>::Info &blockInfo,
                         bson_t *bson_meta_data)
{
}

template <class T>
void ParseVarTypeToBSON(Variable<T> &variable, const T *data,
                        Metadata *metadata)
{

    // if (helper::GetType<T>() == "string")
    // {
    //     metadata->var_type = STRING;
    // }
    // else if (helper::GetType<T>() == "int8_t")
    // {
    //     metadata->var_type = INT8;
    //     metadata->sizeof_var_type = sizeof(int8_t);
    //     // metadata->min_value.integer_8 = variable.Min();
    // }
    // else if (helper::GetType<T>() == "uint8_t")
    // {
    //     metadata->var_type = UINT8;
    //     metadata->sizeof_var_type = sizeof(uint8_t);
    //     // metadata->min_value.u_integer_8 = variable.Min();
    // }
    // ...
}

template <>
void ParseVarTypeToBSON<std::string>(Variable<std::string> &variable,
                                     const std::string *data,
                                     Metadata *metadata)
{
    metadata->var_type = STRING;
    metadata->sizeof_var_type = sizeof(std::string);
    // metadata->min_value->string = variable.Min().; //what would be the use of
    // a string minimum?
}

template <>
void ParseVarTypeToBSON<int8_t>(Variable<int8_t> &variable, const int8_t *data,
                                Metadata *metadata)
{
    metadata->var_type = INT8;
    metadata->sizeof_var_type = sizeof(int8_t);
    metadata->min_value.integer_8 = variable.Min();
    metadata->max_value.integer_8 = variable.Max();
    metadata->curr_value.integer_8 = variable.m_Value;
    std::cout << "ParseVarTypeToBSON int8_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeToBSON<uint8_t>(Variable<uint8_t> &variable,
                                 const uint8_t *data, Metadata *metadata)
{
    metadata->var_type = UINT8;
    metadata->sizeof_var_type = sizeof(uint8_t);
    metadata->min_value.u_integer_8 = variable.Min();
    metadata->max_value.u_integer_8 = variable.Max();
    metadata->curr_value.u_integer_8 = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<int16_t>(Variable<int16_t> &variable,
                                 const int16_t *data, Metadata *metadata)
{
    metadata->var_type = INT16;
    metadata->sizeof_var_type = sizeof(int16_t);
    metadata->min_value.integer_16 = variable.Min();
    metadata->max_value.integer_16 = variable.Max();
    metadata->curr_value.integer_16 = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<uint16_t>(Variable<uint16_t> &variable,
                                  const uint16_t *data, Metadata *metadata)
{
    metadata->var_type = UINT16;
    metadata->sizeof_var_type = sizeof(uint16_t);
    metadata->min_value.u_integer_16 = variable.Min();
    metadata->max_value.u_integer_16 = variable.Max();
    metadata->curr_value.u_integer_16 = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<int32_t>(Variable<int32_t> &variable,
                                 const int32_t *data, Metadata *metadata)
{
    metadata->var_type = INT32;
    metadata->sizeof_var_type = sizeof(int32_t);
    metadata->min_value.integer_32 = variable.Min();
    metadata->max_value.integer_32 = variable.Max();
    metadata->curr_value.integer_32 = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<uint32_t>(Variable<uint32_t> &variable,
                                  const uint32_t *data, Metadata *metadata)
{
    metadata->var_type = UINT32;
    metadata->sizeof_var_type = sizeof(uint32_t);
    metadata->min_value.u_integer_32 = variable.Min();
    metadata->max_value.u_integer_32 = variable.Max();
    metadata->curr_value.u_integer_32 = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<int64_t>(Variable<int64_t> &variable,
                                 const int64_t *data, Metadata *metadata)
{
    metadata->var_type = INT64;
    metadata->sizeof_var_type = sizeof(int64_t);
    metadata->min_value.integer_64 = variable.Min();
    metadata->max_value.integer_64 = variable.Max();
    metadata->curr_value.integer_64 = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<uint64_t>(Variable<uint64_t> &variable,
                                  const uint64_t *data, Metadata *metadata)
{
    metadata->var_type = UINT64;
    metadata->sizeof_var_type = sizeof(uint64_t);
    metadata->min_value.u_integer_64 = variable.Min();
    metadata->max_value.u_integer_64 = variable.Max();
    metadata->curr_value.u_integer_64 = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<float>(Variable<float> &variable, const float *data,
                               Metadata *metadata)
{
    metadata->var_type = FLOAT;
    metadata->sizeof_var_type = sizeof(float);
    metadata->min_value.real_float = variable.Min();
    metadata->max_value.real_float = variable.Max();
    metadata->curr_value.real_float = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<double>(Variable<double> &variable, const double *data,
                                Metadata *metadata)
{
    metadata->var_type = DOUBLE;
    metadata->sizeof_var_type = sizeof(double);
    metadata->min_value.real_double = variable.Min();
    metadata->max_value.real_double = variable.Max();
    metadata->curr_value.real_double = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<long double>(Variable<long double> &variable,
                                     const long double *data,
                                     Metadata *metadata)
{
    metadata->var_type = LONG_DOUBLE;
    metadata->sizeof_var_type = sizeof(long double);
    metadata->min_value.real_long_double = variable.Min();
    metadata->max_value.real_long_double = variable.Max();
    metadata->curr_value.real_long_double = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<std::complex<float>>(
    Variable<std::complex<float>> &variable, const std::complex<float> *data,
    Metadata *metadata)
{
    metadata->var_type = COMPLEX_FLOAT;
    metadata->sizeof_var_type = sizeof(std::complex<float>);
    metadata->min_value.complex_float = variable.Min();
    metadata->max_value.complex_float = variable.Max();
    metadata->curr_value.complex_float = variable.m_Value;
}

template <>
void ParseVarTypeToBSON<std::complex<double>>(
    Variable<std::complex<double>> &variable, const std::complex<double> *data,
    Metadata *metadata)
{
    metadata->var_type = COMPLEX_DOUBLE;
    metadata->sizeof_var_type = sizeof(std::complex<double>);
    metadata->min_value.complex_double = variable.Min();
    metadata->max_value.complex_double = variable.Max();
    metadata->curr_value.complex_double = variable.m_Value;
}

#define declare_template_instantiation(T)                                      \
    template void ParseVariableToMetadataStruct(                               \
        core::Variable<T> &, const T *data, Metadata *metadata);

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
#endif /* ADIOS2_ENGINE_JULEAFORMATWRITER_LEGACY_ */
