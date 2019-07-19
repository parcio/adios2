/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAWRITER_TCC_
#define ADIOS2_ENGINE_JULEAWRITER_TCC_

#include "JuleaWriter.h"

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

/**
 * Parsing the variable types to enum defined in JULEA's Adios Client.
 * Great that types are handled as string here...
 */
template <class T>
void parse_variable_type(Variable<T> &variable,
                         const typename Variable<T>::Info &blockInfo,
                         Metadata *metadata)
{

    if (helper::GetType<T>() == "string")
    {
        metadata->var_type = STRING;
        // metadata->min_value.string = blockInfo.Min;
    }
    else if (helper::GetType<T>() == "int8_t")
    {
        metadata->var_type = INT8;
        metadata->sizeof_var_type = sizeof(int8_t);
    }
    else if (helper::GetType<T>() == "uint8_t")
    {
        metadata->var_type = UINT8;
        metadata->sizeof_var_type = sizeof(uint8_t);
    }
    else if (helper::GetType<T>() == "int16_t")
    {
        metadata->var_type = INT16;
        metadata->sizeof_var_type = sizeof(int16_t);
        // metadata->min_value = (short) variable.m_Min;
        // metadata->min_value.shorter = static_cast<short>(variable.m_Min);
        // metadata->min_value.shorter = reinterpret_cast<T>(variable.m_Min);
    }
    else if (helper::GetType<T>() == "uint16_t")
    {
        metadata->var_type = UINT16;
        metadata->sizeof_var_type = sizeof(uint16_t);
    }
    else if (helper::GetType<T>() == "int32_t")
    {
        metadata->var_type = INT32;
        metadata->sizeof_var_type = sizeof(int32_t);
    }
    else if (helper::GetType<T>() == "uint32_t")
    {
        metadata->var_type = UINT32;
        metadata->sizeof_var_type = sizeof(uint32_t);
    }
    else if (helper::GetType<T>() == "int64_t")
    {
        metadata->var_type = INT64;
        metadata->sizeof_var_type = sizeof(int64_t);
    }
    else if (helper::GetType<T>() == "uint64_t")
    {
        metadata->var_type = UINT64;
        metadata->sizeof_var_type = sizeof(uint64_t);
    }
    else if (helper::GetType<T>() == "float")
    {
        metadata->var_type = FLOAT;
        metadata->sizeof_var_type = sizeof(float);
    }
    else if (helper::GetType<T>() == "double")
    {
        metadata->var_type = DOUBLE;
        metadata->sizeof_var_type = sizeof(double);
    }
    else if (helper::GetType<T>() == "long double")
    {
        metadata->var_type = LONG_DOUBLE;
        metadata->sizeof_var_type = sizeof(long double);
    }

    else if (helper::GetType<T>() == "float complex")
    {
        metadata->var_type = FLOAT_COMPLEX;
        // metadata->sizeof_var_type = sizeof(float complex); //TODO
    }
    else if (helper::GetType<T>() == "double complex")
    {
        metadata->var_type = DOUBLE_COMPLEX;
        // metadata->sizeof_var_type = sizeof(double complex); //TODO
    }
}

template <class T>
void JuleaWriter::PutSyncCommon(Variable<T> &variable,
                                const typename Variable<T>::Info &blockInfo)
{
    // g_autoptr(JBatch) batch = NULL;
    // g_autoptr(JSemantics) semantics = NULL;
    gboolean use_batch = TRUE;

    const gchar *name_space = m_Name.c_str();
    uint data_size = 0;

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
    Metadata *metadata = g_slice_new(Metadata);
    metadata->name = strdup(variable.m_Name.c_str());

    metadata->shape_size = blockInfo.Shape.size();
    // std::cout << "++ Julea Writer DEBUG PRINT blockInfo.Shape.size(): " <<
    // blockInfo.Shape.size() << std::endl;
    if (blockInfo.Shape.size() > 0)
    {
        // metadata->shape = g_slice_new(sizeof(metadata->shape) *
        // metadata->shape_size);
        metadata->shape = (unsigned long *)g_slice_alloc(
            sizeof(metadata->shape) * metadata->shape_size);
        *metadata->shape = blockInfo.Shape[0];
    }
    metadata->start_size = blockInfo.Start.size();
    // std::cout << "++ Julea Writer DEBUG PRINT blockInfo.Start.size(): " <<
    // blockInfo.Start.size() << std::endl;
    if (blockInfo.Start.size() > 0)
    {
        // metadata->start = g_slice_new(unsigned long);
        metadata->start = (unsigned long *)g_slice_alloc(
            sizeof(metadata->start) * metadata->start_size);
        *metadata->start = blockInfo.Start[0];
    }
    metadata->count_size = blockInfo.Count.size();
    // std::cout << "++ Julea Writer DEBUG PRINT blockInfo.Count.size(): " <<
    // blockInfo.Count.size() << std::endl;
    if (blockInfo.Count.size() > 0)
    {
        metadata->count = (unsigned long *)g_slice_alloc(
            sizeof(metadata->count) * metadata->count_size);
        *metadata->count = blockInfo.Count[0];
    }
    metadata->memory_start_size = blockInfo.MemoryStart.size();
    if (blockInfo.MemoryStart.size() > 0)
    {
        metadata->memory_start = (unsigned long *)g_slice_alloc(
            sizeof(metadata->memory_start) * metadata->memory_start_size);
        *metadata->memory_start = blockInfo.MemoryStart[0];
    }
    metadata->memory_count_size = blockInfo.MemoryCount.size();
    if (blockInfo.MemoryCount.size() > 0)
    {
        metadata->memory_count = (unsigned long *)g_slice_alloc(
            sizeof(metadata->memory_count) * metadata->memory_count_size);
        *metadata->memory_count = blockInfo.MemoryCount[0];
    }

    // metadata->test_header = 42;     //additional test member for transition
    // of adios client logic to engine

    metadata->steps_start = blockInfo.StepsStart;
    metadata->steps_count = blockInfo.StepsCount;
    metadata->block_id = blockInfo.BlockID;
    metadata->index_start = variable.m_IndexStart;
    metadata->element_size = variable.m_ElementSize;
    metadata->available_steps_start = variable.m_AvailableStepsStart;
    metadata->available_steps_count = variable.m_AvailableStepsCount;

    parse_variable_type(variable, blockInfo, metadata);

    // TODO: implement min, max, curr

    /* compute data_size; dimension entries !> 0 are ignored */
    int number_elements = 1; // FIXME: how to initialise?
    for (int i = 0; i < blockInfo.Count.size(); i++)
    {
        if (blockInfo.Count[i] > 0)
        {
            number_elements = number_elements * blockInfo.Count[i];
            std::cout << "number_elements: " << number_elements << std::endl;
        }
    }

    // metadata->data_size = sizeof(helper::GetType<T>()) * number_elements;
    metadata->data_size = metadata->sizeof_var_type * number_elements;
    // std::cout << "size of type: " << sizeof(helper::GetType<T>()) <<
    // std::endl; std::cout << "size of type: " << metadata->sizeof_var_type <<
    // std::endl; std::cout << "data_size: " << metadata->data_size <<
    // std::endl;

    // metadata->deferred_counter = variable.m_DeferredCounter;
    metadata->is_value = blockInfo.IsValue;
    metadata->is_single_value = variable.m_SingleValue;
    metadata->is_constant_dims = variable.IsConstantDims();
    metadata->is_read_as_joined = variable.m_ReadAsJoined;
    metadata->is_read_as_local_value = variable.m_ReadAsLocalValue;
    metadata->is_random_access = variable.m_RandomAccess;
    metadata->is_first_streaming_step = variable.m_FirstStreamingStep;

    // for(int i = 0; i < 10; i++)
    // {
    //     std::cout << "blockInfo.Data: " << blockInfo.Data[i] << std::endl;
    // }

    // std::cout << "Data type: " << variable.m_Type << std::endl;
    // FIXME: resizeresult

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    PutVariableToJulea(m_JuleaInfo->name_space, metadata, blockInfo.Data,
                       batch);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
    // g_free((void*)name_space);
    g_free(metadata->name);
    g_slice_free(Metadata, metadata);
}

template <class T>
void JuleaWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    // std::cout << "JULEA ENGINE: PutDeferredCommon" << std::endl;
    // std::cout << "You successfully reached the JULEA engine with the DEFERRED
    // mode "<< std::endl;
    // variable.SetBlockInfo(data, CurrentStep());

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutDeferred("
                  << variable.m_Name << ")\n";
    }
    m_DeferredVariables.insert(variable.m_Name);

    // FIXME: adapt name!
    // DESIGN: is it necessary/useful to do this?
    m_DeferredVariablesDataSize += static_cast<size_t>(
        1.05 * helper::PayloadSize(variable.m_Data, variable.m_Count) +
        4 * GetBPIndexSizeInData(variable.m_Name, variable.m_Count));

    m_NeedPerformPuts = true;
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_SKELETONWRITER_TCC_ */
