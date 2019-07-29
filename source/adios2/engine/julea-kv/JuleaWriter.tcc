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

#include "JuleaFormatWriter.h"
#include "JuleaWriter.h"

#include <adios2_c.h>
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

// TODO: necessary function?
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
    metadata->index_start = variable.m_IndexStart;
    metadata->element_size = variable.m_ElementSize;
    metadata->available_steps_start = variable.m_AvailableStepsStart;
    metadata->available_steps_count = variable.m_AvailableStepsCount;

    ParseVariableType(variable, blockInfo, metadata);

    // TODO: implement min, max, curr

    size_t valuesSize = adios2::helper::GetTotalSize(variable.m_Count);
    std::cout << "valuesSize" << valuesSize << std::endl;
    T min, max;
    adios2::helper::GetMinMax(blockInfo.Data, valuesSize, min, max);
    std::cout << "variable: " << variable.m_Name << " min: " << min
              << std::endl;
    std::cout << "variable: " << variable.m_Name << " max: " << max
              << std::endl;

    variable.m_Min = min;
    // blockInfo.Min = min;
    variable.m_Max = max;
    // blockInfo.Max = max;

    // std::cout << "variable: " << variable.m_Name << " min: " <<
    blockInfo.Min << std::endl;
    std::cout << "variable: " << variable.m_Name << " min: " << variable.Min()
              << std::endl;
    std::cout << "variable: " << variable.m_Name << " min: " << variable.m_Min
              << std::endl;

    // std::cout << "variable: " << variable.m_Name << " max: " <<
    blockInfo.Max << std::endl;
    std::cout << "variable: " << variable.m_Name << " max: " << variable.Max()
              << std::endl;
    std::cout << "variable: " << variable.m_Name << " max: " << variable.m_Max
              << std::endl;

    std::cout << " -------------------------------------------- " << std::endl;
    std::cout << " BlockInfo Min and Max " << std::endl;

    std::cout << "variable: " << variable.m_Name << " min: " << blockInfo.Min
              << std::endl;
    std::cout << "variable: " << variable.m_Name << " max: " << blockInfo.Max
              << std::endl;
    variable.MinMax(0);
    std::cout << "variable: " << variable.m_Name << " min: " << blockInfo.Min
              << std::endl;
    std::cout << "variable: " << variable.m_Name << " max: " << blockInfo.Max
              << std::endl;

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
    // std::endl; std::cout << "size of type: " << metadata->sizeof_var_type
    // <<
    // std::endl; std::cout << "data_size: " << metadata->data_size <<
    // std::endl;

    // metadata->deferred_counter = variable.m_DeferredCounter;
    // metadata->is_value = blockInfo.IsValue;
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
    std::cout << " =========================================== " << std::endl;
    // g_free((void*)name_space);
    g_free(metadata->name);
    g_slice_free(Metadata, metadata);
    j_batch_unref(batch);
    // FIXME free only if size > 0
    // g_slice_free(unsigned long, metadata->shape);
    // g_slice_free(unsigned long, metadata->start);
    // g_slice_free(unsigned long, metadata->count);
}

template <class T>
void JuleaWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    T min;
    T max;

    uint data_size = 0;
    size_t number_elements = 0;
    const char *name_space = m_Name.c_str();

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
    Metadata *metadata = g_slice_new(Metadata);
    // Metadata *metadata = new Metadata(*metadata); //FIXME

    ParseVariableMetadata(variable, data, metadata);
    ParseVariable(variable, data);

    adios2::helper::GetMinMax(data, number_elements, min, max);
    variable.m_Min = min;
    variable.m_Max = max;

    ParseVariableType(variable, data, metadata);
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

    // // for(int i = 0; i < 10; i++)
    // // {
    // //     std::cout << "blockInfo.Data: " << blockInfo.Data[i] << std::endl;
    // // }

    // // std::cout << "Data type: " << variable.m_Type << std::endl;
    // // FIXME: resizeresult

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    PutVariableToJulea(m_JuleaInfo->name_space, metadata, data, batch);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
    std::cout << " ============================================ " << std::endl;
    // // g_free((void*)name_space);

    // TODO: delete instead of g_free
    g_free(metadata->name);
    g_slice_free(Metadata, metadata);
    j_batch_unref(batch);

    // g_slice_free(unsigned long, metadata->shape); //FIXME free only if size >
    // 0 g_slice_free(unsigned long, metadata->start); g_slice_free(unsigned
    // long, metadata->count);
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
