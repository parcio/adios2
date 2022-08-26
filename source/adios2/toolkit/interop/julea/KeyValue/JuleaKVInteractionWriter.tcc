/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaKVInteractionWriter_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaKVInteractionWriter_TCC_

#include "JuleaKVInteractionReader.h"
#include "JuleaKVInteractionWriter.h"
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"
// #include "JuleaMetadata.h"

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
#include <julea-db.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace interop
{

template <class T>
void JuleaKVInteractionWriter::AppendMinMaxValueToBSON(
    core::Variable<T> &variable, bson_t *bsonMetadata)
{
    auto type = helper::GetDataType<T>();
    switch (type)
    {
    case adios2::DataType::None:
        // TODO: Do something?
        break;
    case adios2::DataType::Int8:
        break;
    case adios2::DataType::UInt8:
        break;
    case adios2::DataType::Int16:
        break;
    case adios2::DataType::UInt16:
        break;
    case adios2::DataType::Int32:
        break;
    case adios2::DataType::UInt32:
        break;
    case adios2::DataType::Int64:
        break;
    case adios2::DataType::UInt64:
        break;
    case adios2::DataType::Float:
        break;
    case adios2::DataType::Double:
        bson_append_double(bsonMetadata, "min_float64", -1, variable.m_Min);
        bson_append_double(bsonMetadata, "max_float64", -1, variable.m_Max);
        if (variable.m_SingleValue)
        {
            // TODO: problems with different types; implement solution...
            bson_append_double(bsonMetadata, "value_float64", -1,
                               variable.m_Value);
        }
        break;
    case adios2::DataType::String:
    case adios2::DataType::LongDouble:
    case adios2::DataType::FloatComplex:
    case adios2::DataType::DoubleComplex:
        // TODO: implement
        break;
    case adios2::DataType::Compound:
        std::cout << "Compound variables not supported";
        break;
    }
}

template <>
void JuleaKVInteractionWriter::AppendMinMaxValueToBSON<std::string>(
    core::Variable<std::string> &variable, bson_t *bsonMetadata)
{
    // std::cout << "ParseVarTypeToBSON String: min = " << variable.Min()
    //   << std::endl;
}

template <>
void JuleaKVInteractionWriter::AppendMinMaxValueToBSON<long double>(
    core::Variable<long double> &variable, bson_t *bsonMetadata)
{
    // how to store long double in bson file?
    // std::cout << "ParseVarTypeToBSON long double: min = " << variable.Min()
    //   << std::endl;
}

template <>
void JuleaKVInteractionWriter::AppendMinMaxValueToBSON<std::complex<float>>(
    core::Variable<std::complex<float>> &variable, bson_t *bsonMetadata)
{
    // use two doubles? one for imaginary, one for real part?
    // std::cout << "ParseVarTypeToBSON std::complex<float>: min = "
    //   << variable.Min() << std::endl;
}

template <>
void JuleaKVInteractionWriter::AppendMinMaxValueToBSON<std::complex<double>>(
    core::Variable<std::complex<double>> &variable, bson_t *bsonMetadata)
{
    // use two doubles? one for imaginary, one for real part?
    // std::cout << "ParseVarTypeToBSON std::complex<double>: min = "
    //   << variable.Min() << std::endl;
}

template <class T>
void JuleaKVInteractionWriter::ParseVariableToBSON(core::Variable<T> &variable,
                                                   bson_t *bsonMetadata,
                                                   size_t currentStep)
{
    std::cout << "_____________________________________________" << std::endl;
    // std::cout << "Test" << std::endl;
    T min;
    T max;

    std::cout << "-- Variable bsonMetadata length: " << bsonMetadata->len
              << std::endl;
    uint data_size = 0;
    size_t number_elements = 0;
    size_t numberSteps = currentStep + 1;
    char *key;
    const int type = static_cast<int>(variable.m_Type);
    int shapeID = static_cast<int>(variable.m_ShapeID);

    size_t blocks[numberSteps];
    for (uint i = 0; i < numberSteps; i++)
    {
        blocks[i] = variable.m_AvailableStepBlockIndexOffsets[i].size();
        // std::cout << "i: " << i << "  blocks: " << blocks[i] << std::endl;
    }

    // std::cout << "shape_size " << variable.m_Shape.size() << std::endl;
    // std::cout << "start_size " << variable.m_Start.size() << std::endl;
    // std::cout << "count_size " << variable.m_Count.size() << std::endl;

    bson_append_int64(bsonMetadata, "shape_size", -1, variable.m_Shape.size());
    for (guint i = 0; i < variable.m_Shape.size(); ++i)
    {
        key = g_strdup_printf("shape_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Shape[i]);
    }
    bson_append_int32(bsonMetadata, "shape_id", -1, shapeID);
    bson_append_int32(bsonMetadata, "type_int", -1, type);

    bson_append_int64(bsonMetadata, "start_size", -1, variable.m_Start.size());
    for (guint i = 0; i < variable.m_Start.size(); ++i)
    {
        key = g_strdup_printf("start_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Start[i]);
    }

    bson_append_int64(bsonMetadata, "count_size", -1, variable.m_Count.size());
    for (guint i = 0; i < variable.m_Count.size(); ++i)
    {
        key = g_strdup_printf("count_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Count[i]);
        std::cout << "count " << i << variable.m_Count[i] << std::endl;
    }

    bson_append_int64(bsonMetadata, "memory_start_size", -1,
                      variable.m_MemoryStart.size());
    for (guint i = 0; i < variable.m_MemoryStart.size(); ++i)
    {
        key = g_strdup_printf("memory_start_%d", i);
        bson_append_int64(bsonMetadata, key, -1, variable.m_MemoryStart[i]);
    }

    bson_append_int64(bsonMetadata, "memory_count_size", -1,
                      variable.m_MemoryCount.size());
    for (guint i = 0; i < variable.m_MemoryCount.size(); ++i)
    {
        key = g_strdup_printf("memory_count_%d", i);
        bson_append_int64(bsonMetadata, key, -1, variable.m_MemoryCount[i]);
    }

    bson_append_int64(bsonMetadata, "steps_start", -1, variable.m_StepsStart);
    bson_append_int64(bsonMetadata, "steps_count", -1, variable.m_StepsCount);
    // std::cout << "DEBUG: steps_start" << variable.m_StepsStart << std::endl;
    // std::cout << "DEBUG: steps_count" << variable.m_StepsCount << std::endl;
    bson_append_int64(bsonMetadata, "number_steps", -1, numberSteps);
    std::cout << "numberSteps: " << numberSteps << "\n";
    for (guint i = 0; i < numberSteps; ++i)
    {
        key = g_strdup_printf("blockArray_%d", i);
        bson_append_int64(bsonMetadata, key, -1, blocks[i]);
        std::cout << "blockArray: " << key << " value: " << blocks[i] << "\n";
    }

    // bson_append_int64(bsonMetadata, "block_id", -1, variable.m_BlockID);
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
    std::cout << "data_size: " << data_size << std::endl;

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
    // std::cout << "-- bsonMetadata length: " << bsonMetadata->len <<
    // std::endl;
    g_free(key);
}

template <class T>
void JuleaKVInteractionWriter::ParseBlockToBSON(core::Variable<T> &variable,
                                                bson_t *bsonMetadata,
                                                T blockMin, T blockMax)
{
    std::cout << "_____________________________________________" << std::endl;
    T min;
    T max;

    // std::cout << "-- bsonMetadata length: " << bsonMetadata->len <<
    // std::endl;
    uint data_size = 0;
    // size_t number_elements = 0;

    char *key;
    bson_append_int64(bsonMetadata, "block_id", -1, variable.m_BlockID);
    bson_append_int64(bsonMetadata, "shape_size", -1, variable.m_Shape.size());
    for (guint i = 0; i < variable.m_Shape.size(); ++i)
    {
        key = g_strdup_printf("shape_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Shape[i]);
    }
    bson_append_int64(bsonMetadata, "start_size", -1, variable.m_Start.size());
    for (guint i = 0; i < variable.m_Start.size(); ++i)
    {
        key = g_strdup_printf("start_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Start[i]);
    }

    bson_append_int64(bsonMetadata, "count_size", -1, variable.m_Count.size());
    for (guint i = 0; i < variable.m_Count.size(); ++i)
    {
        key = g_strdup_printf("count_%d", i);

        bson_append_int64(bsonMetadata, key, -1, variable.m_Count[i]);
        std::cout << "count " << i << variable.m_Count[i] << std::endl;
    }

    bson_append_int64(bsonMetadata, "memory_start_size", -1,
                      variable.m_MemoryStart.size());
    for (guint i = 0; i < variable.m_MemoryStart.size(); ++i)
    {
        key = g_strdup_printf("memory_start_%d", i);
        bson_append_int64(bsonMetadata, key, -1, variable.m_MemoryStart[i]);
    }

    bson_append_int64(bsonMetadata, "memory_count_size", -1,
                      variable.m_MemoryCount.size());
    for (guint i = 0; i < variable.m_MemoryCount.size(); ++i)
    {
        key = g_strdup_printf("memory_count_%d", i);
        bson_append_int64(bsonMetadata, key, -1, variable.m_MemoryCount[i]);
    }

    bson_append_int64(bsonMetadata, "steps_start", -1, variable.m_StepsStart);
    bson_append_int64(bsonMetadata, "steps_count", -1, variable.m_StepsCount);

    bson_append_int64(bsonMetadata, "is_value", -1, variable.m_SingleValue);

    AppendMinMaxValueToBSON(variable, bsonMetadata);

    /* compute data_size; dimension entries !> 0 are ignored ?!*/
    // number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    // data_size = variable.m_ElementSize * number_elements;
    // bson_append_int64(bsonMetadata, "data_size", -1, data_size);
    // std::cout << "data_size: " << data_size << std::endl;

    std::cout << "-- block bsonMetadata length: " << bsonMetadata->len
              << std::endl;
    g_free(key);
}

/**
 *  Writes name of a variable to Julea KV. Also checks if name is
 * already in kv.
 */
void JuleaKVInteractionWriter::PutVarNameToJulea(
    std::string const projectNamespace, std::string const fileName,
    std::string const varName)
{
    bool err = false;
    guint32 valueLen = 0;
    bson_t *bsonNames;
    bson_iter_t bIter;
    bson_iter_t bIter2;

    void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "variable-names");

    /** store all variable names for a file = namespace */
    auto varNames = j_kv_new(completeNamespace, fileName.c_str());

    j_kv_get(varNames, &namesBuf, &valueLen, batch);
    err = j_batch_execute(batch);

    // JULEA does not return an error value but TRUE or FALSE
    if (err == false)
    {
        // std::cout << "something went wrong while retrieving the names from
        // kv\n";
    }
    // auto name = (char *)namesBuf;
    // std::cout << "namesBuf: " << namesBuf << "\n";
    // std::cout << "namesBuf: " << &namesBuf << "\n";
    // std::cout << "namesBuf: " << *&namesBuf << "\n";
    // std::cout << "name: " << name << "\n";
    // std::cout << "name: " << &name << "\n";
    // std::cout << "valueLen: " << valueLen << "\n";

    if (valueLen == 0)
    {
        bsonNames = bson_new();
        std::cout << "valueLen = 0 \n";
    }
    else
    {
        bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
        // std::cout << "value len not 0: new bson from data. namesBuf = " <<
        // namesBuf <<"\n"; std::cout << "value len not 0: new bson from data.
        // namesBuf = " << &namesBuf <<"\n"; std::cout << "value len not 0: new
        // bson from data. namesBuf = " << *namesBuf <<"\n";
    }
    free(namesBuf);

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&bIter, bsonNames, varName.c_str()))
    {
        bson_append_int32(bsonNames, varName.c_str(), -1, 42);
    }
    else
    {
        std::cout << "++ Julea Interaction Writer:  " << varName
                  << " already in kv store. " << std::endl;
    }

    // bson_iter_init_find(&bIter2, bsonNames, "T");
    // auto key = bson_iter_key (&bIter2);
    // auto value = bson_iter_value (&bIter2);
    // std::cout << "key: " << key << " value: " << value << "\n";

    // bson_iter_init_find(&bIter2, bsonNames, "P");
    // key = bson_iter_key (&bIter2);
    // value = bson_iter_value (&bIter2);
    // std::cout << "key: " << key << " value: " << value << "\n";

    namesBuf = g_memdup2(bson_get_data(bsonNames), bsonNames->len);
    j_kv_put(varNames, namesBuf, bsonNames->len, g_free, batch2);
    // err = j_batch_execute(batch2);
    g_assert_true(j_batch_execute(batch2) == true);

    // free(namesBuf); //TODO: why does this lead to segfaults?
    bson_destroy(bsonNames);
    j_kv_unref(varNames);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}

template <class T>
void JuleaKVInteractionWriter::PutVariableMetadataToJulea(
    core::Variable<T> &variable, const std::string projectNamespace,
    const std::string fileName, const std::string varName, size_t step,
    size_t block, bool original)
{
    auto bsonMetadata = bson_new();
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "variable-metadata");
    // auto fileVarStepBlock = g_strdup_printf(
    // "%s_%s_%d_%d", fileName.c_str(), variable.m_Name.c_str(), step, block);
    auto fileVar =
        g_strdup_printf("%s_%s", fileName.c_str(), variable.m_Name.c_str());
    // auto varMetadata = j_kv_new(completeNamespace, fileVarStepBlock);
    auto varMetadata = j_kv_new(completeNamespace, fileVar);

    ParseVariableToBSON(variable, bsonMetadata, step);

    j_kv_put(varMetadata, (gpointer)bson_get_data(bsonMetadata),
             bsonMetadata->len, g_free, batch);
    g_assert_true(j_batch_execute(batch) == true);
}

template <class T>
void JuleaKVInteractionWriter::PutBlockMetadataToJulea(
    core::Variable<T> &variable, const std::string projectNamespace,
    const std::string fileName, const std::string varName, size_t step,
    size_t block, const typename core::Variable<T>::Info &blockInfo,
    T &blockMin, T &blockMax, T &blockMean, T &blockSum, T &blockVar,
    uint32_t &entryID, bool original)
{
    auto bsonMetadata = bson_new();
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "block-metadata");
    auto fileVarStepBlock = g_strdup_printf(
        "%s_%s_%d_%d", fileName.c_str(), variable.m_Name.c_str(), step, block);
    auto varMetadata = j_kv_new(completeNamespace, fileVarStepBlock);

    ParseBlockToBSON(variable, bsonMetadata, blockMin, blockMax);

    j_kv_put(varMetadata, (gpointer)bson_get_data(bsonMetadata),
             bsonMetadata->len, g_free, batch);
    g_assert_true(j_batch_execute(batch) == true);
}

} // end namespace interop
} // end namespace adios2

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaKVInteractionWriter_TCC_ */
