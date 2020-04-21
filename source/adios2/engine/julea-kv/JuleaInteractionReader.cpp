/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaInteractionReader.h"

#include "JuleaFormatReader.h" //for ParseVariableFromBSON
#include "JuleaKVReader.h"

#include <bson.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

// template <class T>
// void GetVariableMetadataFromJulea(Variable<T> &variable,
//                                   const std::string nameSpace,
//                                   long unsigned int *dataSize, size_t step,
//                                   size_t block)
// {
//     std::cout << "-- GetVariableMetadataFromJulea -----" << std::endl;
//     bson_t *bsonMetadata = NULL;

//     GetVariableBSONFromJulea(nameSpace, variable.m_Name, &bsonMetadata);
//     ParseVariableFromBSON(variable, bsonMetadata, nameSpace, dataSize);
//     bson_destroy(bsonMetadata);
// }

void GetVariableMetadataFromJulea(const std::string nameSpace,
                                  const std::string varName, gpointer *md,
                                  guint32 *buffer_len)
{
    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV =
        g_strdup_printf("%s_%s", nameSpace.c_str(), "variables");
    std::cout << "stringMetadataKV: " << stringMetadataKV << std::endl;
    std::cout << "varName: " << varName << std::endl;
    auto kvVarMetadata = j_kv_new(stringMetadataKV, varName.c_str());

    j_kv_get(kvVarMetadata, md, buffer_len, batch);
    // j_kv_get(kvVarMetadata, &metaDataBuf, buffer_len, batch);

    // j_kv_get(kvVarMetadata, &metaDataBuf, &valueLen, batch);
    g_assert_true(j_batch_execute(batch) == true);
    // j_batch_execute(batch);

    // md = g_memdup(metaDataBuf, *buffer_len);
}

void GetBlockMetadataFromJulea(const std::string nameSpace,
                               const std::string varName, gpointer *md,
                               guint32 *buffer_len,
                               const std::string stepBlockID)
{
    // std::cout << "-- GetBlockMetadataFromJulea -----" << std::endl;
    // bson_t *bsonMetadata = NULL;

    void *metaDataBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV = g_strdup_printf("%s_%s_%s", nameSpace.c_str(),
                                            varName.c_str(), "variableblocks");
    // std::cout << "stringMetadataKV " << stringMetadataKV << std::endl;

    auto kvBlockMetadata = j_kv_new(stringMetadataKV, stepBlockID.c_str());

    j_kv_get(kvBlockMetadata, md, buffer_len, batch);
    g_assert_true(j_batch_execute(batch) == true);
    // j_batch_execute(batch);

    g_free(stringMetadataKV);
    j_kv_unref(kvBlockMetadata);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void ReadVarMetadataFromJuleaKV(Variable<T> &variable,
                                const std::string nameSpace, size_t currStep)
{
    // guint32 valueLen = 0;
    // void *metaDataBuf = NULL;

    // auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    // auto batch = j_batch_new(semantics);

    // auto stringMetadataKV =
    //     g_strdup_printf("%s_%s", nameSpace.c_str(), "variables");
    // auto kvVarMetadata = j_kv_new(stringMetadataKV, variable.m_Name.c_str());
    // // std::cout << "nameSpace " << nameSpace << std::endl;
    // // std::cout << "stringMetadataKV " << stringMetadataKV << std::endl;

    // JuleaKVWriter::StepMetadata *md =
    // g_slice_new(JuleaKVWriter::StepMetadata);

    // j_kv_get(kvVarMetadata, (gpointer *) &md, &valueLen, batch);
    // j_batch_execute(batch);

    // std::cout << "md->numberSteps: " << md->numberSteps << std::endl;
    //     std::cout << "md->blocks: " << md->blocks[0] << std::endl;
    //     std::cout << "md->blocks: " << md->blocks[1] << std::endl;
    // std::cout << "======== DEBUG =====================" << std::endl;
}

template <class T>
void GetVariableDataFromJulea(Variable<T> &variable, T *data,
                              const std::string nameSpace, size_t dataSize,
                              size_t step, size_t block)
{
    // std::cout << "-- GetVariableDataFromJulea ----- " << std::endl;
    // std::cout << "-- Datasize = " << dataSize << std::endl;

    guint64 bytesRead = 0;
    const char *varName = variable.m_Name.c_str();

    std::string objName = "variableblocks";
    auto stringDataObject =
        g_strdup_printf("%s_%s_%s", nameSpace.c_str(), variable.m_Name.c_str(),
                        objName.c_str());
    // std::cout << "stringDataObject: " << stringDataObject << std::endl;

    auto stepBlockID = g_strdup_printf("%lu_%lu", step, block);
    // std::cout << "stepBlockID: " << stepBlockID << std::endl;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto dataObject = j_object_new(stringDataObject, stepBlockID);

    j_object_read(dataObject, data, dataSize, 0, &bytesRead, batch);
    // j_batch_execute(batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesRead == dataSize)
    {
        std::cout << "++ Julea Interaction Reader: Read data for variable "
                  << varName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesRead << " bytes read instead of "
                  << dataSize << " bytes!" << std::endl;
    }

    j_batch_unref(batch);
    j_semantics_unref(semantics);
    g_free(stringDataObject);
    j_object_unref(dataObject);
}
// template void GetBlockMetadataFromJulea(Variable<T> &variable,          \
    //                                            const std::string nameSpace,    \
    //                                            long unsigned int *dataSize, size_t step, size_t block);   \

#define variable_template_instantiation(T)                                     \
    template void GetVariableDataFromJulea(                                    \
        Variable<T> &variable, T *data, const std::string nameSpace,           \
        long unsigned int dataSize, size_t step, size_t block);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

void GetNamesFromJulea(const std::string nameSpace, bson_t **bsonNames,
                       unsigned int *varCount, bool isVariable)
{
    std::cout << "-- GetNamesFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    void *namesBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    char *kvName;

    if (isVariable)
    {
        kvName = "variable_names";
    }
    else
    {
        kvName = "attribute_names";
    }

    auto kvObject = j_kv_new(kvName, nameSpace.c_str());
    std::cout << "kvName :" << kvName << std::endl;

    j_kv_get(kvObject, &namesBuf, &valueLen, batch);
    j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    if (valueLen == 0)
    {
        // bsonNames = bson_new();
        std::cout << "WARNING: The kv store: " << kvName << " is empty!"
                  << std::endl;

        *varCount = 0;
        free(namesBuf);
        j_semantics_unref(semantics);
        j_kv_unref(kvObject);
        j_batch_unref(batch);
        return;
    }
    else
    {
        *bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
        // std::cout << "-- bsonNames length: " << (*bsonNames)->len <<
        // std::endl;
    }

    *varCount = bson_count_keys(*bsonNames);
    // printf("-- JADIOS DEBUG PRINT: count_names %d\n", *varCount);
    // std::cout << "-- count_names: " << *varCount << std::endl;

    j_semantics_unref(semantics);
    j_kv_unref(kvObject);
    j_batch_unref(batch);
    free(namesBuf);
}

void GetVariableBSONFromJulea(const std::string nameSpace,
                              const std::string varName, bson_t **bsonMetadata)
{
    guint32 valueLen = 0;
    void *metaDataBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    std::cout << "-- GetVariableBSONFromJulea ---- " << std::endl;

    auto stringMetadataKV = g_strdup_printf("%s_variables", nameSpace.c_str());
    std::cout << "stringMetadataKV: " << stringMetadataKV << std::endl;
    auto kvObject = j_kv_new(stringMetadataKV, varName.c_str());

    j_kv_get(kvObject, &metaDataBuf, &valueLen, batch);
    // j_batch_execute(batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (valueLen == 0)
    {
        printf("WARNING: The variable key-value store is empty! \n");
    }
    else
    {
        *bsonMetadata =
            bson_new_from_data((const uint8_t *)metaDataBuf, valueLen);
    }

    g_free(stringMetadataKV);
    j_kv_unref(kvObject);
    j_semantics_unref(semantics);
    j_batch_unref(batch);
    free(metaDataBuf);
}

/** ------------------------- Attributes -----------------------------------**/

void GetAttributeBSONFromJulea(const std::string nameSpace,
                               const std::string attrName,
                               bson_t **bsonMetadata, guint32 *valueLen)
{
    // guint32 valueLen = 0;
    void *metaDataBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    std::cout << "-- GetAttributeBSONFromJulea ---- " << std::endl;

    auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace.c_str());
    auto kvObject = j_kv_new(stringMetadataKV, attrName.c_str());
    // std::cout << "-- stringMetadataKV: " << stringMetadataKV << std::endl;

    j_kv_get(kvObject, &metaDataBuf, valueLen, batch);
    j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    if (valueLen == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The attribute key-value store is empty! \n");
    }
    else
    {
        *bsonMetadata =
            bson_new_from_data((const uint8_t *)metaDataBuf, *valueLen);
    }
    free(metaDataBuf);
    j_kv_unref(kvObject);
    j_semantics_unref(semantics);
    j_batch_unref(batch);
    g_free(stringMetadataKV);
}

void GetAttributeMetadataFromJulea(const std::string attrName,
                                   const std::string nameSpace,
                                   long unsigned int *dataSize,
                                   size_t *numberElements, bool *IsSingleValue,
                                   int *type)
{
    bson_t *bsonMetadata;
    std::cout << "-- GetAttributeMetadataFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    GetAttributeBSONFromJulea(nameSpace, attrName, &bsonMetadata, &valueLen);

    if (valueLen > 0)
    {
        ParseAttributeFromBSON(nameSpace, attrName, bsonMetadata, dataSize,
                               numberElements, IsSingleValue, type);
    }
    bson_destroy(bsonMetadata);
}

void GetAttributeMetadataFromJulea(const std::string attrName,
                                   const std::string nameSpace,
                                   long unsigned int *completeSize,
                                   size_t *numberElements, bool *IsSingleValue,
                                   int *type, unsigned long **dataSizes)
{
    bson_t *bsonMetadata;
    std::cout << "-- GetAttributeMetadataFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    GetAttributeBSONFromJulea(nameSpace, attrName, &bsonMetadata, &valueLen);

    if (valueLen > 0)
    {
        ParseAttributeFromBSON(nameSpace, attrName, bsonMetadata, completeSize,
                               numberElements, IsSingleValue, type, dataSizes);
    }
    bson_destroy(bsonMetadata);
}

template <class T>
void GetAttributeDataFromJulea(const std::string attrName, T *data,
                               const std::string nameSpace,
                               long unsigned int dataSize)
{
    std::cout << "-- GetAttributeDataFromJulea -----" << std::endl;

    guint64 bytesRead = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    // auto batch = j_batch_new(m_JuleaSemantics);

    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // "%s_attributes_%s", nameSpace.c_str(), attrName.c_str());
    auto dataObject = j_object_new(stringDataObject, attrName.c_str());

    // std::cout << "-- stringDataObject: " << stringDataObject << std::endl;
    // std::cout << "-- Datasize = " << dataSize << std::endl;

    j_object_read(dataObject, data, dataSize, 0, &bytesRead, batch);
    // j_batch_execute(batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesRead == dataSize)
    {
        std::cout << "++ Julea Interaction Reader: Read data for attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesRead << " bytes read instead of "
                  << dataSize << " bytes!" << std::endl;
    }

    j_batch_unref(batch);
    j_semantics_unref(semantics);
    g_free(stringDataObject);
    j_object_unref(dataObject);
}

void GetAttributeStringDataFromJulea(const std::string attrName, char *data,
                                     const std::string nameSpace,
                                     long unsigned int completeSize,
                                     bool IsSingleValue, size_t numberElements)
{
    std::cout << "-- GetAttributeDataFromJulea -- String version -----"
              << std::endl;

    guint64 bytesRead = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    // void *dataBuf;

    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // "%s_attributes_%s", nameSpace.c_str(), attrName.c_str());
    auto dataObject = j_object_new(stringDataObject, attrName.c_str());

    // std::cout << "-- stringDataObject: " << stringDataObject << std::endl;
    // std::cout << "-- Datasize = " << completeSize << std::endl;

    if (IsSingleValue)
    {
        char *charArray = data;

        j_object_read(dataObject, charArray, completeSize, 0, &bytesRead,
                      batch);
        // j_batch_execute(batch);
        g_assert_true(j_batch_execute(batch) == true);
        // std::cout << "-- charArray = " << charArray << std::endl;
    }
    else
    {
        char *stringArray = data;

        j_object_read(dataObject, stringArray, completeSize, 0, &bytesRead,
                      batch);
        // j_batch_execute(batch);
        g_assert_true(j_batch_execute(batch) == true);
        // std::cout << "string: " << stringArray << std::endl;
    }
    if (bytesRead == completeSize)
    {
        std::cout << "++ Julea Interaction Reader: Read data for attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesRead << " bytes read instead of "
                  << completeSize << " bytes!" << std::endl;
    }

    j_batch_unref(batch);
    j_semantics_unref(semantics);
    g_free(stringDataObject);
    j_object_unref(dataObject);
}

#define attribute_template_instantiation(T)                                    \
    template void GetAttributeDataFromJulea(                                   \
        const std::string attrName, T *data, const std::string nameSpace,      \
        long unsigned int dataSize);
ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
