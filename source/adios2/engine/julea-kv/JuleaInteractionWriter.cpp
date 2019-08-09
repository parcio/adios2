/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaInteractionWriter.h"
#include "JuleaMetadata.h"

#include <julea-config.h>

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
// #include <julea-internal.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

/** -------------------------------------------------------------------------**/
/** -------------- TESTING GENERIC FUNCTIONS --------------------------------**/
/** -------------------------------------------------------------------------**/

/**
 * kvName = variables/attributes
 * paramName = variableName/attributeName
 */
void CheckIfAlreadyInKV(std::string kvName, std::string paramName,
                        std::string nameSpace, bson_t *bsonNames,
                        JKV *kvObjectNames)
{
    guint64 bytesWritten = 0;
    guint32 valueLen = 0;

    bson_iter_t bIter;
    // bson_t *bsonNames;
    void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    // auto batch2 = j_batch_new(semantics);

    auto name = strdup(paramName.c_str());

    // auto batch_2 = j_batch_new(j_batch_get_semantics(batch));

    /* names_kv = kv holding all variable names */
    // auto kvObjectNames = j_kv_new(kvName.c_str(), nameSpace.c_str());
    j_kv_get(kvObjectNames, &namesBuf, &valueLen, batch);
    j_batch_execute(batch);

    if (valueLen == 0)
    {
        bsonNames = bson_new();
    }
    else
    {
        bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&bIter, bsonNames, name))
    {
        std::cout << "Init b_iter successfull " << std::endl;
        // bson_append_int32(bsonNames, varName, -1, bsonMetaData->var_type);
        // //FIXME: var_type?!
    }
    else
    {
        std::cout << "++ Julea Interaction Writer: Attribute " << name
                  << " already in kv store. " << std::endl;
        // TODO: update variable -> is there anything else necessary to do?
    }

    j_kv_unref(kvObjectNames);
    // j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    // j_batch_unref(batch2);
    // bson_destroy(bsonNames);
}

void WriteMetadataToJuleaKV(std::string kvName, std::string paramName,
                            std::string nameSpace, bson_t *bsonNames,
                            bson_t *bsonMetaData, JKV *kvObjectNames)
{
    void *namesBuf = NULL;
    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    /* Put name to names kv */
    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch);

    /* Write metadata struct to kv store*/
    // auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace);
    auto stringMetadataKV =
        g_strdup_printf("%s_%s", kvName.c_str(), nameSpace.c_str());
    auto kvObjectMetadata = j_kv_new(stringMetadataKV, paramName.c_str());

    metaDataBuf = g_memdup(bson_get_data(bsonMetaData), bsonMetaData->len);
    j_kv_put(kvObjectMetadata, metaDataBuf, bsonMetaData->len, g_free, batch);

    // j_batch_execute(batch);
    // j_kv_unref(kvObjectMetadata);
    // g_free(stringMetadataKV);
}

template <class T>
void PutAttributeMetadataToJuleaSmall(Attribute<T> &attribute,
                                      bson_t *bsonMetaData,
                                      const std::string nameSpace)
{
    bson_t *bsonNames = bson_new(); // FIXME
    std::string kvName = "attributes";
    const char *kvNameC = kvName.c_str();

    /* names_kv = kv holding all attribute names */
    auto kvObjectNames = j_kv_new(kvNameC, nameSpace.c_str());
    CheckIfAlreadyInKV(kvNameC, attribute.m_Name, nameSpace.c_str(), bsonNames,
                       kvObjectNames);
    WriteMetadataToJuleaKV(kvNameC, attribute.m_Name, nameSpace.c_str(),
                           bsonNames, bsonMetaData, kvObjectNames);
    // std::cout << "++ Julea Interaction: PutAttributeMetadataToJuleaSmall  "
    // << std::endl;
}

template <class T>
void PutVariableMetadataToJuleaSmall(Variable<T> &variable,
                                     bson_t *bsonMetaData,
                                     const std::string nameSpace)
{
    bson_t *bsonNames = bson_new(); // FIXME
    std::string kvName = "variables";
    const char *kvNameC = kvName.c_str();

    /* names_kv = kv holding all variable names */
    auto kvObjectNames = j_kv_new(kvNameC, nameSpace.c_str());

    CheckIfAlreadyInKV(kvNameC, variable.m_Name, nameSpace.c_str(), bsonNames,
                       kvObjectNames);
    WriteMetadataToJuleaKV(kvNameC, variable.m_Name, nameSpace.c_str(),
                           bsonNames, bsonMetaData, kvObjectNames);

    // std::cout << "++ Julea Interaction: PutVariableMetadataToJuleaSmall  " <<
    // std::endl;
}

/** ------------------------- DATA ------------------------------------------**/

void WriteDataToJuleaObjectStore(std::string objName, std::string paramName,
                                 std::string nameSpace, unsigned int dataSize,
                                 const void *data)
{
    guint64 bytesWritten = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto name = strdup(paramName.c_str());

    /* Write data pointer to object store*/
    // auto stringDataObject =
    // g_strdup_printf("%s_variables_%s", nameSpace, name);
    auto stringDataObject =
        g_strdup_printf("%s_%s_%s", nameSpace.c_str(), objName.c_str(), name);
    auto dataObject = j_object_new(stringDataObject, name);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for:  " << name
                  << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << dataSize << " bytes! "
                  << std::endl;
    }
    // g_free(stringDataObject);
    // j_object_unref(dataObject);
    // j_batch_unref(batch);
}

template <class T>
void PutVariableDataToJuleaSmall(Variable<T> &variable, const T *data,
                                 const std::string nameSpace)
{

    std::string objName = "variables";
    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    auto dataSize = variable.m_ElementSize * numberElements;

    WriteDataToJuleaObjectStore(objName, variable.m_Name, nameSpace.c_str(),
                                dataSize, data);
    std::cout << "++ Julea Interaction: PutVariableDataToJuleaSmall"
              << std::endl;
}

template <class T>
void PutAttributeDataToJuleaSmall(Attribute<T> &attribute, const T *data,
                                  const std::string nameSpace)
{
    std::string objName = "attributes";
    // unsigned int dataSize = -1; //does this work?
    unsigned int dataSize = 0; //does this work?

    if (attribute.m_IsSingleValue)
    {
        // TODO: check if this is correct
        dataSize = sizeof(attribute.m_DataSingleValue);
    }
    else
    {
        dataSize = attribute.m_DataArray.size();
    }

    WriteDataToJuleaObjectStore(objName, attribute.m_Name, nameSpace.c_str(),
                                dataSize, data);
    std::cout << "++ Julea Interaction: PutAttributeDataToJuleaSmall"
              << std::endl;
}

/** -------------------------------------------------------------------------**/
/** -------------- TESTING GENERIC FUNCTIONS END ----------------------------**/
/** -------------------------------------------------------------------------**/

template <class T>
void PutVariableDataToJulea(Variable<T> &variable, const T *data,
                            const std::string nameSpace)
{
    guint64 bytesWritten = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto varName = strdup(variable.m_Name.c_str());
    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    auto dataSize = variable.m_ElementSize * numberElements;

    /* Write data pointer to object store*/
    auto stringDataObject =
        g_strdup_printf("%s_variables_%s", nameSpace.c_str(), varName);
    auto dataObject = j_object_new(stringDataObject, varName);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for variable "
                  << varName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << dataSize << " bytes! "
                  << std::endl;
    }
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);

    std::cout << "++ Julea Interaction Writer: Put Variable " << std::endl;
}

template <class T>
void PutVariableMetadataToJulea(Variable<T> &variable, bson_t *bsonMetaData,
                                const std::string nameSpace)
{
    // guint64 bytesWritten = 0;
    guint32 valueLen = 0;

    bson_iter_t bIter;
    bson_t *bsonNames;

    void *namesBuf = NULL;
    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto varName = strdup(variable.m_Name.c_str());

    // auto batch_2 = j_batch_new(j_batch_get_semantics(batch));

    /* names_kv = kv holding all variable names */
    auto kvObjectNames = j_kv_new("variable_names", nameSpace.c_str());
    j_kv_get(kvObjectNames, &namesBuf, &valueLen, batch);
    j_batch_execute(batch);

    std::cout << "-- namesKV: valueLen = " << valueLen << std::endl;
    if (valueLen == 0)
    {
        bsonNames = bson_new();
    }
    else
    {
        bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&bIter, bsonNames, varName))
    {
        std::cout << "Init b_iter successfull " << std::endl;
        bson_append_int32(bsonNames, varName, -1, 42);
        std::cout << "-- bsonNames length: " << bsonNames->len << std::endl;

        // bson_append_int32(bsonNames, varName, -1, bsonMetaData->var_type);
        // //FIXME: var_type?!
    }
    else
    {
        std::cout << "++ Julea Interaction Writer: Variable " << varName
                  << " already in kv store. " << std::endl;
        // TODO: update variable -> is there anything else necessary to do?
        std::cout << "-- bsonNames length: " << bsonNames->len << std::endl;
    }

    /* Write metadata struct to kv store*/
    auto stringMetadataKV = g_strdup_printf("variables_%s", nameSpace.c_str());
    auto kvObjectMetadata = j_kv_new(stringMetadataKV, varName);

    metaDataBuf = g_memdup(bson_get_data(bsonMetaData), bsonMetaData->len);
    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);

    j_kv_put(kvObjectMetadata, metaDataBuf, bsonMetaData->len, g_free, batch2);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch2);

    j_batch_execute(batch2); // Writing metadata

    g_free(stringMetadataKV);
    j_kv_unref(kvObjectNames);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    bson_destroy(bsonNames);

    std::cout << "++ Julea Interaction Writer: Put Variable " << std::endl;
}

/** ------------ ATTRIBUTES -------------------------------------------------**/

template <class T>
void PutAttributeDataToJulea(Attribute<T> &attribute, const T *data,
                             const std::string nameSpace)
{
    guint64 bytesWritten = 0;
    unsigned int dataSize = -1;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto attrName = strdup(attribute.m_Name.c_str());
    // auto numberElements = adios2::helper::GetTotalSize(attribute.m_Elements);
    auto numberElements = attribute.m_Elements;

    if (attribute.m_IsSingleValue)
    {
        // TODO: check if this is correct
        dataSize = sizeof(attribute.m_DataSingleValue);
    }
    else
    {
        dataSize = attribute.m_DataArray.size();
    }
    /* Write data pointer to object store*/
    auto stringDataObject =
        g_strdup_printf("%s_attributes_%s", nameSpace.c_str(), attrName);
    auto dataObject = j_object_new(stringDataObject, attrName);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << dataSize << " bytes! "
                  << std::endl;
    }
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);

    std::cout << "++ Julea Interaction Writer: Put Variable " << std::endl;
}

template <class T>
void PutAttributeMetadataToJulea(Attribute<T> &attribute, bson_t *bsonMetaData,
                                 const std::string nameSpace)
{
    // guint64 bytesWritten = 0;
    guint32 valueLen = 0;

    bson_iter_t bIter;
    bson_t *bsonNames;

    void *namesBuf = NULL;
    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto attrName = strdup(attribute.m_Name.c_str());

    // auto batch_2 = j_batch_new(j_batch_get_semantics(batch));

    /* names_kv = kv holding all variable names */
    auto kvObjectNames = j_kv_new("attribute_names", nameSpace.c_str());
    j_kv_get(kvObjectNames, &namesBuf, &valueLen, batch);
    j_batch_execute(batch);

    if (valueLen == 0)
    {
        bsonNames = bson_new();
    }
    else
    {
        bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&bIter, bsonNames, attrName))
    {
        std::cout << "Init b_iter successfull " << std::endl;
        // bson_append_int32(bsonNames, varName, -1, bsonMetaData->var_type);
        // //FIXME: var_type?!
    }
    else
    {
        std::cout << "++ Julea Interaction Writer: Attribute " << attrName
                  << " already in kv store. " << std::endl;
        // TODO: update variable -> is there anything else necessary to do?
    }

    /* Write metadata struct to kv store*/
    auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace.c_str());
    auto kvObjectMetadata = j_kv_new(stringMetadataKV, attrName);

    metaDataBuf = g_memdup(bson_get_data(bsonMetaData), bsonMetaData->len);
    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);

    j_kv_put(kvObjectMetadata, metaDataBuf, bsonMetaData->len, g_free, batch2);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch2);

    j_batch_execute(batch2); // Writing metadata

    g_free(stringMetadataKV);
    j_kv_unref(kvObjectNames);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    bson_destroy(bsonNames);

    std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

#define declare_template_instantiation(T)                                      \
    template void PutVariableDataToJulea(Variable<T> &variable, const T *data, \
                                         const std::string nameSpace);         \
    template void PutVariableDataToJuleaSmall(                                 \
        Variable<T> &variable, const T *data, const std::string nameSpace);    \
    template void PutVariableMetadataToJulea(Variable<T> &variable,            \
                                             bson_t *bsonMetaData,             \
                                             const std::string nameSpacee);    \
    template void PutVariableMetadataToJuleaSmall(                             \
        Variable<T> &variable, bson_t *bsonMetaData,                           \
        const std::string nameSpace);                                          \
    template void PutAttributeDataToJulea(                                     \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    template void PutAttributeDataToJuleaSmall(                                \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    template void PutAttributeMetadataToJulea(Attribute<T> &attribute,         \
                                              bson_t *bsonMetaData,            \
                                              const std::string nameSpace);    \
    template void PutAttributeMetadataToJuleaSmall(                            \
        Attribute<T> &attribute, bson_t *bsonMetaData,                         \
        const std::string nameSpace);

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
