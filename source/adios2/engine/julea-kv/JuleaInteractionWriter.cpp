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

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

/**
 *  Writes name of variable/attribute to Julea KV. Also checks if name is
 * already in kv. paramName = name of variable/attribute nameSpace = file kvName
 * = variable_names/ attribute_names
 */
void PutNameToJulea(std::string paramName, std::string nameSpace,
                    std::string kvName)
{
    bool err = false;

    guint32 valueLen = 0;
    bson_t *bsonNames;
    bson_iter_t bIter;

    void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);
    auto name = strdup(paramName.c_str());

    std::cout << "kvName: " << kvName << std::endl;
    std::cout << "nameSpace: " << nameSpace << std::endl;
    std::cout << "paramName: " << paramName << std::endl;

    /** store all variable names for a file = namespace */
    auto kvObjectNames = j_kv_new(kvName.c_str(), nameSpace.c_str());

    j_kv_get(kvObjectNames, &namesBuf, &valueLen, batch);
    // err = j_batch_execute(batch);
    g_assert_true(j_batch_execute(batch) == true );

    std::cout << "valueLen: " << valueLen << std::endl;
    if (valueLen == 0)
    {
        bsonNames = bson_new();
    }
    else
    {
        bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }
    free(namesBuf);

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&bIter, bsonNames, name))
    {
        bson_append_int32(bsonNames, name, -1, 42);
    }
    else
    {
        std::cout << "++ Julea Interaction Writer:  " << name
                  << " already in kv store. " << std::endl;
    }

    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch2);
    // err = j_batch_execute(batch2);
    g_assert_true(j_batch_execute(batch2) == true );

    // free(namesBuf); //TODO: why does this lead to segfaults?
    free(name);
    bson_destroy(bsonNames);
    j_kv_unref(kvObjectNames);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}



void PutVariableMetadataToJulea(const std::string nameSpace, gpointer &buffer,
                                guint32 bufferLen, const std::string varName)
{
    // TODO: make sure that there are no races when updating
    // communicate to rest?
    // j_kv_get(kvVarMetadata, &metaDataBuf, &valueLen, batch);
    // j_batch_execute(batch);

    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV =
        g_strdup_printf("%s_%s", nameSpace.c_str(), "variables");
    auto kvVarMetadata = j_kv_new(stringMetadataKV, varName.c_str());
    std::cout << "--- WriteVarMetadataToJuleaKV --- kv = " << stringMetadataKV
              << " key = " << varName.c_str() << " --- " << std::endl;

    // buffer = SerializeVarMetadata(variable, &bufferLen, currStep);

    metaDataBuf = g_memdup(buffer, bufferLen);

    j_kv_put(kvVarMetadata, metaDataBuf, bufferLen, g_free, batch);
    g_assert_true(j_batch_execute(batch) == true );
    // j_batch_execute(batch);

    g_free(stringMetadataKV);
    j_kv_unref(kvVarMetadata);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

void PutBlockMetadataToJulea(const std::string nameSpace, gpointer &buffer,
                             guint32 bufferLen, const std::string stepBlockID)
{
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV =
        g_strdup_printf("%s_%s", nameSpace.c_str(), "variableblocks");
    std::cout << "stringMetadataKV " << stringMetadataKV << std::endl;

    auto kvObjectMetadata = j_kv_new(stringMetadataKV, stepBlockID.c_str());

    j_kv_put(kvObjectMetadata, buffer, bufferLen, g_free, batch);
    g_assert_true(j_batch_execute(batch) == true );
    // j_batch_execute(batch);

    g_free(stringMetadataKV);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}


template <class T>
void PutAttributeMetadataToJuleaSmall(Attribute<T> &attribute,
                                      bson_t *bsonMetaData,
                                      const std::string nameSpace)
{

    const char *kvNames = "attribute_names";
    const char *kvMD = "attributes";

    // TODO: more leaks than old version below ?!
    PutNameToJulea(attribute.m_Name, nameSpace.c_str(), kvNames);
    // WriteMetadataToJuleaKV(kvMD, attribute.m_Name, nameSpace.c_str(),
    // bsonMetaData);

    //  // TODO: check if update version is necessary!
    // bool IsAlreadyInKV = false;
    //  bson_t *bsonNames;

    // //  /* names_kv = kv holding all variable names */
    //  auto kvObjectNames = j_kv_new(kvNames, nameSpace.c_str());

    //  CheckIfAlreadyInKV(kvMD, attribute.m_Name, nameSpace.c_str(),
    //  &bsonNames,
    //                     kvObjectNames, &IsAlreadyInKV);

    //  if (!IsAlreadyInKV)
    //  {
    //      WriteNameToJuleaKVOld(kvMD, attribute.m_Name, nameSpace.c_str(),
    //      bsonNames,
    //                         kvObjectNames);
    //      std::cout << "Test IsAlreadyInKV " << IsAlreadyInKV << std::endl;
    //  }
    //  else
    //  {
    //      // UpdateMetadataInKV(kvMD, variable.m_Name, nameSpace.c_str(),
    //      //                    bsonNames, bsonMetaData, kvObjectNames);
    //      std::cout << "___ NEEDS UPDATE ___ " << std::endl;
    //  }
    // WriteMetadataToJuleaKV(kvMD, attribute.m_Name, nameSpace.c_str(),
    // bsonMetaData);

    // j_kv_unref(kvObjectNames);
    // bson_destroy(bsonNames);
}


template <class T>
void PutVariableDataToJulea(Variable<T> &variable, const T *data,
                            const std::string nameSpace, size_t currStep,
                            size_t block)
{
    guint64 bytesWritten = 0;
    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    auto dataSize = variable.m_ElementSize * numberElements;

    std::string objName = "variableblocks";
    auto stepBlockID = g_strdup_printf("%lu_%lu", currStep, block);
    std::cout << "stepBlockID: " << stepBlockID << std::endl;
    auto stringDataObject =
        g_strdup_printf("%s_%s", nameSpace.c_str(), objName.c_str());
    std::cout << "stringDataObject " << stringDataObject << std::endl;

    std::cout << "data [0] = " << data[0] << std::endl;
    std::cout << "data [1] = " << data[1] << std::endl;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto dataObject = j_object_new(stringDataObject, stepBlockID);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);
    g_assert_true(j_batch_execute(batch) == true );
    // j_batch_execute(batch);

    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for:  "
                  << stepBlockID << std::endl;
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
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction: PutVariableDataToJulea" << std::endl;
}

// FIXME: not yet implemented correctly! need to differentiate between strings
// and other types
template <class T>
void PutAttributeDataToJuleaSmall(Attribute<T> &attribute, const T *data,
                                  const std::string nameSpace)
{
    // std::string objName = "attributes";
    // unsigned int dataSize = 0;

    // if (attribute.m_IsSingleValue)
    // {
    //     // TODO: check if this is correct
    //     dataSize = sizeof(attribute.m_DataSingleValue);
    // }
    // else
    // {
    //     dataSize = attribute.m_DataArray.size();
    // }

    // WriteDataToJuleaObjectStore(objName, attribute.m_Name, nameSpace.c_str(),
    //                             dataSize, data);
    std::cout << "++ Julea Interaction: PutAttributeDataToJuleaSmall"
              << std::endl;
}

/** ------------ ATTRIBUTES -------------------------------------------------**/

template <class T>
void PutAttributeDataToJulea(Attribute<T> &attribute,
                             const std::string nameSpace)
{
    std::cout << "-- PutAttributeDataToJulea -------" << std::endl;
    void *dataBuf = NULL;
    guint64 bytesWritten = 0;
    unsigned int dataSize = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    char *cString;

    auto attrName = strdup(attribute.m_Name.c_str());
    auto numberElements = attribute.m_Elements;

    /* Write data pointer to object store*/
    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // g_strdup_printf("%s_attributes_%s", nameSpace.c_str(),
    // attrName);
    auto dataObject = j_object_new(stringDataObject, attrName);

    j_object_create(dataObject, batch);

    if (attribute.m_IsSingleValue)
    {
        dataSize = sizeof(attribute.m_DataSingleValue);
        j_object_write(dataObject, &attribute.m_DataSingleValue, dataSize, 0,
                       &bytesWritten, batch);
    }
    else
    {
        dataSize = attribute.m_DataArray.size() * sizeof(T);
        j_object_write(dataObject, attribute.m_DataArray.data(), dataSize, 0,
                       &bytesWritten, batch);
    }

    // j_batch_execute(batch);
    g_assert_true(j_batch_execute(batch) == true );
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for \
            attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << dataSize << " bytes!"
                  << std::endl;
    }

    free(attrName);
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

template <>
void PutAttributeDataToJulea<std::string>(Attribute<std::string> &attribute,
                                          const std::string nameSpace)
{
    std::cout << "-- PutAttributeDataToJulea -------" << std::endl;

    unsigned int dataSize = 0;
    guint64 bytesWritten = 0;
    void *dataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto attrName = strdup(attribute.m_Name.c_str());
    auto numberElements = attribute.m_Elements;

    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // g_strdup_printf("%s_attributes_%s", nameSpace.c_str(),
    // attrName);
    auto dataObject = j_object_new(stringDataObject, attrName);
    unsigned int offset = 0;

    j_object_create(dataObject, batch);

    if (attribute.m_IsSingleValue)
    {
        dataSize = attribute.m_DataSingleValue.length() + 1;
        char *dataElement = new char[dataSize];
        strcpy(dataElement, attribute.m_DataSingleValue.c_str());

        j_object_write(dataObject, dataElement, dataSize, 0, &bytesWritten,
                       batch);
        g_assert_true(j_batch_execute(batch) == true );
        // j_batch_execute(batch);
        delete[] dataElement;
    }
    else
    {
        for (size_t i = 0; i < attribute.m_DataArray.size(); ++i)
        {
            dataSize = attribute.m_DataArray.data()[i].length() + 1;
            j_object_write(dataObject, attribute.m_DataArray.data()[i].c_str(),
                           dataSize, offset, &bytesWritten, batch);
            offset += dataSize;
        }
        g_assert_true(j_batch_execute(batch) == true );
        // j_batch_execute(batch);
    }

    if (bytesWritten == offset)
    {
        std::cout << "++ Julea Interaction Writer: Data written for \
            attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << offset << " bytes! "
                  << std::endl;
    }
    free(attrName);
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

template <class T>
void PutAttributeMetadataToJulea(Attribute<T> &attribute, bson_t *bsonMetaData,
                                 const std::string nameSpace)
{
    std::cout << "-- PutAttributeMetadataToJulea ------ " << std::endl;
    guint32 valueLen = 0;

    bson_iter_t bIter;
    bson_t *bsonNames;

    void *namesBuf = NULL;
    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto attrName = strdup(attribute.m_Name.c_str());

    /* names_kv = kv holding all variable names */
    auto kvObjectNames = j_kv_new("attribute_names", nameSpace.c_str());
    j_kv_get(kvObjectNames, &namesBuf, &valueLen, batch);
    g_assert_true(j_batch_execute(batch) == true );
    // j_batch_execute(batch);

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
        // bson_append_int32(bsonNames, varName, -1,
        // bsonMetaData->var_type);
        // //FIXME: var_type?!
    }
    else
    {
        std::cout << "++ Julea Interaction Writer: Attribute " << attrName
                  << " already in kv store. " << std::endl;
        // TODO: update variable -> is there anything else necessary to
        // do?
    }

    /* Write metadata struct to kv store*/
    auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace.c_str());
    auto kvObjectMetadata = j_kv_new(stringMetadataKV, attrName);

    metaDataBuf = g_memdup(bson_get_data(bsonMetaData), bsonMetaData->len);
    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);

    j_kv_put(kvObjectMetadata, metaDataBuf, bsonMetaData->len, g_free, batch2);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch2);

    // j_batch_execute(batch2); // Writing metadata
    g_assert_true(j_batch_execute(batch2) == true );

    free(attrName);
    g_free(stringMetadataKV);
    j_kv_unref(kvObjectNames);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    bson_destroy(bsonNames);
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

#define declare_template_instantiation(T)                                      \
                                                                               \
    template void PutAttributeDataToJulea(Attribute<T> &attribute,             \
                                          const std::string nameSpace);        \
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
