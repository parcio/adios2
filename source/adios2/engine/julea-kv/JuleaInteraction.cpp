/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaInteraction.h"
#include "JuleaMetadata.h"

#include <julea-config.h>

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
#include <julea-internal.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
void PutVariableDataToJulea(Variable<T> &variable, const T *data, const char *nameSpace)
{
    guint64 bytesWritten = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto varName = strdup(variable.m_Name.c_str());
    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    auto dataSize = variable.m_ElementSize * numberElements;

    /* Write data pointer to object store*/
    auto stringDataObject =
        g_strdup_printf("%s_variables_%s", nameSpace, varName);
    auto dataObject = j_object_new(stringDataObject, varName);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Client Logic: Data written for variable " << varName << std::endl;
    }
    else
    {
        std::cout<< "WARNING: only " << bytesWritten << " bytes written instead of " << dataSize << " bytes! " << std::endl;
    }
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);

    std::cout << "++ Julea Client Logic: Put Variable " << std::endl;
}

template <class T>
void PutVariableMetadataToJulea(Variable<T> &variable, const bson_t *bsonMetaData, const char *nameSpace)
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
    auto kvObjectNames = j_kv_new("variable_names", nameSpace);
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
    if (!bson_iter_init_find(&bIter, bsonNames, varName))
    {
        std::cout << "Init b_iter successfull " << std::endl;
        // bson_append_int32(bsonNames, varName, -1, bsonMetaData->var_type); //FIXME: var_type?!
    }
    else
    {
        std::cout << "++ Julea Client Logic: Variable " << varName << " already in kv store. " << std::endl;
        // TODO: update variable -> is there anything else necessary to do?
    }

    /* Write metadata struct to kv store*/
    auto stringMetadataKV = g_strdup_printf("variables_%s", nameSpace);
    auto kvObjectMetadata = j_kv_new(stringMetadataKV, varName);

    metaDataBuf =
        g_memdup(bson_get_data(bsonMetaData), bsonMetaData->len);
    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);

    j_kv_put(kvObjectMetadata, metaDataBuf, bsonMetaData->len, g_free,
             batch2);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch2);

    j_batch_execute(batch2); // Writing metadata

    g_free(stringMetadataKV);
    j_kv_unref(kvObjectNames);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    bson_destroy(bsonNames);

    std::cout << "++ Julea Client Logic: Put Variable " << std::endl;
}

template <class T>
void PutAttributeDataToJulea(Attribute<T> &attribute, const T *data, const char *nameSpace)
{
    guint64 bytesWritten = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto attrName = strdup(attribute.m_Name.c_str());
    // auto numberElements = adios2::helper::GetTotalSize(attribute.m_Count);
    int dataSize = 0;
    int numberElements = 0;
    // auto dataSize = attribute.m_ElementSize * numberElements;

    /* Write data pointer to object store*/
    auto stringDataObject =
        g_strdup_printf("%s_attributes_%s", nameSpace, attrName);
    auto dataObject = j_object_new(stringDataObject, attrName);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Client Logic: Data written for attribute " << attrName << std::endl;
    }
    else
    {
        std::cout<< "WARNING: only " << bytesWritten << " bytes written instead of " << dataSize << " bytes! " << std::endl;
    }
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);

    std::cout << "++ Julea Client Logic: Put Variable " << std::endl;
}



#define declare_template_instantiation(T)                                      \
    template void PutVariableDataToJulea(Variable<T> &variable, const T *data, const char *name_space);              \
    template void PutVariableMetadataToJulea(Variable<T> &variable, const bson_t *bson_meta_data, const char *name_space);        \
    template void PutAttributeDataToJulea(Attribute<T> &attribute, const T *data, const char *nameSpace);\

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation


} // end namespace engine
} // end namespace core
} // end namespace adios2