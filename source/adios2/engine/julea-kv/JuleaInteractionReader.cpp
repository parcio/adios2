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

template <class T>
void GetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata,
                                  const std::string nameSpace,
                                  long unsigned int *dataSize)
{
    std::cout << "++ Julea Interaction Reader: GetVariableMetadataFromJulea"
              << std::endl;
    GetVariableBSONFromJulea(nameSpace, variable.m_Name, &bsonMetadata);

    ParseVariableFromBSON(variable, bsonMetadata, nameSpace, dataSize);
}

template <class T>
void GetVariableDataFromJulea(Variable<T> &variable, T *data,
                              const std::string nameSpace,
                              long unsigned int dataSize)
{
    std::cout << "-- Julea Interaction Reader: -- DEBUG 1: name = "
              << variable.m_Name << std::endl;

    guint64 bytesRead = 0;
    const char *varName = variable.m_Name.c_str();
    // auto batch = j_batch_new(m_JuleaSemantics);
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringDataObject =
        g_strdup_printf("%s_variables_%s", nameSpace.c_str(), varName);
    auto dataObject = j_object_new(stringDataObject, varName);

    std::cout << "___ Datasize = " << dataSize << std::endl;

    j_object_read(dataObject, data, dataSize, 0, &bytesRead, batch);
    j_batch_execute(batch);

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

    g_free(stringDataObject);
    j_object_unref(dataObject);
}

#define variable_template_instantiation(T)                                     \
    template void GetVariableMetadataFromJulea(                                \
        Variable<T> &variable, bson_t *bsonMetadata,                           \
        const std::string nameSpace, long unsigned int *dataSize);             \
    template void GetVariableDataFromJulea(Variable<T> &variable, T *data,     \
                                           const std::string nameSpace,        \
                                           long unsigned int dataSize);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

void GetNamesBSONFromJulea(const std::string nameSpace, bson_t **bsonNames,
                           unsigned int *varCount, const std::string kvName)
{
    std::cout << "-- GetNamesBSONFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    // g_autoptr(JKV) kv_object = NULL;
    void *namesBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    // auto kv_object = j_kv_new("variable_names", nameSpace.c_str());
    auto kvObject = j_kv_new(kvName.c_str(), nameSpace.c_str());

    j_kv_get(kvObject, &namesBuf, &valueLen, batch);
    j_batch_execute(batch);

    if (valueLen == 0)
    {
        // bsonNames = bson_new();
        // std::cout << "WARNING: The variable names key-value store is empty!"
        // << std::endl;
        std::cout << "WARNING: The kv store: " << kvName << " is empty!"
                  << std::endl;

        *varCount = 0;
        j_kv_unref(kvObject);
        j_batch_unref(batch);
        return;
    }
    else
    {
        *bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
        std::cout << "-- bsonNames length: " << (*bsonNames)->len << std::endl;
    }

    *varCount = bson_count_keys(*bsonNames);
    // printf("-- JADIOS DEBUG PRINT: count_names %d\n", *varCount);
    std::cout << "-- JADIOS DEBUG PRINT: count_names " << *varCount
              << std::endl;

    j_kv_unref(kvObject);
    j_batch_unref(batch);
}

// void GetNamesKVFromJulea(const std::string nameSpace, bson_t **bsonNames,
//                            unsigned int *varCount, const std::string kvName)
// {
//     // bson_t *bson_names;
//     // bson_iter_t b_iter;
//     guint32 value_len = 0;

//     // g_autoptr(JKV) kv_object = NULL;
//     void *names_buf = NULL;

//     auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
//     auto batch = j_batch_new(semantics);
//     // printf("-- JADIOS DEBUG PRINT: get_all_var_names_from_kv \n");

//     // auto kv_object = j_kv_new("variable_names", nameSpace.c_str());
//     auto kv_object = j_kv_new(kvName.c_str(), nameSpace.c_str());

//     j_kv_get(kv_object, &names_buf, &value_len, batch);
//     j_batch_execute(batch);

//     if (value_len == 0)
//     {
//         // bsonNames = bson_new();
//         // std::cout << "WARNING: The variable names key-value store is
//         empty!" << std::endl; std::cout << "WARNING: The kv store: " <<
//         kvName << " is empty!" << std::endl; *varCount = 0;
//         j_kv_unref(kv_object);
//         j_batch_unref(batch);
//         return;
//     }
//     else
//     {
//         *bsonNames = bson_new_from_data((const uint8_t *)names_buf,
//         value_len); std::cout << "-- bsonNames length: " << (*bsonNames)->len
//         << std::endl;

//     }

//     *varCount = bson_count_keys(*bsonNames);
//     // printf("-- JADIOS DEBUG PRINT: count_names %d\n", *varCount);
//     std::cout << "-- JADIOS DEBUG PRINT: count_names " << *varCount <<
//     std::endl;

//     j_kv_unref(kv_object);
//     j_batch_unref(batch);
// }

void GetVariableBSONFromJulea(const std::string nameSpace,
                              const std::string varName, bson_t **bsonMetadata)
{
    guint32 valueLen = 0;
    void *metaDataBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV = g_strdup_printf("variables_%s", nameSpace.c_str());
    auto kvObject = j_kv_new(stringMetadataKV, varName.c_str());
    // bson_metadata = bson_new();

    j_kv_get(kvObject, &metaDataBuf, &valueLen, batch);
    j_batch_execute(batch);

    if (valueLen == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The variable key-value store is empty! \n");
    }
    else
    {
        *bsonMetadata =
            bson_new_from_data((const uint8_t *)metaDataBuf, valueLen);
    }
}

/** ------------------------- Attributes -----------------------------------**/

template <class T>
void GetAttributeDataFromJulea(const std::string attrName, T *data,
                               const std::string nameSpace,
                               long unsigned int dataSize)
{
    std::cout << "-- Julea Interaction Reader: -- DEBUG 1: name = " << attrName
              << std::endl;

    guint64 bytesRead = 0;
    // const char *attrName = attribute.m_Name.c_str();
    // auto batch = j_batch_new(m_JuleaSemantics);
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringDataObject = g_strdup_printf(
        "%s_attributes_%s", nameSpace.c_str(), attrName.c_str());
    auto dataObject = j_object_new(stringDataObject, attrName.c_str());

    std::cout << "stringDataObject: " << stringDataObject << std::endl;

    std::cout << "___ GetAttributeDataFromJulea Datasize = " << dataSize
              << std::endl;

    j_object_read(dataObject, data, dataSize, 0, &bytesRead, batch);
    j_batch_execute(batch);

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

    g_free(stringDataObject);
    j_object_unref(dataObject);
}

#define attribute_template_instantiation(T)                                    \
    template void GetAttributeDataFromJulea(                                   \
        const std::string attrName, T *data, const std::string nameSpace,      \
        long unsigned int dataSize);
ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

void GetAttributeBSONFromJulea(const std::string nameSpace,
                               const std::string attrName,
                               bson_t **bsonMetadata)
{
    guint32 valueLen = 0;
    void *metaDataBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    std::cout << "-- GetAttributeBSONFromJulea 1111 " << std::endl;
    auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace.c_str());
    auto kvObject = j_kv_new(stringMetadataKV, attrName.c_str());
    // bson_metadata = bson_new();
    std::cout << "-- stringMetadataKV: " << stringMetadataKV << std::endl;

    j_kv_get(kvObject, &metaDataBuf, &valueLen, batch);
    j_batch_execute(batch);

    if (valueLen == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The attribute key-value store is empty! \n");
    }
    else
    {
        *bsonMetadata =
            bson_new_from_data((const uint8_t *)metaDataBuf, valueLen);
    }
}
// template <class T>
// void GetAttributeMetadataFromJulea(Attribute<T> &attribute, bson_t
// *bsonMetadata, const std::string nameSpace, long unsigned int dataSize)
void GetAttributeMetadataFromJulea(const std::string attrName,
                                   bson_t *bsonMetadata,
                                   const std::string nameSpace,
                                   long unsigned int *dataSize,
                                   size_t *numberElements, bool *IsSingleValue,
                                   int *type)
{
    // long unsigned int dataSize = 0;
    // size_t numberElements = 0;
    // bool IsSingleValue = false;

    std::cout << "++ Julea Interaction Reader: GetAttributeMetadataFromJulea"
              << std::endl;
    // GetVariableBSONFromJulea(nameSpace,attrName, &bsonMetadata);
    GetAttributeBSONFromJulea(nameSpace, attrName, &bsonMetadata);
    ParseAttributeFromBSON(nameSpace, attrName, bsonMetadata, dataSize,
                           numberElements, IsSingleValue, type);
    // ParseAttrTypeFromBSON();
    // ParseVariableFromBSON(attrName, bsonMetadata, nameSpace, dataSize);
}

// #define attribute_template_instantiation(T)                                     \
//     template void GetAttributeMetadataFromJulea(Attribute<T> &attribute, bson_t *bsonMetadata, const std::string nameSpace,long unsigned int dataSize);\
//     template void GetAttributeDataFromJulea(Attribute<T> &attribute, T *data, const std::string nameSpace,long unsigned int dataSize);\
// ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
// #undef attribute_template_instantiation

// #define variable_template_instantiation(T)                                      \
//     template void GetVariableDataFromJulea(Variable<T> &variable, const T *data, const std::string nameSpace);    \
//     template void OLDGetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata,           \
//                                                const std::string nameSpace);   \
//     template void GetAllAttributeNamesFromJulea(                               \
//         Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
//     template void GetAttributeDataFromJulea(                                   \
//         Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
//     template void GetAttributeMetadataFromJulea(Attribute<T> &attribute,       \
//                                                 bson_t *bsonMetadata,          \
//                                                 const std::string nameSpace);  \
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
