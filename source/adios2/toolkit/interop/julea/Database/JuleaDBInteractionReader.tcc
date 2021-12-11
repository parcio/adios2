/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_TCC_

#include "JuleaDBInteractionReader.h"
// #include "JuleaDBDAIInteractionReader.h"
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
void JuleaDBInteractionReader::GetCountFromBlockMetadata(const std::string nameSpace,
                               const std::string varName, size_t step,
                               size_t block, Dims *count, size_t entryID,
                               bool isLocalValue, T *value)
{
    // std::cout << "------ GetCountFromBlockMetadata ----------" << std::endl;
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBSelector) selectorShort = NULL;
    std::string valueField;
    const char *varType;

    size_t *countSize;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    // selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    // j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
    //                         nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
    //                         NULL);
    // j_db_selector_add_field(selector, "variableName",
    // J_DB_SELECTOR_OPERATOR_EQ,
    //                         varName.c_str(), strlen(varName.c_str()) + 1,
    //                         NULL);
    // j_db_selector_add_field(selector, "step", J_DB_SELECTOR_OPERATOR_EQ,
    // &step,
    //                         sizeof(step), NULL);
    // j_db_selector_add_field(selector, "block", J_DB_SELECTOR_OPERATOR_EQ,
    //                         &block, sizeof(block), NULL);
    // iterator = j_db_iterator_new(schema, selector, NULL);

    selectorShort = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selectorShort, "_id", J_DB_SELECTOR_OPERATOR_EQ,
                            &entryID, sizeof(entryID), NULL);

    iterator = j_db_iterator_new(schema, selectorShort, NULL);
    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t *tmpCountBuffer;
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);
            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
            *count = tmpCount;
            g_free(tmpCountBuffer);
        }
        // if(isLocalValue)
        // {
        //     //FIXME: not yet tested!
        //     JuleaInteraction::SetMinMaxValueFields(NULL, NULL,valueField, varType );
        //     std::cout << "valueField: " << valueField << std::endl;
        //     j_db_iterator_get_field(iterator, valueField.c_str(), &type,
        //                             (gpointer *)&value, &db_length,
        //                             NULL);
        // }
    }
    g_free(countSize);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void JuleaDBInteractionReader::GetBlockMetadataNEW(core::Variable<T> &variable,
                           typename core::Variable<T>::Info &blockInfo,
                           size_t entryID)
{
    // std::cout << "--- DBGetBlockMetadata ---" << std::endl;
    // std::unique_ptr<typename Variable<T>::Info> blockInfo(
    // new (typename Variable<T>::Info));
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    // g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    // g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBSelector) selectorShort = NULL;

    // const char *varName = variableName.c_str();
    const char *varName = variable.m_Name.c_str();

    bool *isValue;
    T *min;
    T *max;
    T *value;
    size_t *blockID;
    size_t *shapeSize;
    size_t *startSize;
    size_t *countSize;
    size_t *memoryStartSize;
    size_t *memoryCountSize;
    size_t *stepsStart;
    size_t *stepsCount;

    Dims shape;
    Dims start;
    Dims count;
    Dims memoryStart;
    Dims memoryCount;
    ShapeID *shapeID;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    selectorShort = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selectorShort, "_id", J_DB_SELECTOR_OPERATOR_EQ,
                            &entryID, sizeof(entryID), NULL);

    iterator = j_db_iterator_new(schema, selectorShort, NULL);
    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "shapeSize", &type,
                                (gpointer *)&shapeSize, &db_length, NULL);
        if (*shapeSize > 0)
        {
            size_t *tmpShapeBuffer;
            j_db_iterator_get_field(iterator, "shape", &type,
                                    (gpointer *)&tmpShapeBuffer, &db_length,
                                    NULL);
            Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + *shapeSize);
            blockInfo.Shape = tmpShape;
            g_free(tmpShapeBuffer);
        }

        j_db_iterator_get_field(iterator, "startSize", &type,
                                (gpointer *)&startSize, &db_length, NULL);
        if (*startSize > 0)
        {
            size_t *tmpStartBuffer;
            j_db_iterator_get_field(iterator, "start", &type,
                                    (gpointer *)&tmpStartBuffer, &db_length,
                                    NULL);
            Dims tmpStart(tmpStartBuffer, tmpStartBuffer + *startSize);
            blockInfo.Start = tmpStart;
            g_free(tmpStartBuffer);
        }
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t *tmpCountBuffer;
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);
            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
            blockInfo.Count = tmpCount;
            g_free(tmpCountBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryStartSize", &type,
                                (gpointer *)&memoryStartSize, &db_length, NULL);
        if (*memoryStartSize > 0)
        {
            size_t *tmpMemoryStartBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryStartBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryStart(tmpMemoryStartBuffer,
                                tmpMemoryStartBuffer + *memoryStartSize);
            blockInfo.MemoryStart = tmpMemoryStart;
            g_free(tmpMemoryStartBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryCountSize", &type,
                                (gpointer *)&memoryCountSize, &db_length, NULL);
        if (*memoryCountSize > 0)
        {
            size_t *tmpMemoryCountBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryCountBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryCount(tmpMemoryCountBuffer,
                                tmpMemoryCountBuffer + *memoryCountSize);
            blockInfo.MemoryCount = tmpMemoryCount;
            g_free(tmpMemoryCountBuffer);
        }

        // std::string variableType = variable.m_Type;
        // const char *varType = variableType.c_str();
        // const int varType = static_cast<int>(variable.m_Type);
        std::string minField;
        std::string maxField;
        std::string valueField;
        std::string meanField;

        JuleaInteraction::SetMinMaxValueFields(&minField, &maxField, &valueField, &meanField,
                             variable.m_Type);

        j_db_iterator_get_field(iterator, minField.c_str(), &type,
                                (gpointer *)&min, &db_length, NULL);
        blockInfo.Min = *min;
        j_db_iterator_get_field(iterator, maxField.c_str(), &type,
                                (gpointer *)&max, &db_length, NULL);
        blockInfo.Max = *max;
        j_db_iterator_get_field(iterator, "isValue", &type,
                                (gpointer *)&isValue, &db_length, NULL);

        blockInfo.IsValue = *isValue;
        if (isValue)
        {
            // std::cout << "Get Value from DB" << std::endl;
            j_db_iterator_get_field(iterator, valueField.c_str(), &type,
                                    (gpointer *)&value, &db_length, NULL);
            blockInfo.Value = *value;
        }
        j_db_iterator_get_field(iterator, "stepsStart", &type,
                                (gpointer *)&stepsStart, &db_length, NULL);
        blockInfo.StepsStart = *stepsStart;
        j_db_iterator_get_field(iterator, "stepsCount", &type,
                                (gpointer *)&stepsCount, &db_length, NULL);
        blockInfo.StepsCount = *stepsCount;
        j_db_iterator_get_field(iterator, "blockID", &type,
                                (gpointer *)&blockID, &db_length, NULL);
        blockInfo.BlockID = *blockID;

        if (false)
        {
            // std::cout << "shapeSize: " << *shapeSize << std::endl;
            // std::cout << "startSize: " << *startSize << std::endl;
            // std::cout << "countSize: " << *countSize << std::endl;
            // std::cout << "memoryStartSize: " << *memoryStartSize <<
            // std::endl; std::cout << "memoryCountSize: " << *memoryCountSize
            // << std::endl; std::cout << "info->Min: " << info->Min <<
            // std::endl; std::cout << "info->Max: " << info->Max << std::endl;
            // std::cout << "info->Value: " << info->Value << std::endl;
            // std::cout << "info->StepsStart: " << info->StepsStart <<
            // std::endl; std::cout << "info->StepsCount: " << info->StepsCount
            // << std::endl; std::cout << "info->BlockID: " << info->BlockID <<
            // std::endl; std::cout << "info->IsValue: " << info->IsValue <<
            // std::endl;
        }
        if (isValue)
        {
            g_free(value);
        }
        g_free(isValue);
        g_free(min);
        g_free(max);
        g_free(shapeSize);
        g_free(startSize);
        g_free(countSize);
        g_free(memoryStartSize);
        g_free(memoryCountSize);
        g_free(stepsStart);
        g_free(stepsCount);
        g_free(blockID);
        j_batch_unref(batch);
        j_semantics_unref(semantics);
    }
}

// TODO: remove step, block from parameter list
template <class T>
std::unique_ptr<typename core::Variable<T>::Info>
JuleaDBInteractionReader::GetBlockMetadata(const core::Variable<T> &variable,
                   // const std::string nameSpace, size_t step, size_t block,
                   size_t entryID) const
{
    // std::cout << "--- DBGetBlockMetadata ---" << std::endl;
    std::unique_ptr<typename core::Variable<T>::Info> info(
        new (typename core::Variable<T>::Info));
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    // g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    // g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBSelector) selectorShort = NULL;

    const char *varName = variable.m_Name.c_str();

    bool *isValue;
    T *min;
    T *max;
    T *value;
    size_t *blockID;
    size_t *shapeSize;
    size_t *startSize;
    size_t *countSize;
    size_t *memoryStartSize;
    size_t *memoryCountSize;
    size_t *stepsStart;
    size_t *stepsCount;

    Dims shape;
    Dims start;
    Dims count;
    Dims memoryStart;
    Dims memoryCount;
    ShapeID *shapeID;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    // selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    // j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
    //                         nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
    //                         NULL);
    // j_db_selector_add_field(selector, "variableName",
    // J_DB_SELECTOR_OPERATOR_EQ,
    //                         varName, strlen(varName) + 1, NULL);
    // j_db_selector_add_field(selector, "step", J_DB_SELECTOR_OPERATOR_EQ,
    // &step,
    //                         sizeof(step), NULL);
    // j_db_selector_add_field(selector, "block", J_DB_SELECTOR_OPERATOR_EQ,
    //                         &block, sizeof(block), NULL);
    // iterator = j_db_iterator_new(schema, selector, NULL);

    selectorShort = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selectorShort, "_id", J_DB_SELECTOR_OPERATOR_EQ,
                            &entryID, sizeof(entryID), NULL);

    iterator = j_db_iterator_new(schema, selectorShort, NULL);
    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "shapeSize", &type,
                                (gpointer *)&shapeSize, &db_length, NULL);
        if (*shapeSize > 0)
        {
            size_t *tmpShapeBuffer;
            j_db_iterator_get_field(iterator, "shape", &type,
                                    (gpointer *)&tmpShapeBuffer, &db_length,
                                    NULL);
            Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + *shapeSize);
            info->Shape = tmpShape;
            g_free(tmpShapeBuffer);
        }

        j_db_iterator_get_field(iterator, "startSize", &type,
                                (gpointer *)&startSize, &db_length, NULL);
        if (*startSize > 0)
        {
            size_t *tmpStartBuffer;
            j_db_iterator_get_field(iterator, "start", &type,
                                    (gpointer *)&tmpStartBuffer, &db_length,
                                    NULL);
            Dims tmpStart(tmpStartBuffer, tmpStartBuffer + *startSize);
            info->Start = tmpStart;
            g_free(tmpStartBuffer);
        }
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t *tmpCountBuffer;
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);
            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
            info->Count = tmpCount;
            g_free(tmpCountBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryStartSize", &type,
                                (gpointer *)&memoryStartSize, &db_length, NULL);
        if (*memoryStartSize > 0)
        {
            size_t *tmpMemoryStartBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryStartBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryStart(tmpMemoryStartBuffer,
                                tmpMemoryStartBuffer + *memoryStartSize);
            info->MemoryStart = tmpMemoryStart;
            g_free(tmpMemoryStartBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryCountSize", &type,
                                (gpointer *)&memoryCountSize, &db_length, NULL);
        if (*memoryCountSize > 0)
        {
            size_t *tmpMemoryCountBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryCountBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryCount(tmpMemoryCountBuffer,
                                tmpMemoryCountBuffer + *memoryCountSize);
            info->MemoryCount = tmpMemoryCount;
            g_free(tmpMemoryCountBuffer);
        }

        // std::string variableType = variable.m_Type;
        // const char *varType = variable.m_Type.c_str();
        // const char *varType = "hello_world";
        // const int varType = static_cast<int>(variable.m_Type);
        std::string minField;
        std::string maxField;
        std::string valueField;
        std::string meanField;

        // if ((strcmp(varType, "char") == 0) ||
        //     (strcmp(varType, "int8_t") == 0) ||
        //     (strcmp(varType, "uint8_t") == 0) ||
        //     (strcmp(varType, "int16_t") == 0) ||
        //     (strcmp(varType, "uint16_t") == 0) ||
        //     (strcmp(varType, "int32_t") == 0))
        // {
        //     minField = "min_sint32";
        //     maxField = "max_sint32";
        //     valueField = "value_sint32";
        // }
        // else if (strcmp(varType, "uint32_t") == 0)
        // {
        //     minField = "min_uint32";
        //     maxField = "max_uint32";
        //     valueField = "value_uint32";
        // }
        // else if (strcmp(varType, "int64_t") == 0)
        // {
        //     minField = "min_sint64";
        //     maxField = "max_sint64";
        //     valueField = "value_sint64";
        // }
        // else if (strcmp(varType, "uint64_t") == 0)
        // {
        //     minField = "min_uint64";
        //     maxField = "max_uint64";
        //     valueField = "value_uint64";
        // }
        // else if (strcmp(varType, "float") == 0)
        // {
        //     minField = "min_float32";
        //     maxField = "max_float32";
        //     valueField = "value_float32";
        // }
        // else if (strcmp(varType, "double") == 0)
        // {
        //     minField = "min_float64";
        //     maxField = "max_float64";
        //     valueField = "value_float64";
        // }
        // else if (strcmp(varType, "string") == 0)
        // {
        //     valueField = "value_sint32";
        // }

        // else if ((strcmp(varType, "long double") == 0) ||
        //          (strcmp(varType, "float complex") == 0) ||
        //          (strcmp(varType, "double complex") == 0))
        // {
        //     minField = "min_blob";
        //     maxField = "max_blob";
        //     valueField = "value_blob";
        // }
        JuleaInteraction::SetMinMaxValueFields(&minField, &maxField, &valueField, &meanField,
                             variable.m_Type);
        // JuleaInteraction::SetMinMaxValueFields(&minField, &maxField, &valueField, &meanField,
        // varType); std::cout << "minField: " << minField << std::endl;
        // std::cout << "maxField: " << maxField << std::endl;
        // std::cout << "valueField: " << valueField << std::endl;
        // std::cout << "varType: " << varType << std::endl;

        j_db_iterator_get_field(iterator, minField.c_str(), &type,
                                (gpointer *)&min, &db_length, NULL);
        info->Min = *min;
        j_db_iterator_get_field(iterator, maxField.c_str(), &type,
                                (gpointer *)&max, &db_length, NULL);
        info->Max = *max;
        j_db_iterator_get_field(iterator, "isValue", &type,
                                (gpointer *)&isValue, &db_length, NULL);

        info->IsValue = *isValue;
        if (isValue)
        {
            // std::cout << "Get Value from DB" << std::endl;
            j_db_iterator_get_field(iterator, valueField.c_str(), &type,
                                    (gpointer *)&value, &db_length, NULL);
            info->Value = *value;
        }
        j_db_iterator_get_field(iterator, "stepsStart", &type,
                                (gpointer *)&stepsStart, &db_length, NULL);
        info->StepsStart = *stepsStart;
        j_db_iterator_get_field(iterator, "stepsCount", &type,
                                (gpointer *)&stepsCount, &db_length, NULL);
        info->StepsCount = *stepsCount;
        j_db_iterator_get_field(iterator, "blockID", &type,
                                (gpointer *)&blockID, &db_length, NULL);
        info->BlockID = *blockID;

        if (false)
        {
            std::cout << "shapeSize: " << *shapeSize << std::endl;
            std::cout << "startSize: " << *startSize << std::endl;
            std::cout << "countSize: " << *countSize << std::endl;
            std::cout << "memoryStartSize: " << *memoryStartSize << std::endl;
            std::cout << "memoryCountSize: " << *memoryCountSize << std::endl;
            std::cout << "info->Min: " << info->Min << std::endl;
            std::cout << "info->Max: " << info->Max << std::endl;
            std::cout << "info->Value: " << info->Value << std::endl;
            std::cout << "info->StepsStart: " << info->StepsStart << std::endl;
            std::cout << "info->StepsCount: " << info->StepsCount << std::endl;
            std::cout << "info->BlockID: " << info->BlockID << std::endl;
            std::cout << "info->IsValue: " << info->IsValue << std::endl;
        }
        if (isValue)
        {
            g_free(value);
        }
        g_free(isValue);
        g_free(min);
        g_free(max);
        g_free(shapeSize);
        g_free(startSize);
        g_free(countSize);
        g_free(memoryStartSize);
        g_free(memoryCountSize);
        g_free(stepsStart);
        g_free(stepsCount);
        g_free(blockID);
        j_batch_unref(batch);
        j_semantics_unref(semantics);
    }
    return info;
}


} // end namespace interop
} // end namespace adios2


#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_TCC_ */
