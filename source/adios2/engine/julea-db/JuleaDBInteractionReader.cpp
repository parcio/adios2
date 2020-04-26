/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaDBInteractionReader.h"

// #include "JuleaFormatReader.h" //for ParseVariableFromBSON
#include "JuleaDBReader.h"

#include <bson.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include <julea-db.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{
void DBInitVariable(core::IO *io, core::Engine &engine, std::string varName,
                  size_t *blocks, size_t numberSteps, ShapeID shapeID)
{
    // std::cout << "----- InitVariable ---" << std::endl;
    const std::string type(io->InquireVariableType(varName));

    /** no sensible information yet to store in AvailableStepBlockIndexOffsets.
     * So it is filled with dummy values. (42 + BlockID) */
    if (type == "compound")
    {
    }
#define declare_type(T)                                                        \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        auto var = io->InquireVariable<T>(varName);                            \
        var->m_ShapeID = shapeID;                                              \
        for (uint i = 0; i < numberSteps; i++)                                 \
        {                                                                      \
            std::cout << "i: " << i << std::endl;                              \
            for (uint j = 0; j < blocks[i]; j++)                               \
            {                                                                  \
                var->m_AvailableStepBlockIndexOffsets[i + 1].push_back(42 +    \
                                                                       i);     \
                std::cout << "AvailableStepBlockIndexOffsets.size"             \
                          << var->m_AvailableStepBlockIndexOffsets.size()      \
                          << std::endl;                                        \
            }                                                                  \
            var->m_AvailableStepsCount++;                                      \
        }                                                                      \
        var->m_AvailableStepsStart = 0;                                        \
        var->m_StepsStart = 0;                                                 \
        var->m_Engine = &engine;                                               \
        var->m_FirstStreamingStep = true;                                      \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    // var.m_IsFirstStreamingStep = true; //TODO: necessary?
    // for(int i = 0; i < 2; i++ )
    // var->m_ShapeID = shapeID;                                           \
    // {
    //     for (int j = 0; j < 1; j++)
    //     {
    //         std::cout << "i: " << i << "j: " << j << std::endl;
    //     }
    // }
}


void DBDefineVariableInInit(core::IO *io, const std::string varName,
                            std::string stringType, Dims shape, Dims start,
                            Dims count, bool constantDims)
{
    const char *type = stringType.c_str();
    std::cout << "------ DefineVariableInInit ----------" << std::endl;
    std::cout << "------ type  ---------- " << type << std::endl;

    if (strcmp(type, "unknown") == 0)
    {
        // TODO
    }
    else if (strcmp(type, "compound") == 0)
    {
    }
    else if (strcmp(type, "string") == 0)
    {
        auto &var = io->DefineVariable<std::string>(varName, shape, start,
                                                    count, constantDims);
        std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "int8_t") == 0)
    {
        auto &var = io->DefineVariable<int8_t>(varName, shape, start, count,
                                               constantDims);
        std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "uint8_t") == 0)
    {
        auto &var = io->DefineVariable<uint8_t>(varName, shape, start, count,
                                                constantDims);
    }
    else if (strcmp(type, "int16_t") == 0)
    {
        auto &var = io->DefineVariable<int16_t>(varName, shape, start, count,
                                                constantDims);
    }
    else if (strcmp(type, "uint16_t") == 0)
    {
        auto &var = io->DefineVariable<uint16_t>(varName, shape, start, count,
                                                 constantDims);
    }
    else if (strcmp(type, "int32_t") == 0)
    {
        auto &var = io->DefineVariable<int32_t>(varName, shape, start, count,
                                                constantDims);
    }
    else if (strcmp(type, "uint32_t") == 0)
    {
        auto &var = io->DefineVariable<uint32_t>(varName, shape, start, count,
                                                 constantDims);
    }
    else if (strcmp(type, "int64_t") == 0)
    {
        auto &var = io->DefineVariable<int64_t>(varName, shape, start, count,
                                                constantDims);
    }
    else if (strcmp(type, "uint64_t") == 0)
    {
        auto &var = io->DefineVariable<uint64_t>(varName, shape, start, count,
                                                 constantDims);
    }
    else if (strcmp(type, "float") == 0)
    {
        auto &var = io->DefineVariable<float>(varName, shape, start, count,
                                              constantDims);
        std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "double") == 0)
    {
        auto &var = io->DefineVariable<double>(varName, shape, start, count,
                                               constantDims);
        std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "long double") == 0)
    {
        auto &var = io->DefineVariable<long double>(varName, shape, start,
                                                    count, constantDims);
        std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex float") == 0)
    {
        auto &var = io->DefineVariable<std::complex<float>>(
            varName, shape, start, count, constantDims);
        std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex double") == 0)
    {
        auto &var = io->DefineVariable<std::complex<double>>(
            varName, shape, start, count, constantDims);
        std::cout << "Defined variable of type: " << type << std::endl;
    }

    std::map<std::string, Params> varMap = io->GetAvailableVariables();

    for (std::map<std::string, Params>::iterator it = varMap.begin();
         it != varMap.end(); ++it)
    {

        // std::cout << "first: " << it->first << " => " << it->second.begin()
        // << '\n';
        std::cout << "first: " << it->first << '\n';
    }
}

void CheckSchemas()
{
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);
    g_autoptr(JDBSchema) varSchema = NULL;
    g_autoptr(JDBSchema) blockSchema = NULL;

    varSchema = j_db_schema_new("adios2", "variable-metadata", NULL);
    blockSchema = j_db_schema_new("adios2", "block-metadata", NULL);

    j_db_schema_get(varSchema, batch, NULL);
    bool existsVar = j_batch_execute(batch);
    j_db_schema_get(blockSchema, batch, NULL);

    bool existsBlock = j_batch_execute(batch);
    std::cout << "existsVar: " << existsVar << " existsBlock: " << existsBlock
              << std::endl;
    if ((existsVar == 0) || (existsBlock == 0))
    {
        std::cout << "ERROR: database adios2 schemas do not exist" << std::endl;
    }
}

void InitVariablesFromDB(const std::string nameSpace, core::IO *io, core::Engine &engine)
{
    std::cout << "--- InitVariablesFromDB ---" << std::endl;
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    // JDBType type2;
    // guint64 db_length2 = 0;

    // std::string varName;
    char *varName;
    Dims shape;
    Dims start;
    Dims count;
    ShapeID *shapeID;

    bool *isConstantDims;
    // uint32_t *constantDims;
    bool *isReadAsJoined;
    bool *isReadAsLocalValue;
    bool *isRandomAccess;

    std::string varType;
    char *varTypePtr;
    size_t typeLen;
    size_t *blocks;
    size_t *shapeSize;
    size_t *startSize;
    size_t *countSize;
    size_t *numberSteps;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "variable-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
                            nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
                            NULL);
    // j_db_selector_add_field(selector, "variableName",
    //                         J_DB_SELECTOR_OPERATOR_EQ, varName.c_str(),
    //                         strlen(varName.c_str()) + 1, NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);

    while (j_db_iterator_next(iterator, NULL))
    {
        std::cout << "---- while --- " << std::endl;

        j_db_iterator_get_field(iterator, "variableName", &type,
                                (gpointer *)&varName, &db_length, NULL);

        // db_length = 0;
        // j_db_iterator_get_field(iterator, "isConstantDims", &type2, (gpointer
        // *)&constantDims, &db_length2, NULL);
        j_db_iterator_get_field(iterator, "isConstantDims", &type,
                                (gpointer *)&isConstantDims, &db_length, NULL);
        j_db_iterator_get_field(iterator, "isReadAsJoined", &type,
                                (gpointer *)&isReadAsJoined, &db_length, NULL);
        j_db_iterator_get_field(iterator, "isReadAsLocalValue", &type,
                                (gpointer *)&isReadAsLocalValue, &db_length,
                                NULL);
        j_db_iterator_get_field(iterator, "isRandomAccess", &type,
                                (gpointer *)&isRandomAccess, &db_length, NULL);

        j_db_iterator_get_field(iterator, "shapeID", &type,
                                (gpointer *)&shapeID, &db_length, NULL);
        j_db_iterator_get_field(iterator, "type", &type,
                                (gpointer *)&varTypePtr, &db_length, NULL);
        std::string varType(varTypePtr);
        // //TODO needed?
        // j_db_iterator_get_field(iterator, "typeLen", &type, (gpointer
        // *)&typeLen, &db_length, NULL);

        j_db_iterator_get_field(iterator, "shapeSize", &type,
                                (gpointer *)&shapeSize, &db_length, NULL);
        if (*shapeSize > 0)
        {
            size_t tmpShapeBuffer[*shapeSize];
            j_db_iterator_get_field(iterator, "shape", &type,
                                    (gpointer *)&tmpShapeBuffer, &db_length,
                                    NULL);
            Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + *shapeSize);
            shape = tmpShape;
        }

        j_db_iterator_get_field(iterator, "startSize", &type,
                                (gpointer *)&startSize, &db_length, NULL);
        if (*startSize > 0)
        {
            size_t tmpStartBuffer[*startSize];
            j_db_iterator_get_field(iterator, "start", &type,
                                    (gpointer *)&tmpStartBuffer, &db_length,
                                    NULL);
            Dims tmpStart(tmpStartBuffer, tmpStartBuffer + *startSize);
            start = tmpStart;
        }
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t tmpCountBuffer[*countSize];
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);
            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *shapeSize);
            count = tmpCount;
        }
        j_db_iterator_get_field(iterator, "numberSteps", &type,
                                (gpointer *)&numberSteps, &db_length, NULL);
        if (*numberSteps > 0)
        {
            size_t *tmpblocks[*numberSteps];
            j_db_iterator_get_field(iterator, "blockArray", &type,
                                    (gpointer *)tmpblocks, &db_length, NULL);
            blocks = *tmpblocks;
            std::cout << "numberSteps: " << blocks[0] << std::endl;
            std::cout << "numberSteps: " << blocks[1] << std::endl;
        }

        std::cout << "varName = " << varName << std::endl;
        std::cout << "length: " << db_length << std::endl;
        std::cout << "constantDims: " << *isConstantDims << std::endl;
        std::cout << "isReadAsJoined: " << *isReadAsJoined << std::endl;
        std::cout << "isReadAsLocalValue: " << *isReadAsLocalValue << std::endl;
        std::cout << "isRandomAccess: " << *isRandomAccess << std::endl;
        std::cout << "shapeID: " << *shapeID << std::endl;
        std::cout << "varType2: " << varTypePtr << std::endl;
        std::cout << "shapeSize: " << *shapeSize << std::endl;
        std::cout << "startSize: " << *startSize << std::endl;
        std::cout << "startSize: " << *countSize << std::endl;
        std::cout << "numberSteps: " << *numberSteps << std::endl;

        // j_db_iterator_get_field(iterator, "blockArray", &type, (gpointer
        // *)&varName, &db_length, NULL); printf("DB contains: %s (%"
        // G_GUINT64_FORMAT " bytes)\n", db_field, db_length);
        DBDefineVariableInInit(io, varName, varType, shape, start, count,
                               isConstantDims);

        DBInitVariable(io, engine, varName, blocks, *numberSteps, *shapeID);
        // DBInitVariable(&m_IO, *this, varName, blocks, numberSteps, shapeID);
    }
            // free blocks;
}

void DBGetVariableMetadataFromJulea(const std::string nameSpace,
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
    g_assert_true(j_batch_execute(batch) == true);
}

void DBGetBlockMetadataFromJulea(const std::string nameSpace,
                                 const std::string varName, gpointer *md,
                                 guint32 *buffer_len,
                                 const std::string stepBlockID)
{
    // std::cout << "-- GetBlockMetadataFromJulea -----" << std::endl;
    void *metaDataBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV = g_strdup_printf("%s_%s_%s", nameSpace.c_str(),
                                            varName.c_str(), "variableblocks");
    // std::cout << "stringMetadataKV " << stringMetadataKV << std::endl;

    auto kvBlockMetadata = j_kv_new(stringMetadataKV, stepBlockID.c_str());

    j_kv_get(kvBlockMetadata, md, buffer_len, batch);
    g_assert_true(j_batch_execute(batch) == true);

    g_free(stringMetadataKV);
    j_kv_unref(kvBlockMetadata);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void DBGetVariableDataFromJulea(Variable<T> &variable, T *data,
                                const std::string nameSpace, size_t dataSize,
                                size_t step, size_t block)
{
    // std::cout << "-- GetVariableDataFromJulea ----- " << std::endl;

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
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesRead == dataSize)
    {
        // std::cout << "++ Julea Interaction Reader: Read data for variable "
        // << varName << std::endl;
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

/** Retrieves all variable names from key-value store. They are all stored in
 * one bson. */
void DBGetNamesFromJulea(const std::string nameSpace, bson_t **bsonNames,
                         unsigned int *varCount, bool isVariable)
{
    std::cout << "-- GetNamesFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    int err = 0;
    void *namesBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    std::string kvName;

    if (isVariable)
    {
        kvName = "variable_names";
    }
    else
    {
        kvName = "attribute_names";
    }

    auto kvObject = j_kv_new(kvName.c_str(), nameSpace.c_str());
    std::cout << "kvName :" << kvName << std::endl;

    j_kv_get(kvObject, &namesBuf, &valueLen, batch);
    err = j_batch_execute(batch);

    if (err != 0)
    {
        std::cout << "j_batch_execute failed in GetNamesFromJulea. "
                  << std::endl;
    }

    if (valueLen == 0)
    {
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
    }

    *varCount = bson_count_keys(*bsonNames);

    j_semantics_unref(semantics);
    j_kv_unref(kvObject);
    j_batch_unref(batch);
    free(namesBuf);
}

#define variable_template_instantiation(T)                                     \
    template void DBGetVariableDataFromJulea(                                  \
        Variable<T> &variable, T *data, const std::string nameSpace,           \
        long unsigned int dataSize, size_t step, size_t block);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

/** ------------------------- Attributes -----------------------------------**/

void DBGetAttributeBSONFromJulea(const std::string nameSpace,
                                 const std::string attrName,
                                 bson_t **bsonMetadata, guint32 *valueLen)
{
    // guint32 valueLen = 0;
    void *metaDataBuf = NULL;
    int err = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    std::cout << "-- GetAttributeBSONFromJulea ---- " << std::endl;

    auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace.c_str());
    auto kvObject = j_kv_new(stringMetadataKV, attrName.c_str());

    j_kv_get(kvObject, &metaDataBuf, valueLen, batch);
    err = j_batch_execute(batch);
    if (err != 0)
    {
        std::cout << "j_batch_execute failed in GetAttributeBSONFromJulea"
                  << std::endl;
    }

    if (valueLen == 0)
    {
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

void DBGetAttributeMetadataFromJulea(const std::string attrName,
                                     const std::string nameSpace,
                                     long unsigned int *dataSize,
                                     size_t *numberElements,
                                     bool *IsSingleValue, int *type)
{
    bson_t *bsonMetadata;
    std::cout << "-- GetAttributeMetadataFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    DBGetAttributeBSONFromJulea(nameSpace, attrName, &bsonMetadata, &valueLen);

    if (valueLen > 0)
    {
        // ParseAttributeFromBSON(nameSpace, attrName, bsonMetadata, dataSize,
        // numberElements, IsSingleValue, type);
    }
    bson_destroy(bsonMetadata);
}

void DBGetAttributeMetadataFromJulea(const std::string attrName,
                                     const std::string nameSpace,
                                     long unsigned int *completeSize,
                                     size_t *numberElements,
                                     bool *IsSingleValue, int *type,
                                     unsigned long **dataSizes)
{
    bson_t *bsonMetadata;
    std::cout << "-- GetAttributeMetadataFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    DBGetAttributeBSONFromJulea(nameSpace, attrName, &bsonMetadata, &valueLen);

    if (valueLen > 0)
    {
        // ParseAttributeFromBSON(nameSpace, attrName, bsonMetadata,
        // completeSize, numberElements, IsSingleValue, type, dataSizes);
    }
    bson_destroy(bsonMetadata);
}

template <class T>
void DBGetAttributeDataFromJulea(const std::string attrName, T *data,
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

void DBGetAttributeStringDataFromJulea(const std::string attrName, char *data,
                                       const std::string nameSpace,
                                       long unsigned int completeSize,
                                       bool IsSingleValue,
                                       size_t numberElements)
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
    template void DBGetAttributeDataFromJulea(                                 \
        const std::string attrName, T *data, const std::string nameSpace,      \
        long unsigned int dataSize);
ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
