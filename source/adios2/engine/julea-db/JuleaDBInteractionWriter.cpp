/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaDBInteractionWriter.h"
#include "JuleaDBInteractionReader.h"
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
namespace core
{
namespace engine
{

void addFieldsForVariableMD(JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *minFloatIndex[] = {"variableName", NULL};
    gchar const *maxFloatIndex[] = {"variableName", NULL};
    gchar const *minDoubleIndex[] = {"variableName", NULL};
    gchar const *maxDoubleIndex[] = {"variableName", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "isConstantDims", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isReadAsJoined", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isReadAsLocalValue", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isRandomAccess", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isSingleValue", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "shapeID", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "type", J_DB_TYPE_STRING, NULL);
    // TODO: needed?
    // j_db_schema_add_field(schema, "typeLen", J_DB_TYPE_UINT64, NULL);

    /** all vectors need to store their size */
    j_db_schema_add_field(schema, "shapeSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "shape", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "startSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "start", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "countSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "count", J_DB_TYPE_BLOB, NULL);

    /** number of blocks (steps are index starting at 0) */
    j_db_schema_add_field(schema, "numberSteps", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "blockArray", J_DB_TYPE_BLOB, NULL);

    j_db_schema_add_field(schema, "min_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "max_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "value_blob", J_DB_TYPE_BLOB, NULL);

    // add min/max/value for every type for performance improvement of querying
    j_db_schema_add_field(schema, "min_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "max_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "value_sint32", J_DB_TYPE_SINT32, NULL);

    j_db_schema_add_field(schema, "min_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "max_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "value_uint32", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "min_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "max_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "value_sint64", J_DB_TYPE_SINT64, NULL);

    j_db_schema_add_field(schema, "min_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "max_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "value_uint64", J_DB_TYPE_UINT64, NULL);

    j_db_schema_add_field(schema, "min_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "max_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "value_float32", J_DB_TYPE_FLOAT32, NULL);

    j_db_schema_add_field(schema, "min_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "max_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "value_float64", J_DB_TYPE_FLOAT64, NULL);

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING, NULL);
    // j_db_schema_add_field(schema, "max_string", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, minFloatIndex, NULL);
    j_db_schema_add_index(schema, maxFloatIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
}

void addFieldsForBlockMD(JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *stepIndex[] = {"step", NULL};
    gchar const *blockIndex[] = {"block", NULL};
    gchar const *minFloatIndex[] = {"variableName", NULL};
    gchar const *maxFloatIndex[] = {"variableName", NULL};
    gchar const *minDoubleIndex[] = {"variableName", NULL};
    gchar const *maxDoubleIndex[] = {"variableName", NULL};
    //    gchar const *minIndex[] = {"min_blob", NULL};
    //    gchar const *maxIndex[] = {"max_blob", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "step", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "block", J_DB_TYPE_UINT64, NULL);

    /** all vectors need to store their size */
    j_db_schema_add_field(schema, "shapeSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "shape", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "startSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "start", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "countSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "count", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "memoryStartSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "memoryStart", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "memoryCountSize", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "memoryCount", J_DB_TYPE_BLOB, NULL);

    j_db_schema_add_field(schema, "isValue", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "min_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "max_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "value_blob", J_DB_TYPE_BLOB, NULL);

    // add min/max/value for every type for performance improvement of querying
    j_db_schema_add_field(schema, "min_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "max_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "value_sint32", J_DB_TYPE_SINT32, NULL);

    j_db_schema_add_field(schema, "min_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "max_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "value_uint32", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "min_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "max_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "value_sint64", J_DB_TYPE_SINT64, NULL);

    j_db_schema_add_field(schema, "min_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "max_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "value_uint64", J_DB_TYPE_UINT64, NULL);

    j_db_schema_add_field(schema, "min_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "max_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "value_float32", J_DB_TYPE_FLOAT32, NULL);

    j_db_schema_add_field(schema, "min_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "max_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "value_float64", J_DB_TYPE_FLOAT64, NULL);

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING, NULL);
    // j_db_schema_add_field(schema, "max_string", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "stepsStart", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "stepsCount", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "blockID", J_DB_TYPE_UINT64, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, stepIndex, NULL);
    j_db_schema_add_index(schema, blockIndex, NULL);
    j_db_schema_add_index(schema, minFloatIndex, NULL);
    j_db_schema_add_index(schema, maxFloatIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
    //    j_db_schema_add_index(schema, minIndex, NULL);
    //    j_db_schema_add_index(schema, maxIndex, NULL);
}

template <class T>
void addEntriesForVariableMD(Variable<T> &variable, const std::string nameSpace,
                             const std::string varName, size_t currStep,
                             JDBSchema *schema, JDBEntry *entry)
{
    bool isConstantDims = variable.IsConstantDims();
    int tmp = isConstantDims ? 1 : 0;
    bool isReadAsJoined = variable.m_ReadAsJoined;
    int tmp2 = isReadAsJoined ? 1 : 0;
    bool isReadAsLocalValue = variable.m_ReadAsLocalValue;
    int tmp3 = isReadAsLocalValue ? 1 : 0;
    bool isRandomAccess = variable.m_RandomAccess;
    int tmp4 = isRandomAccess ? 1 : 0;
    bool isSingleValue = variable.m_SingleValue;
    int tmp5 = isSingleValue ? 1 : 0;

    int shapeID = (int)variable.m_ShapeID;

    const char *type = variable.m_Type.c_str();
    size_t shapeSize = variable.m_Shape.size();
    size_t startSize = variable.m_Start.size();
    size_t countSize = variable.m_Count.size();
    size_t numberSteps = currStep + 1;

    size_t shapeIDLen = sizeof(int);
    size_t typeLen = sizeof(variable.m_Type.c_str());

    size_t shapeBuffer[shapeSize];
    for (uint i = 0; i < shapeSize; i++)
    {
        shapeBuffer[i] = variable.m_Shape.data()[i];
    }

    size_t startBuffer[startSize];
    for (uint i = 0; i < startSize; i++)
    {
        startBuffer[i] = variable.m_Start.data()[i];
    }

    size_t countBuffer[countSize];
    // std::cout << "countSize: " << countSize << std::endl;
    for (uint i = 0; i < countSize; i++)
    {
        countBuffer[i] = variable.m_Count.data()[i];
        // std::cout << "countBuffer[i]:" << countBuffer[i] << std::endl;
        // std::cout << "sizeof(countBuffer): " << sizeof(countBuffer)
        // << std::endl;
    }

    size_t blocks[numberSteps];

    // if the entry does not exist in the map it will be added. This is actually
    // what we want here. This way, for all steps prior to the first step of
    // this variable, an element will be created of size 0
    for (uint i = 0; i < numberSteps; i++)
    {
        std::cout << "mapsize: "
                  << variable.m_AvailableStepBlockIndexOffsets.size()
                  << std::endl;
        blocks[i] = variable.m_AvailableStepBlockIndexOffsets[i].size();
        std::cout << "mapsize: "
                  << variable.m_AvailableStepBlockIndexOffsets.size()
                  << std::endl;
        std::cout << "i: " << i << "  blocks: " << blocks[i] << std::endl;
    }
    if (false)
    {
        std::cout << "typeLen: " << typeLen << std::endl;
        std::cout << "variable.m_ShapeID: " << variable.m_ShapeID << std::endl;
        std::cout << "shapeID: " << shapeID << std::endl;
        std::cout << "constantDims: " << isConstantDims << std::endl;
        std::cout << "isReadAsJoined: " << isReadAsJoined << std::endl;
        std::cout << "isReadAsLocalValue: " << isReadAsLocalValue << std::endl;
        std::cout << "isRandomAccess: " << isRandomAccess << std::endl;
        std::cout << "type: " << type << std::endl;
        std::cout << "shapeSize: " << shapeSize << std::endl;
        std::cout << "startSize: " << startSize << std::endl;
        std::cout << "countSize: " << countSize << std::endl;
        std::cout << "numberSteps: " << numberSteps << std::endl;
        std::cout << "shape.data = " << variable.m_Shape.data() << std::endl;
        std::cout << "count.data = " << variable.m_Count.data() << std::endl;
        std::cout << "numberSteps: " << numberSteps << std::endl;
        std::cout << "m_AvailableStepStart: " << variable.m_AvailableStepsStart
                  << std::endl;
        std::cout << "m_AvailableStepsCount: " << variable.m_AvailableStepsCount
                  << std::endl;
        std::cout << "m_StepsStart: " << variable.m_StepsStart << std::endl;
        std::cout << "m_StepsCount: " << variable.m_StepsCount << std::endl;
    }

    j_db_entry_set_field(entry, "file", nameSpace.c_str(),
                         strlen(nameSpace.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "variableName", varName.c_str(),
                         strlen(nameSpace.c_str()) + 1, NULL);

    j_db_entry_set_field(entry, "isConstantDims", &tmp, sizeof(tmp), NULL);
    j_db_entry_set_field(entry, "isReadAsJoined", &tmp2, sizeof(tmp2), NULL);
    j_db_entry_set_field(entry, "isReadAsLocalValue", &tmp3, sizeof(tmp3),
                         NULL);
    j_db_entry_set_field(entry, "isRandomAccess", &tmp4, sizeof(tmp4), NULL);
    j_db_entry_set_field(entry, "isSingleValue", &tmp5, sizeof(tmp5), NULL);

    j_db_entry_set_field(entry, "shapeID", &shapeID, sizeof(shapeID), NULL);
    j_db_entry_set_field(entry, "type", type, strlen(type) + 1, NULL);
    j_db_entry_set_field(entry, "typeLen", &typeLen, typeLen, NULL);

    j_db_entry_set_field(entry, "shapeSize", &shapeSize, sizeof(shapeSize),
                         NULL);
    j_db_entry_set_field(entry, "shape", shapeBuffer, sizeof(shapeBuffer),
                         NULL);
    j_db_entry_set_field(entry, "startSize", &startSize, sizeof(startSize),
                         NULL);
    j_db_entry_set_field(entry, "start", startBuffer, sizeof(startBuffer),
                         NULL);
    j_db_entry_set_field(entry, "countSize", &countSize, sizeof(countSize),
                         NULL);
    j_db_entry_set_field(entry, "count", countBuffer, sizeof(countBuffer),
                         NULL);

    j_db_entry_set_field(entry, "numberSteps", &numberSteps,
                         sizeof(numberSteps), NULL);
    j_db_entry_set_field(entry, "blockArray", blocks, sizeof(blocks), NULL);

    const char *varType = variable.m_Type.c_str();
    std::string minField;
    std::string maxField;
    std::string valueField;

    setMinMaxValueFields(&minField, &maxField, &valueField, varType);

    j_db_entry_set_field(entry, minField.c_str(), &variable.m_Min,
                         sizeof(variable.m_Min), NULL);
    j_db_entry_set_field(entry, maxField.c_str(), &variable.m_Max,
                         sizeof(variable.m_Max), NULL);
}

template <class T>
void addEntriesForBlockMD(Variable<T> &variable, const std::string nameSpace,
                          const std::string varName, size_t currStep,
                          size_t block,
                          const typename Variable<T>::Info &blockInfo,
                          JDBSchema *schema, JDBEntry *entry, T &blockMin,
                          T &blockMax)
{
    size_t shapeSize = variable.m_Shape.size();
    size_t startSize = variable.m_Start.size();
    size_t countSize = variable.m_Count.size();
    size_t memoryStartSize = blockInfo.MemoryStart.size();
    size_t memoryCountSize = blockInfo.MemoryCount.size();

    size_t minLen = sizeof(blockMin);
    size_t maxLen = sizeof(blockMax);
    size_t valueLen = sizeof(variable.m_Value);

    size_t stepsStart = blockInfo.StepsStart;
    size_t stepsCount = blockInfo.StepsCount;
    size_t blockID = block;
    size_t stepID = currStep;

    bool isValue = blockInfo.IsValue;
    int tmp = isValue ? 1 : 0;

    size_t shapeBuffer[shapeSize];
    for (uint i = 0; i < shapeSize; i++)
    {
        shapeBuffer[i] = variable.m_Shape.data()[i];
    }

    size_t startBuffer[startSize];
    for (uint i = 0; i < startSize; i++)
    {
        startBuffer[i] = variable.m_Start.data()[i];
    }

    size_t countBuffer[countSize];
    for (uint i = 0; i < countSize; i++)
    {
        countBuffer[i] = variable.m_Count.data()[i];
    }

    size_t memoryStartBuffer[memoryStartSize];
    for (uint i = 0; i < memoryStartSize; i++)
    {
        memoryStartBuffer[i] = blockInfo.MemoryStart.data()[i];
    }
    size_t memoryCountBuffer[memoryCountSize];
    for (uint i = 0; i < memoryCountSize; i++)
    {
        memoryCountBuffer[i] = blockInfo.MemoryCount.data()[i];
    }

    if (false)
    {
        std::cout << "shapeSize: " << shapeSize << std::endl;
        std::cout << "var: shape.data: " << variable.m_Shape.data()
                  << std::endl;
        std::cout << "blockInfo:shape.data: " << variable.m_Shape.data()
                  << std::endl;
        std::cout << "    variable minimum: " << variable.m_Min << std::endl;
        std::cout << "    variable maximum: " << variable.m_Max << std::endl;
        std::cout << "variable min size: " << minLen << std::endl;
        std::cout << "size of T: " << sizeof(T) << std::endl;
        std::cout << "stepsStart" << stepsStart << std::endl;
        std::cout << "stepsCount" << stepsCount << std::endl;
        std::cout << "blockID: " << blockID << std::endl;
    }

    j_db_entry_set_field(entry, "file", nameSpace.c_str(),
                         strlen(nameSpace.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "variableName", varName.c_str(),
                         strlen(varName.c_str()) + 1, NULL);

    j_db_entry_set_field(entry, "step", &stepID, sizeof(stepID), NULL);
    j_db_entry_set_field(entry, "block", &blockID, sizeof(blockID), NULL);

    j_db_entry_set_field(entry, "shapeSize", &shapeSize, sizeof(shapeSize),
                         NULL);
    j_db_entry_set_field(entry, "shape", shapeBuffer, sizeof(shapeBuffer),
                         NULL);
    j_db_entry_set_field(entry, "startSize", &startSize, sizeof(startSize),
                         NULL);
    j_db_entry_set_field(entry, "start", startBuffer, sizeof(startBuffer),
                         NULL);
    j_db_entry_set_field(entry, "countSize", &countSize, sizeof(countSize),
                         NULL);
    j_db_entry_set_field(entry, "count", countBuffer, sizeof(countBuffer),
                         NULL);
    j_db_entry_set_field(entry, "memoryStartSize", &memoryStartSize,
                         sizeof(memoryStartSize), NULL);
    j_db_entry_set_field(entry, "memoryStart", &memoryStartBuffer,
                         sizeof(memoryStartBuffer), NULL);
    j_db_entry_set_field(entry, "memoryCountSize", &memoryCountSize,
                         sizeof(memoryCountSize), NULL);
    j_db_entry_set_field(entry, "memoryCount", &memoryCountBuffer,
                         sizeof(memoryCountBuffer), NULL);

    const char *varType = variable.m_Type.c_str();
    std::string minField;
    std::string maxField;
    std::string valueField;

    setMinMaxValueFields(&minField, &maxField, &valueField, varType);

    j_db_entry_set_field(entry, minField.c_str(), &blockMin, minLen, NULL);
    j_db_entry_set_field(entry, maxField.c_str(), &blockMax, maxLen, NULL);

    j_db_entry_set_field(entry, "isValue", &tmp, sizeof(tmp), NULL);

    if (isValue)
    {
        std::cout << "Writing local value for " << varName << std::endl;
        j_db_entry_set_field(entry, valueField.c_str(), &variable.m_Value,
                             valueLen, NULL);
    }
    else
    {
        size_t value = 0;
        valueLen = sizeof(value);
        j_db_entry_set_field(entry, "value", &value, valueLen, NULL);
    }

    j_db_entry_set_field(entry, "stepsStart", &stepsStart, sizeof(stepsStart),
                         NULL);
    j_db_entry_set_field(entry, "stepsCount", &stepsCount, sizeof(stepsCount),
                         NULL);
    j_db_entry_set_field(entry, "blockID", &blockID, sizeof(blockID), NULL);
}
void InitDBSchemas()
{
    // std::cout << "--- InitDBSchemas ---" << std::endl;
    int err = 0;
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
    // std::cout << "existsVar: " << existsVar << " existsBlock: " <<
    // existsBlock
    // << std::endl;

    if (existsVar == 0)
    {
        // std::cout << "variable schema does not exist" << std::endl;
        varSchema = j_db_schema_new("adios2", "variable-metadata", NULL);
        addFieldsForVariableMD(varSchema);
        j_db_schema_create(varSchema, batch, NULL);
        g_assert_true(j_batch_execute(batch) == true);
    }

    if (existsBlock == 0)
    {

        // std::cout << "block schema does not exist" << std::endl;
        blockSchema = j_db_schema_new("adios2", "block-metadata", NULL);
        addFieldsForBlockMD(blockSchema);
        j_db_schema_create(blockSchema, batch2, NULL);
        g_assert_true(j_batch_execute(batch2) == true);
    }

    // g_assert_true(j_batch_execute(batch2) == true);
    // j_db_schema_unref(varSchema);
    // j_db_schema_unref(blockSchema);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}

template <class T>
void DBPutVariableMetadataToJulea(Variable<T> &variable,
                                  const std::string nameSpace,
                                  const std::string varName, size_t currStep,
                                  size_t block)
{
    int err = 0;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    g_autoptr(JDBSelector) selector = NULL;

    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "variable-metadata", NULL);

    // TODO: necessary to get schema every time?
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    entry = j_db_entry_new(schema, NULL);
    addEntriesForVariableMD(variable, nameSpace, varName, currStep, schema,
                            entry);

    /** check whether variable needs to be updated or inserted */
    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
                            nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
                            NULL);
    j_db_selector_add_field(selector, "variableName", J_DB_SELECTOR_OPERATOR_EQ,
                            varName.c_str(), strlen(varName.c_str()) + 1, NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);

    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_entry_update(entry, selector, batch2, NULL);
    }
    else
    {
        // std::cout << "Variable metadata does not exist yet." << std::endl;
        j_db_entry_insert(entry, batch2, NULL);
    }

    err = j_batch_execute(batch2);
    // g_assert_true(j_batch_execute(batch2) == true);

    // j_db_entry_unref(entry);
    // j_db_schema_unref(schema);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}

template <class T>
void DBPutBlockMetadataToJulea(Variable<T> &variable,
                               const std::string nameSpace,
                               const std::string varName, size_t step,
                               size_t block,
                               const typename Variable<T>::Info &blockInfo,
                               T &blockMin, T &blockMax)
{
    int err = 0;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBIterator) iterator = NULL;

    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    entry = j_db_entry_new(schema, NULL);
    addEntriesForBlockMD(variable, nameSpace, varName, step, block, blockInfo,
                         schema, entry, blockMin, blockMax);

    /** check whether blcock needs to be updated or inserted */
    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
                            nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
                            NULL);
    j_db_selector_add_field(selector, "variableName", J_DB_SELECTOR_OPERATOR_EQ,
                            varName.c_str(), strlen(varName.c_str()) + 1, NULL);
    j_db_selector_add_field(selector, "step", J_DB_SELECTOR_OPERATOR_EQ, &step,
                            sizeof(step), NULL);
    j_db_selector_add_field(selector, "block", J_DB_SELECTOR_OPERATOR_EQ,
                            &block, sizeof(block), NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);
    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_entry_update(entry, selector, batch2, NULL);
    }
    else
    {
        // std::cout << "Variable metadata does not exist yet." << std::endl;
        j_db_entry_insert(entry, batch2, NULL);
    }

    err = j_batch_execute(batch2);
    // g_assert_true(j_batch_execute(batch2) == true);

    // j_db_entry_unref(entry);
    // j_db_schema_unref(schema);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}

template <class T>
void DBPutVariableDataToJulea(Variable<T> &variable, const T *data,
                              const std::string nameSpace, size_t currStep,
                              size_t block)
{
    // std::cout << "--- PutVariableDataToJulea ----- " << std::endl;
    std::cout << "data: " << data[0] << std::endl;
    std::cout << "data: " << data[1] << std::endl;

    guint64 bytesWritten = 0;
    std::string objName = "variableblocks";

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    auto dataSize = variable.m_ElementSize * numberElements;

    auto stepBlockID = g_strdup_printf("%lu_%lu", currStep, block);
    auto stringDataObject =
        g_strdup_printf("%s_%s_%s", nameSpace.c_str(), variable.m_Name.c_str(),
                        objName.c_str());

    auto dataObject = j_object_new(stringDataObject, stepBlockID);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesWritten == dataSize)
    {
        // std::cout << "++ Julea Interaction Writer: Data written for:  "
        // << stepBlockID << std::endl;
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

    // std::cout << "++ Julea Interaction: PutVariableDataToJulea" << std::endl;
}

template <class T>
void DBPutAttributeMetadataToJuleaSmall(Attribute<T> &attribute,
                                        bson_t *bsonMetaData,
                                        const std::string nameSpace)
{

    const char *kvNames = "attribute_names";
    const char *kvMD = "attributes";

    // TODO: more leaks than old version below ?!
    // DBPutNameToJulea(attribute.m_Name, nameSpace.c_str(), kvNames);
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

// FIXME: not yet implemented correctly! need to differentiate between strings
// and other types
template <class T>
void DBPutAttributeDataToJuleaSmall(Attribute<T> &attribute, const T *data,
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
    // std::cout << "++ Julea Interaction: PutAttributeDataToJuleaSmall"
    // << std::endl;
}

/** ------------ ATTRIBUTES -------------------------------------------------**/

template <class T>
void DBPutAttributeDataToJulea(Attribute<T> &attribute,
                               const std::string nameSpace)
{
    // std::cout << "-- PutAttributeDataToJulea -------" << std::endl;
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
    g_assert_true(j_batch_execute(batch) == true);
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

    // std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

template <>
void DBPutAttributeDataToJulea<std::string>(Attribute<std::string> &attribute,
                                            const std::string nameSpace)
{
    // std::cout << "-- PutAttributeDataToJulea -------" << std::endl;

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
        g_assert_true(j_batch_execute(batch) == true);
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
        g_assert_true(j_batch_execute(batch) == true);
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
void DBPutAttributeMetadataToJulea(Attribute<T> &attribute,
                                   bson_t *bsonMetaData,
                                   const std::string nameSpace)
{
    // std::cout << "-- PutAttributeMetadataToJulea ------ " << std::endl;
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
    g_assert_true(j_batch_execute(batch) == true);
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
    g_assert_true(j_batch_execute(batch2) == true);

    free(attrName);
    g_free(stringMetadataKV);
    j_kv_unref(kvObjectNames);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    bson_destroy(bsonNames);
    j_semantics_unref(semantics);

    // std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

#define declare_template_instantiation(T)                                      \
    template void DBPutVariableDataToJulea(                                    \
        Variable<T> &variable, const T *data, const std::string nameSpace,     \
        size_t currentStep, size_t blockID);                                   \
    template void DBPutVariableMetadataToJulea(                                \
        Variable<T> &variable, const std::string nameSpace,                    \
        const std::string varName, size_t currStep, size_t block);             \
    template void DBPutBlockMetadataToJulea(                                   \
        Variable<T> &variable, const std::string nameSpace,                    \
        const std::string varName, size_t step, size_t block,                  \
        const typename Variable<T>::Info &blockInfo, T &blockMin,              \
        T &blockMax);                                                          \
                                                                               \
    template void DBPutAttributeDataToJulea(Attribute<T> &attribute,           \
                                            const std::string nameSpace);      \
    template void DBPutAttributeDataToJuleaSmall(                              \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    template void DBPutAttributeMetadataToJulea(Attribute<T> &attribute,       \
                                                bson_t *bsonMetaData,          \
                                                const std::string nameSpace);  \
    template void DBPutAttributeMetadataToJuleaSmall(                          \
        Attribute<T> &attribute, bson_t *bsonMetaData,                         \
        const std::string nameSpace);

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
