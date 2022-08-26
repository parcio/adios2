/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaKVInteractionReader.h"
#include "JuleaKVInteractionReader.tcc"
// #include "JuleaKVDAIInteractionReader.h"
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

JuleaKVInteractionReader::JuleaKVInteractionReader(helper::Comm const &comm)
: JuleaInteraction(std::move(comm))
{
    // std::cout << "This is the constructor of the reader" << std::endl;
}

void getIDFromBSON(bson_t *bsonMetadata, uint32_t *blockID)
{
    bson_iter_t bIter;

    // std::cout << "----- ParseVariableFromBSON --- " << std::endl;
    if (bson_iter_init(&bIter, bsonMetadata))
    {
        // std::cout << "++ Julea Client Logic: Bson iterator is valid"
        //   << std::endl;
        while (bson_iter_next(&bIter))
        {
            // if (g_strcmp0(bson_iter_key(&bIter), "shape_size") == 0)
            if (g_strcmp0(bson_iter_key(&bIter), "block_id") == 0)
            {
                *blockID = (std::size_t)bson_iter_int64(&bIter);
            }
        }
    }
    // else
    // {
    //     std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    // }
}

void JuleaKVInteractionReader::InitVariable(
    core::IO *io, core::Engine &engine, const std::string projectNamespace,
    const std::string fileName, std::string varName, std::vector<size_t> blocks,
    size_t numberSteps, ShapeID shapeID, bool isReadAsJoined,
    bool isReadAsLocalValue, bool isRandomAccess, bool isSingleValue)
{
    std::cout << "----- InitVariable --- " << varName << std::endl;
    const adios2::DataType type(io->InquireVariableType(varName));

    // std::cout << "type(io->InquireVariableType(varName): " << type <<
    // std::endl;

    int err = 0;
    uint32_t blockID = 0;
    uint32_t *tmpID;
    size_t step;
    size_t block;
    void *mdBuf = NULL;
    guint32 mdLen = 0;
    bson_t bsonMetadata;
    // bson_t *bsonMetadata;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    // auto completeNamespace = g_strdup_printf(
    //     "%s_%s_%s", "adios2", projectNamespace.c_str(), "variable-names");

    /** store all variable names for a file = namespace */
    // auto varNames = j_kv_new(completeNamespace, fileName.c_str());
    // j_kv_get(varNames, &namesBuf, &valueLen, batch);
    // err = j_batch_execute(batch);

    // schema = j_db_schema_new(completeNamespace, "variable-metadata", NULL);

    // auto blockNamespace =
    //     g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
    // // auto blockSchema = j_db_schema_new(blockNamespace, "block-metadata",
    // NULL);
    // // j_db_schema_get(blockSchema, batch, NULL);
    // err = j_batch_execute(batch);

    // auto varNamespace =
    //     g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
    // // auto varSchema = j_db_schema_new(varNamespace, "variable-metadata",
    // // NULL); j_db_schema_get(varSchema, batch, NULL);
    // err = j_batch_execute(batch);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "block-metadata");

    auto fileVarStepBlock = g_strdup_printf("%s_%s_%d_%d", fileName.c_str(),
                                            varName.c_str(), step, block);
    auto blockMetadata = j_kv_new(completeNamespace, fileVarStepBlock);
    // j_kv_get(blockMetadata, &mdBuf, &mdLen, batch);
    // err = j_batch_execute(batch);

    std::string minField;
    std::string maxField;
    std::string valueField;
    std::string meanField;
    std::string sumField;

    // TODO: min/max not set yet!
    //  somewhere this has to be done: var->m_Min = *min;
    // bson_append_double(bsonMetadata, "min_float64", -1, variable.m_Min);
    // bson_append_double(bsonMetadata, "max_float64", -1, variable.m_Max);

    // TODO: was i+1; what is correct? i, i+1?!
    //   var->m_AvailableStepBlockIndexOffsets[i+1].push_back(blockID);

    /** AvailableStepBlockIndexOffsets stores the entries (= blocks) _id (= line
     * in the sql table) */
    if (type == DataType::Compound)
    {
    }
#define declare_type(T)                                                        \
    else if (type == helper::GetDataType<T>())                                 \
    {                                                                          \
        auto var = io->InquireVariable<T>(varName);                            \
        if (var)                                                               \
        {                                                                      \
            var->m_ShapeID = shapeID;                                          \
            for (size_t i = 0; i < numberSteps; i++)                           \
            {                                                                  \
                for (size_t j = 0; j < blocks[i]; j++)                         \
                {                                                              \
                    step = i;                                                  \
                    block = j;                                                 \
                    fileVarStepBlock =                                         \
                        g_strdup_printf("%s_%s_%d_%d", fileName.c_str(),       \
                                        varName.c_str(), step, block);         \
                    blockMetadata =                                            \
                        j_kv_new(completeNamespace, fileVarStepBlock);         \
                    j_kv_get(blockMetadata, &mdBuf, &mdLen, batch);            \
                    err = j_batch_execute(batch);                              \
                    if (mdLen == 0)                                            \
                    {                                                          \
                        printf("WARNING: The variable metadata is empty! \n"); \
                    }                                                          \
                    else                                                       \
                    {                                                          \
                        bson_init_static(&bsonMetadata, (uint8_t *)mdBuf,      \
                                         mdLen);                               \
                        getIDFromBSON(&bsonMetadata, &blockID);                \
                        var->m_AvailableStepBlockIndexOffsets[i + 1]           \
                            .push_back(blockID);                               \
                    }                                                          \
                }                                                              \
                var->m_AvailableStepsCount++;                                  \
            }                                                                  \
                                                                               \
            JuleaInteraction::SetMinMaxValueFields(&minField, &maxField,       \
                                                   &valueField, &meanField,    \
                                                   &sumField, type);           \
            var->m_AvailableStepsStart = 0;                                    \
            var->m_StepsStart = 0;                                             \
            var->m_Engine = &engine;                                           \
            var->m_FirstStreamingStep = true;                                  \
            var->m_ReadAsJoined = isReadAsJoined;                              \
            var->m_ReadAsLocalValue = isReadAsLocalValue;                      \
            var->m_RandomAccess = isRandomAccess;                              \
            var->m_SingleValue = isSingleValue;                                \
                                                                               \
            if (var->m_ShapeID == ShapeID::LocalValue)                         \
            {                                                                  \
                var->m_ShapeID = ShapeID::GlobalArray;                         \
                var->m_SingleValue = true;                                     \
            }                                                                  \
        }                                                                      \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    // j_batch_unref(batch);
    // j_semantics_unref(semantics);
}

void JuleaKVInteractionReader::DefineVariableInEngineIO(
    core::IO *io, const std::string varName, adios2::DataType type,
    ShapeID shapeID, Dims shape, Dims start, Dims count, bool constantDims,
    bool isLocalValue)
{
    std::cout << "----- DefineVariableInEngineIO --- " << varName << std::endl;

    // std::cout << "shapeID: " << shapeID << "\n";
    // std::cout << "shape size: " << shape.size() << "\n";
    // std::cout << "start size: " << start.size() << "\n";
    // std::cout << "count size: " << count.size() << "\n";

    if (type == DataType::Compound)
    {
    }
#define declare_type(T)                                                        \
    else if (type == helper::GetDataType<T>())                                 \
    {                                                                          \
        core::Variable<T> *variable = nullptr;                                 \
        {                                                                      \
            switch (shapeID)                                                   \
            {                                                                  \
            case (ShapeID::GlobalValue): {                                     \
                variable = &io->DefineVariable<T>(varName);                    \
                break;                                                         \
            }                                                                  \
            case (ShapeID::GlobalArray): {                                     \
                variable = &io->DefineVariable<T>(                             \
                    varName, shape, Dims(shape.size(), 0), shape);             \
                break;                                                         \
            }                                                                  \
            case (ShapeID::LocalValue): {                                      \
                variable = &io->DefineVariable<T>(varName, {1}, {0}, {1});     \
                variable->m_ShapeID = ShapeID::LocalValue;                     \
                break;                                                         \
            }                                                                  \
            case (ShapeID::LocalArray): {                                      \
                variable = &io->DefineVariable<T>(varName, {}, {}, count);     \
                break;                                                         \
            }                                                                  \
            default:                                                           \
                throw std::runtime_error("ERROR: invalid ShapeID or not yet "  \
                                         "supported for variable " +           \
                                         varName + ", in call to Open\n");     \
            }                                                                  \
        }                                                                      \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
}

void JuleaKVInteractionReader::GetVarNamesFromJulea(
    const std::string projectNamespace, const std::string fileName,
    bson_t **bsonNames, unsigned int *varCount)
{
    // std::cout << "-- GetNamesFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    int err = 0;
    void *namesBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    std::string kvName;

    // if (isVariable)
    // {
    //     kvName = "variable_names";
    // }
    // else
    // {
    //     kvName = "attribute_names";
    // }

    // auto kvObject = j_kv_new(kvName.c_str(), fileName.c_str());

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "variable-names");

    /** store all variable names for a file = namespace */
    auto varNames = j_kv_new(completeNamespace, fileName.c_str());

    j_kv_get(varNames, &namesBuf, &valueLen, batch);
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
        j_kv_unref(varNames);
        j_batch_unref(batch);
        return;
    }
    else
    {
        *bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }

    *varCount = bson_count_keys(*bsonNames);

    j_semantics_unref(semantics);
    j_kv_unref(varNames);
    j_batch_unref(batch);
    free(namesBuf);
}

// //TODO: add new params
// //TODO: make variable template function
// void JuleaKVInteractionReader::ParseBlockFromBSON(
//     bson_t *bsonMetadata, ShapeID *shapeID, int *varTypeAsInt, Dims *shape,
//     Dims *start, Dims *count, size_t *numberSteps, std::vector<size_t>
//     *blocks, bool *isConstantDims, bool *isReadAsJoined, bool
//     *isReadAsLocalValue, bool *isRandomAccess, bool *isSingleValue)
// {
//     // bson_iter_t bIter;
//     // bson_t *bsonNames;

//     // size_t shapeSize;
//     // size_t startSize;
//     // size_t countSize;

//     // //TODO: as params
//     // size_t blockID;
//     // Dims *memoryStart;
//     // Dims *memoryCount;
//     // size_t stepsStart;
//     // size_t stepsCount;

//     // gchar const *key;

//     // std::cout << "----- ParseVariableFromBSON --- " << std::endl;
//     // if (bson_iter_init(&bIter, bsonMetadata))
//     // {
//     //     std::cout << "++ Julea Client Logic: Bson iterator is valid"
//     //               << std::endl;
//     // }
//     // else
//     // {
//     //     std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
//     // }

//     // /* probably not very efficient */
//     // while (bson_iter_next(&bIter))
//     // {
//     //     if (g_strcmp0(bson_iter_key(&bIter), "block_id") == 0)
//     //     {
//     //         blockID = (std::size_t)bson_iter_int64(&bIter);
//     //     }

//     //     else if (g_strcmp0(bson_iter_key(&bIter), "shape_size") == 0)
//     //     {
//     //         shapeSize = (std::size_t)bson_iter_int64(&bIter);
//     //         if (shapeSize > 0)
//     //         {
//     //             // size_t *tmpShapeBuffer;
//     //             size_t tmpShapeBuffer[shapeSize];
//     //             for (guint i = 0; i < shapeSize; i++)
//     //             {
//     //                 bson_iter_next(&bIter);
//     //                 key = g_strdup_printf("shape_%d", i);
//     //                 if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
//     //                 {
//     //                     // shape[i] =  bson_iter_int64(&bIter);
//     //                     // bson_iter_int64(&bIter);
//     //                     tmpShapeBuffer[i] = bson_iter_int64(&bIter);
//     //                     std::cout << "tmpShapeBuffer: " <<
//     tmpShapeBuffer[i]
//     //                               << "\n";
//     //                 }
//     //             }
//     //             std::cout << "shapeSize: " << shapeSize << "\n";
//     //             Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + shapeSize);
//     //             // shape = &tmpShape;
//     //             *shape = tmpShape;
//     //             // g_free(tmpShapeBuffer);
//     //         }
//     //     }
//     //     // else if (g_strcmp0(bson_iter_key(&bIter), "shape_id") == 0)
//     //     // {
//     //     //     auto tmp = bson_iter_int32(&bIter);
//     //     //     *shapeID = static_cast<ShapeID>(tmp);
//     //     // }
//     //     // else if (g_strcmp0(bson_iter_key(&bIter), "type_int") == 0)
//     //     // {
//     //     //     *varTypeAsInt = bson_iter_int32(&bIter);
//     //     // }
//     //     else if (g_strcmp0(bson_iter_key(&bIter), "start_size") == 0)
//     //     {
//     //         startSize = (std::size_t)bson_iter_int64(&bIter);
//     //         if (startSize > 0)
//     //         {
//     //             // size_t *tmpStartBuffer;
//     //             size_t tmpStartBuffer[startSize];
//     //             for (guint i = 0; i < startSize; i++)
//     //             {
//     //                 bson_iter_next(&bIter);
//     //                 key = g_strdup_printf("start_%d", i);
//     //                 if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
//     //                 {
//     //                     // start[i] = bson_iter_int64(&bIter);
//     //                     tmpStartBuffer[i] = bson_iter_int64(&bIter);
//     //                 }
//     //             }
//     //             Dims tmpStart(tmpStartBuffer, tmpStartBuffer + startSize);
//     //             // start = &tmpStart;
//     //             *start = tmpStart;
//     //             // g_free(tmpStartBuffer);
//     //         }
//     //     }
//     //     else if (g_strcmp0(bson_iter_key(&bIter), "count_size") == 0)
//     //     {
//     //         countSize = (std::size_t)bson_iter_int64(&bIter);
//     //         if (countSize > 0)
//     //         {
//     //             // size_t *tmpCountBuffer;
//     //             size_t tmpCountBuffer[countSize];
//     //             for (guint i = 0; i < countSize; i++)
//     //             {
//     //                 bson_iter_next(&bIter);
//     //                 key = g_strdup_printf("count_%d", i);
//     //                 if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
//     //                 {
//     //                     // count[i] = bson_iter_int64(&bIter);
//     //                     tmpCountBuffer[i] = bson_iter_int64(&bIter);
//     //                 }
//     //             }
//     //             Dims tmpCount(tmpCountBuffer, tmpCountBuffer + countSize);
//     //             // count = &tmpCount;
//     //             *count = tmpCount;
//     //             // g_free(tmpCountBuffer);
//     //         }
//     //     }
//     //     else if (g_strcmp0(bson_iter_key(&bIter), "memory_start_size") ==
//     0)
//     //     {
//     //         memoryStartSize = (std::size_t)bson_iter_int64(&bIter);
//     //         if (memoryStartSize > 0)
//     //         {
//     //             size_t tmpMemStartBuffer[memoryStartSize];
//     //             for (guint i = 0; i < memoryStartSize; i++)
//     //             {
//     //                 bson_iter_next(&bIter);
//     //                 key = g_strdup_printf("memory_start_%d", i);
//     //                 if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
//     //                 {
//     //                     // count[i] = bson_iter_int64(&bIter);
//     //                     tmpMemStartBuffer[i] = bson_iter_int64(&bIter);
//     //                 }
//     //             }
//     //             Dims tmpMemStart(tmpMemStartBuffer, tmpMemStartBuffer +
//     memoryStartSize);
//     //             *memoryStart = tmpMemStart;
//     //         }
//     //     }
//     //             else if (g_strcmp0(bson_iter_key(&bIter),
//     "memory_count_size") == 0)
//     //     {
//     //         memoryCountSize = (std::size_t)bson_iter_int64(&bIter);
//     //         if (memoryCountSize > 0)
//     //         {
//     //             size_t tmpMemCountBuffer[memoryCountSize];
//     //             for (guint i = 0; i < memoryCountSize; i++)
//     //             {
//     //                 bson_iter_next(&bIter);
//     //                 key = g_strdup_printf("memory_count_%d", i);
//     //                 if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
//     //                 {
//     //                     // count[i] = bson_iter_int64(&bIter);
//     //                     tmpMemCountBuffer[i] = bson_iter_int64(&bIter);
//     //                 }
//     //             }
//     //             Dims tmpMemCount(tmpMemCountBuffer, tmpMemCountBuffer +
//     memoryCountSize);
//     //             *memoryCount = tmpMemCount;
//     //         }
//     //     }

//     //     else if (g_strcmp0(bson_iter_key(&bIter), "steps_start") == 0)
//     //     {
//     //         *stepsStart = bson_iter_bool(&bIter);
//     //     }
//     //             else if (g_strcmp0(bson_iter_key(&bIter), "steps_count")
//     == 0)
//     //     {
//     //         *stepsCount = bson_iter_bool(&bIter);
//     //     }

//     //     if (false)
//     //     {
//     //         std::cout << "countSize: " << countSize << std::endl;
//     //         std::cout << "startSize: " << startSize << std::endl;
//     //         std::cout << "countSize: " << countSize << std::endl;
//     //         std::cout << "count: " << count->front() << std::endl;
//     //         std::cout << "numberSteps: " << *numberSteps << std::endl;
//     //     }

//     //     if (*shapeID == ShapeID::LocalValue)
//     //     {
//     //         // std::cout << " SHAPEID: LOCAL VALUE" << std::endl;
//     //         // localValue = true;
//     //     }
//     // }
// }

void JuleaKVInteractionReader::ParseVariableFromBSON(
    bson_t *bsonMetadata, ShapeID *shapeID, int *varTypeAsInt, Dims *shape,
    Dims *start, Dims *count, size_t *numberSteps, std::vector<size_t> *blocks,
    bool *isConstantDims, bool *isReadAsJoined, bool *isReadAsLocalValue,
    bool *isRandomAccess, bool *isSingleValue)
{
    bson_iter_t bIter;
    bson_t *bsonNames;

    size_t shapeSize;
    size_t startSize;
    size_t countSize;

    gchar const *key;

    std::cout << "----- ParseVariableFromBSON --- " << std::endl;
    if (bson_iter_init(&bIter, bsonMetadata))
    {
        // std::cout << "++ Julea Client Logic: Bson iterator is valid"
        //   << std::endl;
    }
    else
    {
        std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    }

    /* probably not very efficient */
    while (bson_iter_next(&bIter))
    {
        if (g_strcmp0(bson_iter_key(&bIter), "shape_size") == 0)
        {
            shapeSize = (std::size_t)bson_iter_int64(&bIter);
            if (shapeSize > 0)
            {
                // size_t *tmpShapeBuffer;
                size_t tmpShapeBuffer[shapeSize];
                for (guint i = 0; i < shapeSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("shape_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        // shape[i] =  bson_iter_int64(&bIter);
                        // bson_iter_int64(&bIter);
                        tmpShapeBuffer[i] = bson_iter_int64(&bIter);
                        // std::cout << "tmpShapeBuffer: " << tmpShapeBuffer[i]
                        //   << "\n";
                    }
                }
                // std::cout << "shapeSize: " << shapeSize << "\n";
                Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + shapeSize);
                // shape = &tmpShape;
                *shape = tmpShape;
                // g_free(tmpShapeBuffer);
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "shape_id") == 0)
        {
            auto tmp = bson_iter_int32(&bIter);
            *shapeID = static_cast<ShapeID>(tmp);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "type_int") == 0)
        {
            *varTypeAsInt = bson_iter_int32(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "start_size") == 0)
        {
            startSize = (std::size_t)bson_iter_int64(&bIter);
            if (startSize > 0)
            {
                // size_t *tmpStartBuffer;
                size_t tmpStartBuffer[startSize];
                for (guint i = 0; i < startSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("start_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        // start[i] = bson_iter_int64(&bIter);
                        tmpStartBuffer[i] = bson_iter_int64(&bIter);
                    }
                }
                Dims tmpStart(tmpStartBuffer, tmpStartBuffer + startSize);
                // start = &tmpStart;
                *start = tmpStart;
                // g_free(tmpStartBuffer);
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "count_size") == 0)
        {
            countSize = (std::size_t)bson_iter_int64(&bIter);
            if (countSize > 0)
            {
                // size_t *tmpCountBuffer;
                size_t tmpCountBuffer[countSize];
                for (guint i = 0; i < countSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("count_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        // count[i] = bson_iter_int64(&bIter);
                        tmpCountBuffer[i] = bson_iter_int64(&bIter);
                    }
                }
                Dims tmpCount(tmpCountBuffer, tmpCountBuffer + countSize);
                // count = &tmpCount;
                *count = tmpCount;
                // g_free(tmpCountBuffer);
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "number_steps") == 0)
        {
            // std::cout << "key: numbersteps\n";
            *numberSteps = (std::size_t)bson_iter_int64(&bIter);
            // std::cout << "--- numberSteps: " << *numberSteps << "\n";

            size_t tmpblocks[*numberSteps];
            blocks->reserve(*numberSteps);
            if (*numberSteps > 0)
            {
                for (guint i = 0; i < *numberSteps; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("blockArray_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        tmpblocks[i] = (size_t)bson_iter_int64(&bIter);
                        blocks->push_back(tmpblocks[i]);
                    }
                }
                // blocks = (size_t **)tmpblocks;
                // *blocks = tmpblocks;
                // std::cout << "blocks[0]: " << blocks[0] << "\n";
                // std::cout << "blocks[0]: " << *blocks[0] << "\n";
            }
        }

        // TODO: get all fields from BSON

        else if (g_strcmp0(bson_iter_key(&bIter), "is_single_value") == 0)
        {
            *isSingleValue = (bool)bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "is_constant_dims") == 0)
        {
            *isConstantDims = (bool)bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "is_read_as_joined") == 0)
        {
            *isReadAsJoined = (bool)bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "is_read_as_local_value") ==
                 0)
        {
            *isReadAsLocalValue = (bool)bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "is_random_access") == 0)
        {
            *isRandomAccess = (bool)bson_iter_bool(&bIter);
        }

        if (false)
        {
            std::cout << "constantDims: " << isConstantDims << std::endl;
            std::cout << "isReadAsJoined: " << isReadAsJoined << std::endl;
            std::cout << "isReadAsLocalValue: " << isReadAsLocalValue
                      << std::endl;
            std::cout << "isRandomAccess: " << isRandomAccess << std::endl;
            std::cout << "isSingleValue: " << isSingleValue << std::endl;
            std::cout << "shapeID: " << shapeID << std::endl;
            std::cout << "countSize: " << countSize << std::endl;
            std::cout << "startSize: " << startSize << std::endl;
            std::cout << "countSize: " << countSize << std::endl;
            std::cout << "count: " << count->front() << std::endl;
            std::cout << "numberSteps: " << *numberSteps << std::endl;
        }

        if (*shapeID == ShapeID::LocalValue)
        {
            // std::cout << " SHAPEID: LOCAL VALUE" << std::endl;
            // localValue = true;
        }
    }
}

/** Retrieves the metadata buffer for the variable metadata that do not vary
 * from block to block. The key is the variable name. */
void JuleaKVInteractionReader::GetVariableMetadataFromJulea(
    const std::string projectNamespace, const std::string fileName,
    const std::string varName, bson_t *bsonMetadata)
// const std::string varName, gpointer *buffer, guint32 *buffer_len)
{
    std::cout << "----- GetVariableMetadataFromJulea --- " << varName
              << std::endl;

    int err = 0;
    JDBType type;
    // char *varName;
    void *mdBuf = NULL;
    guint32 mdLen = 0;
    guint32 nameLen = 0;
    // void *metaDataBuf = NULL;
    // char *varTypePtr;
    // std::string varType;
    // bson_t bsonMetadata;
    bson_iter_t bIter;
    bson_t *bsonNames;

    // bool localValue;
    bool isConstantDims;
    bool isReadAsJoined;
    bool isReadAsLocalValue;
    bool isRandomAccess;
    bool isSingleValue;

    // size_t typeLen;
    size_t *blocks;
    size_t *numberSteps;
    // size_t *shapeSize;
    size_t shapeSize;
    size_t startSize;
    size_t countSize;

    ShapeID shapeID;
    adios2::DataType typeInt;
    int *varTypeAsInt;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    // auto kvIterator = j_kv_iterator_new(completeNamespace, fileName.c_str());
    // while (j_kv_iterator_next(kvIterator))
    // {
    gchar const *key;
    // gconstpointer value;
    // guint32 len;
    // gconstpointer data = NULL;
    // gsize alen = 0;

    // get variable name
    // key = j_kv_iterator_get(kvIterator, &value, &nameLen);
    // varName = (char *)value;
    // varName = "T";
    // std::cout << "fileName: " << fileName << "\n";
    // std::cout << "varName: " << varName << "\n";

    // auto fileVar = g_strdup_printf("%s_%s", fileName.c_str(), varName);
    // auto varMetadata = j_kv_new(completeNamespace, fileVar);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "variable-metadata");
    // auto fileVarStepBlock = g_strdup_printf(
    // "%s_%s_%d_%d", fileName.c_str(), variable.m_Name.c_str(), step, block);
    auto fileVar = g_strdup_printf("%s_%s", fileName.c_str(), varName.c_str());
    // auto varMetadata = j_kv_new(completeNamespace, fileVarStepBlock);
    auto varMetadata = j_kv_new(completeNamespace, fileVar);

    // std::cout << "fileVar: " << fileVar << "\n";
    // Read name from kv -> TODO: do something with key

    j_kv_get(varMetadata, &mdBuf, &mdLen, batch);
    err = j_batch_execute(batch);

    if (mdLen == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The variable metadata is empty! \n");
    }
    else
    {
        bson_init_static(bsonMetadata, (uint8_t *)mdBuf, mdLen);
    }
}

void JuleaKVInteractionReader::InitVariablesFromKV(
    const std::string projectNamespace, const std::string fileName,
    core::IO *io, core::Engine &engine)
{
    std::cout << "--- InitVariablesFromKV ---" << std::endl;
    int err = 0;
    char *varName;
    bson_t bsonMetadata;
    bson_iter_t bIter;
    bson_t *bsonNames;
    unsigned int varCount = 0;
    int varTypeAsInt = 0;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "variable-metadata");
    // std::cout << "completeNamespace: " << completeNamespace << "\n";

    GetVarNamesFromJulea(projectNamespace, fileName, &bsonNames, &varCount);

    if (varCount == 0)
    {
        // if (m_Verbosity == 5)
        // {
        std::cout << "++ InitVariables: no variables stored in KV" << std::endl;
        // }
    }
    else
    {
        bson_iter_init(&bIter, bsonNames);

        while (bson_iter_next(&bIter))
        {
            Dims shape;
            Dims start;
            Dims count;
            ShapeID shapeID = ShapeID::Unknown;

            bool isConstantDims;
            bool isReadAsJoined;
            bool isReadAsLocalValue;
            bool isRandomAccess;
            bool isSingleValue;

            std::string type;
            // size_t *blocks;
            std::vector<size_t> blocks;
            size_t numberSteps = 0;

            std::string varName(bson_iter_key(&bIter));

            if (varName == "time")
            {
                // localValueDim is screwing everything up -> no idea what this
                // means (25.08.22)
                std::cout << "\n TODO: implement this case for time variable\n"
                          << std::endl;
                continue;
            }

            GetVariableMetadataFromJulea(projectNamespace, fileName, varName,
                                         &bsonMetadata);

            ParseVariableFromBSON(
                &bsonMetadata, &shapeID, &varTypeAsInt, &shape, &start, &count,
                &numberSteps, &blocks, &isConstantDims, &isReadAsJoined,
                &isReadAsLocalValue, &isRandomAccess, &isSingleValue);
            //          const std::string projectNamespace, const std::string
            //          fileName,
            // const std::string varName, gpointer *buffer, guint32 *buffer_len
            adios2::DataType adiosType{
                static_cast<adios2::DataType>(varTypeAsInt)};
            // std::cout << "adiosType: " << adiosType << std::endl;
            // std::cout << "After ParseVariableFromBSON: numberSteps = "
            //   << numberSteps << "\n";

            DefineVariableInEngineIO(io, varName, adiosType, shapeID, shape,
                                     start, count, isConstantDims,
                                     isSingleValue);
            // std::cout << "after function: blocks[0]: " << blocks[0] << "\n";
            // std::cout << "after function: blocks[0]: " << &blocks[0] << "\n";
            InitVariable(io, engine, projectNamespace, fileName, varName,
                         blocks, numberSteps, shapeID, isReadAsJoined,
                         isReadAsLocalValue, isRandomAccess, isSingleValue);

            // if (numberSteps > 0)
            // {
            //     g_free(*blocks);
            // }
        }
    }
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

#define variable_template_instantiation(T)                                     \
    template void JuleaKVInteractionReader::GetCountFromBlockMetadata(         \
        const std::string projectNamespace, const std::string fileName,        \
        const std::string varName, size_t step, size_t block, Dims *count,     \
        size_t entryID, bool isLocalValue, T *value);                          \
    template std::unique_ptr<typename core::Variable<T>::Info>                 \
    JuleaKVInteractionReader::GetBlockMetadata(                                \
        const core::Variable<T> &variable, std::string projectNamespace,       \
        const std::string fileName, size_t step, size_t block) const;
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

} // end namespace interop
} // end namespace adios2
