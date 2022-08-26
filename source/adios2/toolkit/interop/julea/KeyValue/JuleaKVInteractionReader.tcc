/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONREADER_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONREADER_TCC_

#include "JuleaKVInteractionReader.h"
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

// TODO: filename etc. needed? or is just the entryID sufficient?
template <class T>
void JuleaKVInteractionReader::GetCountFromBlockMetadata(
    const std::string projectNamespace, const std::string fileName,
    const std::string varName, size_t step, size_t block, Dims *count,
    size_t entryID, bool isLocalValue, T *value)
{
    // std::cout << "------ GetCountFromBlockMetadata ----------" << std::endl;
    int err = 0;
    JDBType type;
}

template <class T>
std::unique_ptr<typename core::Variable<T>::Info>
JuleaKVInteractionReader::GetBlockMetadata(const core::Variable<T> &variable,
                                           std::string projectNamespace,
                                           const std::string fileName,
                                           size_t step, size_t block
                                           // size_t entryID
) const
{
    // std::cout << "--- DBGetBlockMetadata ---" << std::endl;
    int err = 0;

    bool *isValue;
    bool *tmpBool;
    T *min;
    T *max;
    T *value;
    size_t blockID;
    size_t shapeSize;
    size_t startSize;
    size_t countSize;
    size_t memoryStartSize;
    size_t memoryCountSize;
    size_t stepsStart;
    size_t stepsCount;

    Dims shape;
    Dims start;
    Dims count;
    Dims memoryStart;
    Dims memoryCount;
    ShapeID *shapeID;

    bson_iter_t bIter;
    bson_t *bsonNames;
    gchar const *key;
    void *mdBuf = NULL;
    guint32 mdLen = 0;
    // bson_t *bsonMetadata;
    bson_t bsonMetadata;

    const char *varName = variable.m_Name.c_str();
    std::unique_ptr<typename core::Variable<T>::Info> info(
        new (typename core::Variable<T>::Info));

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "block-metadata");
    // std::cout << "completeNamespace: " << completeNamespace << "\n";
    auto fileVarStepBlock =
        g_strdup_printf("%s_%s_%d_%d", fileName.c_str(), varName, step, block);
    // std::cout << "fileVarStepBlock: " << fileVarStepBlock << "\n";

    auto blockMetadata = j_kv_new(completeNamespace, fileVarStepBlock);
    j_kv_get(blockMetadata, &mdBuf, &mdLen, batch);
    err = j_batch_execute(batch);

    // std::cout << "err: " << err << " mdLen: " << mdLen << "\n";
    if (mdLen == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The block metadata is empty! \n");
    }
    else
    {
        // bson_init_static(bsonMetadata, (uint8_t *)mdBuf, mdLen);
        bson_init_static(&bsonMetadata, (uint8_t *)mdBuf, mdLen);
    }

    // std::cout << "----- ParseBlockFromBSON --- " << std::endl;
    if (bson_iter_init(&bIter, &bsonMetadata))
    {
        // std::cout << "++ Julea Client Logic: Bson iterator is valid"
        //   << std::endl;
    }
    else
    {
        std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    }

    while (bson_iter_next(&bIter))
    {
        if (g_strcmp0(bson_iter_key(&bIter), "block_id") == 0)
        {
            info->BlockID = (std::size_t)bson_iter_int64(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "shape_size") == 0)
        {
            shapeSize = (std::size_t)bson_iter_int64(&bIter);
            if (shapeSize > 0)
            {
                size_t tmpShapeBuffer[shapeSize];
                for (guint i = 0; i < shapeSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("shape_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        tmpShapeBuffer[i] = bson_iter_int64(&bIter);
                        // std::cout << "tmpShapeBuffer: " << tmpShapeBuffer[i]
                        //   << "\n";
                    }
                }
                // std::cout << "shapeSize: " << shapeSize << "\n";
                Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + shapeSize);
                // *shape = tmpShape;
                info->Shape = tmpShape;
                // TODO: free buffer?
                //  g_free(tmpShapeBuffer);
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "start_size") == 0)
        {
            startSize = (std::size_t)bson_iter_int64(&bIter);
            if (startSize > 0)
            {
                size_t tmpStartBuffer[startSize];
                for (guint i = 0; i < startSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("start_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        tmpStartBuffer[i] = bson_iter_int64(&bIter);
                    }
                }
                Dims tmpStart(tmpStartBuffer, tmpStartBuffer + startSize);
                // *start = tmpStart;
                info->Start = tmpStart;
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "count_size") == 0)
        {
            countSize = (std::size_t)bson_iter_int64(&bIter);
            if (countSize > 0)
            {
                size_t tmpCountBuffer[countSize];
                for (guint i = 0; i < countSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("count_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        tmpCountBuffer[i] = bson_iter_int64(&bIter);
                    }
                }
                Dims tmpCount(tmpCountBuffer, tmpCountBuffer + countSize);
                // *count = tmpCount;
                info->Count = tmpCount;
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "memory_start_size") == 0)
        {
            memoryStartSize = (std::size_t)bson_iter_int64(&bIter);
            if (memoryStartSize > 0)
            {
                size_t tmpMemStartBuffer[memoryStartSize];
                for (guint i = 0; i < memoryStartSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("memory_start_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        tmpMemStartBuffer[i] = bson_iter_int64(&bIter);
                    }
                }
                Dims tmpMemStart(tmpMemStartBuffer,
                                 tmpMemStartBuffer + memoryStartSize);
                // *memoryStart = tmpMemStart;
                info->MemoryStart = tmpMemStart;
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "memory_count_size") == 0)
        {
            memoryCountSize = (std::size_t)bson_iter_int64(&bIter);
            if (memoryCountSize > 0)
            {
                size_t tmpMemCountBuffer[memoryCountSize];
                for (guint i = 0; i < memoryCountSize; i++)
                {
                    bson_iter_next(&bIter);
                    key = g_strdup_printf("memory_count_%d", i);
                    if (g_strcmp0(bson_iter_key(&bIter), key) == 0)
                    {
                        // count[i] = bson_iter_int64(&bIter);
                        tmpMemCountBuffer[i] = bson_iter_int64(&bIter);
                    }
                }
                Dims tmpMemCount(tmpMemCountBuffer,
                                 tmpMemCountBuffer + memoryCountSize);
                // *memoryCount = tmpMemCount;
                info->MemoryCount = tmpMemCount;
            }
        }

        else if (g_strcmp0(bson_iter_key(&bIter), "steps_start") == 0)
        {
            info->StepsStart = bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "steps_count") == 0)
        {
            info->StepsCount = bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "steps_count") == 0)
        {
            info->StepsCount = bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "is_value") == 0)
        {
            info->IsValue = bson_iter_bool(&bIter);
            if (info->IsValue)
            {
                if (g_strcmp0(bson_iter_key(&bIter), "value_float64") == 0)
                {
                    info->Value = bson_iter_bool(&bIter);
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "min_float64") == 0)
        {
            info->Min = bson_iter_bool(&bIter);
        }
        else if (g_strcmp0(bson_iter_key(&bIter), "max_float64") == 0)
        {
            info->Max = bson_iter_bool(&bIter);
        }

        // TODO: info->IsValue
        // TODO: info->Value
        // TODO: info->Min
        // TODO: info->Max

        if (false)
        {
            std::cout << "countSize: " << countSize << std::endl;
            std::cout << "startSize: " << startSize << std::endl;
            std::cout << "countSize: " << countSize << std::endl;
            std::cout << "count: " << count.front() << std::endl;
        }
    }

    return info;
}

} // end namespace interop
} // end namespace adios2

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_TCC_ */
