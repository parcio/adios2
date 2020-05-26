/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATREADER_
#define ADIOS2_ENGINE_JULEAFORMATREADER_

#include "JuleaFormatReader.h"

#include <bson.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
void SetVariableBlockInfo(Variable<T> &variable,
                          typename core::Variable<T>::Info &blockInfo)
{
    // FIXME: probably annoying complicated blockselection stuff to be done here
}

void InitVariable(core::IO *io, core::Engine &engine, std::string varName,
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

void DefineVariableInInit(core::IO *io, const std::string varName,
                          std::string stringType, Dims shape, Dims start,
                          Dims count, bool constantDims)
{
    const char *type = stringType.c_str();
    // std::cout << "------ DefineVariableInInit ----------" << std::endl;
    // std::cout << "------ type  ---------- " << type << std::endl;

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
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "int8_t") == 0)
    {
        auto &var = io->DefineVariable<int8_t>(varName, shape, start, count,
                                               constantDims);
        // std::cout << "Defined variable of type: " << type << std::endl;
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
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "double") == 0)
    {
        auto &var = io->DefineVariable<double>(varName, shape, start, count,
                                               constantDims);
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "long double") == 0)
    {
        auto &var = io->DefineVariable<long double>(varName, shape, start,
                                                    count, constantDims);
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex float") == 0)
    {
        auto &var = io->DefineVariable<std::complex<float>>(
            varName, shape, start, count, constantDims);
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex double") == 0)
    {
        auto &var = io->DefineVariable<std::complex<double>>(
            varName, shape, start, count, constantDims);
        // std::cout << "Defined variable of type: " << type << std::endl;
    }

    std::map<std::string, Params> varMap = io->GetAvailableVariables();

    for (std::map<std::string, Params>::iterator it = varMap.begin();
         it != varMap.end(); ++it)
    {

        // std::cout << "first: " << it->first << " => " << it->second.begin()
        // << '\n';
        // std::cout << "first: " << it->first << '\n';
    }
}

void DeserializeVariableMetadata(gpointer buffer, std::string *type,
                                 Dims *shape, Dims *start, Dims *count,
                                 bool *constantDims, size_t **blocks,
                                 size_t *numberSteps, ShapeID *shapeID,
                                 bool *readAsJoined, bool *readAsLocalValue,
                                 bool *randomAccess)
{
    // std::cout << "------ DeserializeVariableMetadata ----------" <<
    // std::endl;
    char *tmpBuffer = (char *)buffer;
    int tmpShapeID = 0;

    size_t typeLen = 0;
    size_t shapeLen = 0;
    size_t startLen = 0;
    size_t countLen = 0;

    size_t shapeSize = 0;
    size_t startSize = 0;
    size_t countSize = 0;
    size_t steps = 0;
    bool isConstantDims = false;
    bool isReadAsJoined = false;
    bool isReadAsLocalValue = false;
    bool isRandomAccess = false;

    /** --- isConstantDims --- */
    memcpy(constantDims, tmpBuffer, sizeof(bool));
    memcpy(&isConstantDims, tmpBuffer, sizeof(bool));
    tmpBuffer += sizeof(bool);

    /** --- isReadAsJoined ---*/
    memcpy(readAsJoined, tmpBuffer, sizeof(bool));
    memcpy(&isReadAsJoined, tmpBuffer, sizeof(bool));
    tmpBuffer += sizeof(bool);

    /** --- isReadAsLocalValue ---*/
    memcpy(readAsLocalValue, tmpBuffer, sizeof(bool));
    tmpBuffer += sizeof(bool);

    /** --- isRandomAccess ---*/
    memcpy(randomAccess, tmpBuffer, sizeof(bool));
    tmpBuffer += sizeof(bool);

    /** --- type --- */
    memcpy(&typeLen, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);
    char tmpType[typeLen];

    memcpy(&tmpType, tmpBuffer, typeLen);
    tmpBuffer += typeLen;
    std::string t(tmpType);
    *type = t;

    /** --- shapeID --- */
    memcpy(&tmpShapeID, tmpBuffer, sizeof(int));
    tmpBuffer += sizeof(int);

    /** --- shape --- */
    memcpy(&shapeSize, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);
    shapeLen = sizeof(size_t) * (shapeSize);

    size_t tmpShapeBuffer[shapeSize];

    memcpy(&tmpShapeBuffer, tmpBuffer, shapeLen);
    tmpBuffer += shapeLen;

    if (shapeSize > 0)
    {
        Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + shapeSize);
        *shape = tmpShape;
    }

    /** --- start --- */
    memcpy(&startSize, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);
    startLen = sizeof(size_t) * (startSize);

    size_t tmpStartBuffer[startSize];

    memcpy(&tmpStartBuffer, tmpBuffer, startLen);
    tmpBuffer += startLen;

    if (startSize > 0)
    {
        Dims tmpStart(tmpStartBuffer, tmpStartBuffer + startSize);
        *start = tmpStart;
    }

    /** --- count --- */
    memcpy(&countSize, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);
    countLen = sizeof(size_t) * (countSize); //

    size_t tmpCountBuffer[countSize];

    memcpy(&tmpCountBuffer, tmpBuffer, countLen);
    tmpBuffer += countLen;
    if (countSize > 0)
    {
        Dims tmpCount(tmpCountBuffer, tmpCountBuffer + countSize);
        *count = tmpCount;
        // std::cout << "count: " << count->front() << std::endl;
    }

    /** --- blocks --- */
    memcpy(&steps, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);
    *numberSteps = steps;

    size_t blocksLen = steps * sizeof(size_t);
    size_t *tmpBlocks = new size_t[steps]; // TODO mem leak?
    memcpy(tmpBlocks, tmpBuffer, blocksLen);

    switch (tmpShapeID)
    {
    case 0:
        *shapeID = ShapeID::Unknown;
        // std::cout << "test: " << std::endl;
        break;
    case 1:
        *shapeID = ShapeID::GlobalValue;
        break;
    case 2:
        *shapeID = ShapeID::GlobalArray;
        break;
    case 3:
        *shapeID = ShapeID::JoinedArray;
        break;
    case 4:
        *shapeID = ShapeID::LocalValue;
        break;
    case 5:
        *shapeID = ShapeID::LocalArray;
        // std::cout << "shapeID: " << *shapeID << std::endl;
        break;
    }

    // std::cout << "isConstantDims: " << isConstantDims << std::endl;
    // std::cout << "tmpShapeID: " << tmpShapeID << std::endl;
    // std::cout << "typeLen: " << typeLen << std::endl;
    // std::cout << "tmpShapeID: " << tmpShapeID << std::endl;
    // std::cout << "tmpType: " << tmpType << std::endl;
    // std::cout << "count size" << countSize << std::endl;
    // std::cout << "steps: " << steps << std::endl;
    // std::cout << "numberSteps: " << *numberSteps << std::endl;

    // std::cout << "block[0]: " << tmpBlocks[0] << std::endl;
    // std::cout << "block[1]: " << tmpBlocks[1] << std::endl;

    *blocks = tmpBlocks;
}

void GetCountFromBlockMetadata(gpointer buffer, Dims *count)
{
    // std::cout << "------ DeserializeBlockMetadata ----------" << std::endl;

    char *tmpBuffer = (char *)buffer;

    size_t countSize = 0;
    size_t countLen = 0;

    /** count */
    memcpy(&countSize, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);

    size_t tmpCountBuffer[countSize];
    countLen = sizeof(size_t) * countSize;

    memcpy(&tmpCountBuffer, tmpBuffer, countLen);
    tmpBuffer += countLen;
    if (countSize > 0)
    {
        Dims tmpCount(tmpCountBuffer, tmpCountBuffer + countSize);
        *count = tmpCount;
        // std::cout << "count: " << tmpCount.front() << std::endl;
        // std::cout << "count size: " << tmpCount.size() << std::endl;
    }
}

/**
 * Deserializes the passed buffer and adds the created blockinfo to the variable
 */
// template <class T>
// void DeserializeBlockMetadata(Variable<T> &variable, gpointer buffer,
//                               size_t block,
//                               typename core::Variable<T>::Info &infoParam)
// {
//     // std::cout << "------ DeserializeBlockMetadata ----------" <<
//     std::endl;

//     typename Variable<T>::Info info = infoParam;
//     char *tmpBuffer = (char *)buffer;

//     size_t shapeSize = 0;
//     size_t startSize = 0;
//     size_t countSize = 0;
//     size_t memoryStartSize = 0;
//     size_t memoryCountSize = 0;

//     size_t shapeLen = 0;
//     size_t startLen = 0;
//     size_t countLen = 0;
//     size_t memoryStartLen = 0;
//     size_t memoryCountLen = 0;

//     size_t minLen = sizeof(T);
//     size_t maxLen = sizeof(T);
//     size_t valueLen = sizeof(T);

//     size_t stepsStart = 0;
//     size_t stepsCount = 0;
//     size_t blockID = 0;

//     bool isValue = false;

//     /** count */
//     memcpy(&countSize, tmpBuffer, sizeof(size_t));
//     tmpBuffer += sizeof(size_t);

//     size_t tmpCountBuffer[countSize];
//     countLen = sizeof(size_t) * countSize;

//     memcpy(&tmpCountBuffer, tmpBuffer, countLen);
//     tmpBuffer += countLen;
//     if (countSize > 0)
//     {
//         Dims tmpCount(tmpCountBuffer, tmpCountBuffer + countSize);
//         info.Count = tmpCount;
//         // std::cout << "count: " << info.Count.front() << std::endl;
//     }

//     /** shape */
//     memcpy(&shapeSize, tmpBuffer, sizeof(size_t));
//     tmpBuffer += sizeof(size_t);

//     shapeLen = sizeof(size_t) * shapeSize;
//     size_t tmpShapeBuffer[shapeSize];

//     memcpy(&tmpShapeBuffer, tmpBuffer, shapeLen);
//     tmpBuffer += shapeLen;
//     if (shapeSize > 0)
//     {
//         Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + shapeSize);
//         info.Shape = tmpShape;
//     }

//     /** start */
//     memcpy(&startSize, tmpBuffer, sizeof(size_t));
//     tmpBuffer += sizeof(size_t);

//     startLen = sizeof(size_t) * startSize;
//     size_t tmpStartBuffer[startSize];

//     memcpy(&tmpStartBuffer, tmpBuffer, startLen);
//     tmpBuffer += startLen;
//     if (startSize > 0)
//     {
//         Dims tmpStart(tmpStartBuffer, tmpStartBuffer + startSize);
//         info.Start = tmpStart;
//     }

//     /** ---memorystart --- */
//     memcpy(&memoryStartSize, tmpBuffer, sizeof(size_t));
//     tmpBuffer += sizeof(size_t);

//     size_t tmpMemoryStartBuffer[memoryStartSize];
//     memoryStartLen = sizeof(size_t) * memoryStartSize;

//     memcpy(&tmpMemoryStartBuffer, tmpBuffer, memoryStartLen);
//     tmpBuffer += memoryStartLen;

//     if (countSize > 0)
//     {
//         Dims tmpMemoryStart(tmpMemoryStartBuffer,
//                             tmpMemoryStartBuffer + memoryStartLen);
//         info.MemoryStart = tmpMemoryStart;
//     }

//     /** ---memorycount --- */
//     memcpy(&memoryCountSize, tmpBuffer, sizeof(size_t));
//     tmpBuffer += sizeof(size_t);

//     size_t tmpMemoryCountBuffer[memoryCountSize];
//     memoryCountLen = sizeof(size_t) * memoryCountSize;

//     memcpy(&tmpMemoryCountBuffer, tmpBuffer, memoryCountLen);
//     tmpBuffer += memoryCountLen;

//     if (countSize > 0)
//     {
//         Dims tmpMemoryCount(tmpMemoryCountBuffer,
//                             tmpMemoryCountBuffer + memoryCountLen);
//         info.MemoryCount = tmpMemoryCount;
//     }

//     /** --- no more vectors from here on --- */
//     memcpy(&info.Min, tmpBuffer, minLen); // Min
//     tmpBuffer += sizeof(minLen);

//     memcpy(&info.Max, tmpBuffer, maxLen); // Max
//     tmpBuffer += sizeof(maxLen);

//     memcpy(&info.Value, tmpBuffer, valueLen); // Value
//     tmpBuffer += sizeof(maxLen);

//     memcpy(&info.StepsStart, tmpBuffer, sizeof(size_t)); // StepsStart
//     tmpBuffer += sizeof(size_t);

//     memcpy(&info.StepsCount, tmpBuffer, sizeof(size_t)); // StepsCount
//     tmpBuffer += sizeof(size_t);

//     memcpy(&info.BlockID, tmpBuffer, sizeof(size_t)); // BlockID
//     tmpBuffer += sizeof(size_t);

//     memcpy(&info.IsValue, tmpBuffer, sizeof(bool)); // isValue
//     tmpBuffer += sizeof(bool);

//     // std::cout << "shapeSize: " << shapeSize << std::endl;
//     // std::cout << "startSize: " << startSize << std::endl;
//     // std::cout << "countSize: " << countSize << std::endl;
//     // std::cout << "memoryStartSize: " << memoryStartSize << std::endl;
//     // std::cout << "memoryCountSize: " << memoryCountSize << std::endl;
//     // std::cout << "info.Min: " << info.Min << std::endl;
//     // std::cout << "info.Max: " << info.Max << std::endl;
//     // std::cout << "info.Value: " << info.Value << std::endl;
//     // std::cout << "info.StepsStart: " << info.StepsStart << std::endl;
//     // std::cout << "info.StepsCount: " << info.StepsCount << std::endl;
//     // std::cout << "info.BlockID: " << info.BlockID << std::endl;
//     // std::cout << "info.IsValue: " << info.IsValue << std::endl;
//     // std::cout << "size: m_BlocksInfo " << variable.m_BlocksInfo.size()
//     //           << std::endl;

//     variable.m_BlocksInfo[0] = info;
// }

/**
 * Deserializes the passed buffer and returns the created info struct.
 *
 * Note: the variable is only passed, because it seems to be not possible to
 * have an info struct without a variable. Template type cannot be deduced then.
 * Variable is const as this function is called with bpls.
 */
// typename core::Variable<T>::Info *
template <class T>
std::unique_ptr<typename core::Variable<T>::Info>
GetDeserializedMetadata(const core::Variable<T> &variable, gpointer buffer)
{
    // std::cout << "------ GetDeserializedMetadata ----------" << std::endl;
    std::unique_ptr<typename Variable<T>::Info> info(
        new (typename Variable<T>::Info));
    char *tmpBuffer = (char *)buffer;

    size_t shapeSize = 0;
    size_t startSize = 0;
    size_t countSize = 0;
    size_t memoryStartSize = 0;
    size_t memoryCountSize = 0;

    size_t shapeLen = 0;
    size_t startLen = 0;
    size_t countLen = 0;
    size_t memoryStartLen = 0;
    size_t memoryCountLen = 0;

    size_t minLen = sizeof(T);
    size_t maxLen = sizeof(T);
    size_t valueLen = sizeof(T);

    size_t stepsStart = 0;
    size_t stepsCount = 0;
    size_t blockID = 0;

    bool isValue = false;

    /** --- count --- */
    memcpy(&countSize, tmpBuffer, sizeof(size_t)); // count
    tmpBuffer += sizeof(size_t);

    size_t tmpCountBuffer[countSize];
    countLen = sizeof(size_t) * countSize;

    memcpy(&tmpCountBuffer, tmpBuffer, countLen);
    tmpBuffer += countLen;
    if (countSize > 0)
    {
        Dims tmpCount(tmpCountBuffer, tmpCountBuffer + countSize);
        info->Count = tmpCount;
        // std::cout << "count: " << info.Count.front() << std::endl;
        // delete tmpCount;
    }

    /** --- shape --- */
    memcpy(&shapeSize, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);

    shapeLen = sizeof(size_t) * shapeSize;
    size_t tmpShapeBuffer[shapeSize];

    memcpy(&tmpShapeBuffer, tmpBuffer, shapeLen);
    tmpBuffer += shapeLen;
    if (shapeSize > 0)
    {
        Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + shapeSize);
        info->Shape = tmpShape;
    }

    /** --- start --- */
    memcpy(&startSize, tmpBuffer, sizeof(size_t));
    tmpBuffer += sizeof(size_t);

    startLen = sizeof(size_t) * startSize;
    size_t tmpStartBuffer[startSize];

    memcpy(&tmpStartBuffer, tmpBuffer, startLen);
    tmpBuffer += startLen;
    if (startSize > 0)
    {
        Dims tmpStart(tmpStartBuffer, tmpStartBuffer + startSize);
        info->Start = tmpStart;
    }

    /** --- memorystart --- */
    memcpy(&memoryStartSize, tmpBuffer, sizeof(size_t)); // count
    tmpBuffer += sizeof(size_t);

    size_t tmpMemoryStartBuffer[memoryStartSize];
    memoryStartLen = sizeof(size_t) * memoryStartSize;

    memcpy(&tmpMemoryStartBuffer, tmpBuffer, memoryStartLen);
    tmpBuffer += memoryStartLen;

    if (countSize > 0)
    {
        Dims tmpMemoryStart(tmpMemoryStartBuffer,
                            tmpMemoryStartBuffer + memoryStartLen);
        info->MemoryStart = tmpMemoryStart;
    }

    /** --- memorycount --- */
    memcpy(&memoryCountSize, tmpBuffer, sizeof(size_t)); // count
    tmpBuffer += sizeof(size_t);

    size_t tmpMemoryCountBuffer[memoryCountSize];
    memoryCountLen = sizeof(size_t) * memoryCountSize;

    memcpy(&tmpMemoryCountBuffer, tmpBuffer, memoryCountLen);
    tmpBuffer += memoryCountLen;

    if (countSize > 0)
    {
        Dims tmpMemoryCount(tmpMemoryCountBuffer,
                            tmpMemoryCountBuffer + memoryCountLen);
        info->MemoryCount = tmpMemoryCount;
    }

    /** --- no more vectors from here on --- */
    memcpy(&info->Min, tmpBuffer, minLen); // Min
    tmpBuffer += sizeof(minLen);

    memcpy(&info->Max, tmpBuffer, maxLen); // Max
    tmpBuffer += sizeof(maxLen);

    memcpy(&info->Value, tmpBuffer, valueLen); // Value
    tmpBuffer += sizeof(maxLen);

    memcpy(&info->StepsStart, tmpBuffer, sizeof(size_t)); // StepsStart
    tmpBuffer += sizeof(size_t);

    memcpy(&info->StepsCount, tmpBuffer, sizeof(size_t)); // StepsCount
    tmpBuffer += sizeof(size_t);

    memcpy(&info->BlockID, tmpBuffer, sizeof(size_t)); // BlockID
    tmpBuffer += sizeof(size_t);

    memcpy(&info->IsValue, tmpBuffer, sizeof(bool)); // isValue
    tmpBuffer += sizeof(bool);

    // std::cout << "shapeSize: " << shapeSize << std::endl;
    // std::cout << "startSize: " << startSize << std::endl;
    // std::cout << "countSize: " << countSize << std::endl;
    // std::cout << "memoryStartSize: " << memoryStartSize << std::endl;
    // std::cout << "memoryCountSize: " << memoryCountSize << std::endl;
    // std::cout << "info.Min: " << info->Min << std::endl;
    // std::cout << "info.Max: " << info->Max << std::endl;
    // std::cout << "info.Value: " << info->Value << std::endl;
    // std::cout << "info.StepsStart: " << info->StepsStart << std::endl;
    // std::cout << "info.StepsCount: " << info->StepsCount << std::endl;
    // std::cout << "info.BlockID: " << info->BlockID << std::endl;
    // std::cout << "info.IsValue: " << info->IsValue << std::endl;

    return info;
}

void GetAdiosTypeString(int type, std::string *typeString)
{
    switch (type)
    {
    // case COMPOUND:
    //     //TODO
    //     break;
    // case UNKNOWN:
    //     //TODO
    //     break;
    case STRING:
        *typeString = "string";
        break;
    case INT8:
        *typeString = "int8_t";
        break;
    case UINT8:
        *typeString = "uint8_t";
        break;
    case INT16:
        *typeString = "int16_t";
        break;
    case UINT16:
        *typeString = "uint16_t";
        break;
    case INT32:
        *typeString = "int32_t";
        break;
    case UINT32:
        *typeString = "uint32_t";
        break;
    case INT64:
        *typeString = "int64_t";
        break;
    case UINT64:
        *typeString = "uint64_t";
        break;
    case FLOAT:
        *typeString = "float";
        break;
    case DOUBLE:
        *typeString = "double";
        break;
    case LONG_DOUBLE:
        *typeString = "long double";
        break;
    case COMPLEX_FLOAT:
        *typeString = "complex float";
        break;
    case COMPLEX_DOUBLE:
        *typeString = "complex double";
        break;
    }
}

void ParseAttributeFromBSON(const std::string nameSpace,
                            const std::string attrName, bson_t *bsonMetadata,
                            long unsigned int *dataSize, size_t *numberElements,
                            bool *IsSingleValue, int *type)
{
    bson_iter_t b_iter;
    gchar *key = NULL;

    if (bson_iter_init(&b_iter, bsonMetadata))
    {
        // std::cout << "++ Julea Format Reader: Bson iterator is valid"
        // << std::endl;
    }
    else
    {
        std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    }

    while (bson_iter_next(&b_iter))
    {
        if (g_strcmp0(bson_iter_key(&b_iter), "number_elements") == 0)
        {
            *numberElements = (size_t)bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_single_value") == 0)
        {
            *IsSingleValue = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "data_size") == 0)
        {
            // std::cout << "___ Datasize = " << *dataSize << std::endl;
            *dataSize = bson_iter_int64(&b_iter);
            // std::cout << "___ Datasize = " << *dataSize << std::endl;
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "attr_type") == 0)
        {
            // std::cout << "___ type = " << *type << std::endl;
            *type = bson_iter_int32(&b_iter);
            // std::cout << "___ type = " << *type << std::endl;
        }
    }
    g_free(key);
}

void ParseAttributeFromBSON(const std::string nameSpace,
                            const std::string attrName, bson_t *bsonMetadata,
                            long unsigned int *completeSize,
                            size_t *numberElements, bool *IsSingleValue,
                            int *type, unsigned long **dataSizes)
{
    bson_iter_t b_iter;
    gchar *key = NULL;

    if (bson_iter_init(&b_iter, bsonMetadata))
    {
        // std::cout << "++ Julea Format Reader: Bson iterator is valid"
                  // << std::endl;
    }
    else
    {
        // std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    }

    while (bson_iter_next(&b_iter))
    {
        if (g_strcmp0(bson_iter_key(&b_iter), "number_elements") == 0)
        {
            *numberElements = (size_t)bson_iter_int64(&b_iter);
            // std::cout << "numberElements: " << *numberElements << std::endl;
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_single_value") == 0)
        {
            *IsSingleValue = (bool)bson_iter_bool(&b_iter);
            // std::cout << "IsSingleValue: " << *IsSingleValue << std::endl;
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "complete_data_size") == 0)
        {
            *completeSize = bson_iter_int64(&b_iter);
            // std::cout << "___ completeSize = " << *completeSize << std::endl;
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "attr_type") == 0)
        {
            // std::cout << "___ type = " << *type << std::endl;
            *type = bson_iter_int32(&b_iter);
            // std::cout << "___ type = " << *type << std::endl;
        }
    }
    if (!*IsSingleValue && (*type == STRING))
    {
        bson_iter_init(&b_iter, bsonMetadata);
        *dataSizes = new unsigned long[*numberElements];
        for (int i = 0; i < *numberElements; ++i)
        {
            std::cout << "i: " << i << std::endl;

            key = g_strdup_printf("entry_size_%d", i);
            // std::cout << "key: " << key << std::endl;

            if (bson_iter_find(&b_iter, key))
            {
                (*dataSizes)[i] = bson_iter_int64(&b_iter);
                // std::cout << "___ Datasize = " << (*dataSizes)[i] <<
                // std::endl;
            }
            g_free(key);
        }
    }
}
// template void DeserializeBlockMetadata(                                    \
    //     Variable<T> &variable, gpointer buffer, size_t block,                  \
    //     typename core::Variable<T>::Info &info);                               \

#define variable_template_instantiation(T)                                     \
    template std::unique_ptr<typename core::Variable<T>::Info>                 \
    GetDeserializedMetadata(const core::Variable<T> &variable,                 \
                            gpointer buffer);                                  \
    template void SetVariableBlockInfo(                                        \
        core::Variable<T> &variable,                                           \
        typename core::Variable<T>::Info &blockInfo);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

// ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
// #undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
#endif /* ADIOS2_ENGINE_JULEAFORMATREADER_ */
