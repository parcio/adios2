/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATWRITER_
#define ADIOS2_ENGINE_JULEAFORMATWRITER_

#include "JuleaFormatWriter.h"
#include "JuleaKVWriter.h"

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
gpointer SerializeVariableMetadata(Variable<T> &variable, guint32 &len,
                                   size_t currStep)
{
    bool constantDims = variable.IsConstantDims();
    bool isReadAsJoined = variable.m_ReadAsJoined;
    bool isReadAsLocalValue = variable.m_ReadAsLocalValue;
    bool isRandomAccess = variable.m_RandomAccess;
    int shapeID = (int)variable.m_ShapeID;

    const char *type = variable.m_Type.c_str();
    size_t shapeSize = variable.m_Shape.size();
    size_t startSize = variable.m_Start.size();
    size_t countSize = variable.m_Count.size();
    size_t numberSteps = currStep + 1;

    size_t shapeIDLen = sizeof(int);
    size_t typeLen = sizeof(variable.m_Type.c_str());

    size_t shapeLen = shapeSize * sizeof(size_t);
    size_t startLen = startSize * sizeof(size_t);
    size_t countLen = countSize * sizeof(size_t);
    size_t blocksLen = numberSteps * sizeof(size_t);

    uint numberVectors = 5; // type + shape + start + count + blocks
    uint numberBools =
        4; // constantDims + readAsJoined + readAsLocalValue + randomAccess

    len = numberVectors * sizeof(size_t) + typeLen + shapeLen + startLen +
          countLen + blocksLen + numberBools * sizeof(bool) + shapeIDLen;
    std::cout << "--- variable metadata buffer length: " << len << std::endl;

    char *buffer = (char *)g_slice_alloc(len);

    size_t blocks[numberSteps];
    for (uint i = 0; i < numberSteps; i++)
    {
        // std::cout << "--- DEBUG ---" << std::endl;
        blocks[i] = variable.m_AvailableStepBlockIndexOffsets[i].size();
        // std::cout << "i: " << i << "  blocks: " << blocks[i] << std::endl;
    }

    memcpy(buffer, &constantDims, sizeof(bool));
    buffer += sizeof(bool);

    memcpy(buffer, &isReadAsJoined, sizeof(bool));
    buffer += sizeof(bool);

    memcpy(buffer, &isReadAsLocalValue, sizeof(bool));
    buffer += sizeof(bool);

    memcpy(buffer, &isRandomAccess, sizeof(bool));
    buffer += sizeof(bool);

    /** allocate memory for variable holding the length of the vector +
    memory for the vector data itself */
    memcpy(buffer, &typeLen, sizeof(size_t)); // type
    buffer += sizeof(size_t);
    memcpy(buffer, type, typeLen);
    buffer += typeLen;

    /** --- shapeID --- */
    memcpy(buffer, &shapeID, shapeIDLen);
    buffer += shapeIDLen;

    /** --- shape ---*/
    memcpy(buffer, &shapeSize, sizeof(size_t));
    buffer += sizeof(size_t);
    size_t shapeBuffer[shapeSize];
    for (uint i = 0; i < shapeSize; i++)
    {
        shapeBuffer[i] = variable.m_Shape.data()[i];
    }
    memcpy(buffer, shapeBuffer, shapeLen);
    buffer += shapeLen;

    /** ---start --- */
    memcpy(buffer, &startSize, sizeof(size_t));
    buffer += sizeof(size_t);
    size_t startBuffer[startSize];
    for (uint i = 0; i < startSize; i++)
    {
        startBuffer[i] = variable.m_Start.data()[i];
    }

    memcpy(buffer, startBuffer, startLen);
    buffer += startLen;

    /** --- count --- */
    memcpy(buffer, &countSize, sizeof(size_t));
    buffer += sizeof(size_t);

    size_t countBuffer[countSize];
    for (uint i = 0; i < countSize; i++)
    {
        countBuffer[i] = variable.m_Count.data()[i];
    }
    memcpy(buffer, countBuffer, countLen);
    buffer += countLen;

    /** --- blocks --- */
    memcpy(buffer, &numberSteps, sizeof(size_t)); // blocks
    buffer += sizeof(size_t);
    memcpy(buffer, blocks, blocksLen);

    // rewind buffer
    buffer -= len - blocksLen;

    // std::cout << "typeLen: " << typeLen << std::endl;
    // std::cout << "variable.m_ShapeID: " << variable.m_ShapeID << std::endl;
    // std::cout << "shapeID: " << shapeID << std::endl;
    // std::cout << "constantDims: " << constantDims << std::endl;
    // std::cout << "type: " << type << std::endl;
    // std::cout << "shapeSize: " << shapeSize << std::endl;
    // std::cout << "startSize: " << startSize << std::endl;
    // std::cout << "countSize: " << countSize << std::endl;
    // std::cout << "numberSteps: " << numberSteps << std::endl;
    // std::cout << "shape.data = " << variable.m_Shape.data() << std::endl;
    // std::cout << "count.data = " << variable.m_Count.data() << std::endl;
    // std::cout << "numberSteps: " << numberSteps << std::endl;

    return (gpointer)buffer;
}

template <class T>
gpointer SerializeBlockMetadata(Variable<T> &variable, guint32 &len,
                                size_t currStep, size_t block,
                                const typename Variable<T>::Info &blockInfo)
{
    std::cout << "--- SerializeBlockMetadata --- BlockID:" << block << std::endl;
    std::cout << "--- m_BlocksInfo.size(): " << variable.m_BlocksInfo.size()
              << std::endl;

    size_t shapeSize = blockInfo.Shape.size();
    size_t startSize = blockInfo.Start.size();
    size_t countSize = blockInfo.Count.size();
    size_t memoryStartSize = blockInfo.MemoryStart.size();
    size_t memoryCountSize = blockInfo.MemoryCount.size();

    size_t shapeLen = shapeSize * sizeof(size_t);
    size_t startLen = startSize * sizeof(size_t);
    size_t countLen = countSize * sizeof(size_t);
    size_t memoryStartLen = memoryStartSize * sizeof(size_t);
    size_t memoryCountLen = memoryCountSize * sizeof(size_t);

    size_t minLen = sizeof(variable.m_Min);
    size_t maxLen = sizeof(variable.m_Max);
    size_t valueLen = sizeof(variable.m_Value);

    size_t stepsStart = blockInfo.StepsStart;
    size_t stepsCount = blockInfo.StepsCount;
    size_t blockID = block;

    bool isValue = blockInfo.IsValue;

    std::cout << "--- variable minimum: " << variable.m_Min << std::endl;
    std::cout << "--- variable maximum: " << variable.m_Max << std::endl;
    // std::cout << "variable min size: " << minLen << std::endl;
    // std::cout << "size of T: " << sizeof(T) << std::endl;

    // shape + start + count + memoryStart + memoryCount
    uint numberVectors = 5;
    // StepsStart + StepsCount + BlockID
    uint numberVariables = 3;
    // SingleValue
    uint numberBools = 1;

    // calculating buffer size
    len = numberVectors * sizeof(size_t) + shapeLen + startLen + countLen +
          memoryStartLen + memoryCountLen + numberVariables * sizeof(size_t) +
          numberBools * sizeof(bool) + minLen + maxLen + valueLen;
    std::cout << "--- block metadata buffer length: " << len << std::endl;

    char *buffer = (char *)g_slice_alloc(len);

    /** --- shape ---*/
    memcpy(buffer, &shapeSize, sizeof(size_t));
    // std::cout << "shapeSize: " << shapeSize << std::endl;
    buffer += sizeof(size_t);

    size_t shapeBuffer[shapeSize];
    for (uint i = 0; i < shapeSize; i++)
    {
        shapeBuffer[i] = blockInfo.Shape.data()[i];
    }
    memcpy(buffer, shapeBuffer, shapeLen);
    buffer += shapeLen;

    // std::cout << "shapeLen:" << shapeLen << std::endl;
    // std::cout << "var: shape.data: " << variable.m_Shape.data() << std::endl;
    // std::cout << "blockInfo:shape.data: " << variable.m_Shape.data()
    //           << std::endl;

    /** ---start --- */
    memcpy(buffer, &startSize, sizeof(size_t));
    buffer += sizeof(size_t);

    size_t startBuffer[startSize];
    for (uint i = 0; i < startSize; i++)
    {
        startBuffer[i] = blockInfo.Start.data()[i];
    }

    memcpy(buffer, startBuffer, startLen);
    buffer += startLen;

    /** --- count --- */
    memcpy(buffer, &countSize, sizeof(size_t));
    buffer += sizeof(size_t);

    size_t countBuffer[countSize];
    for (uint i = 0; i < countSize; i++)
    {
        countBuffer[i] = blockInfo.Count.data()[i];
    }
    memcpy(buffer, countBuffer, countLen);
    buffer += countLen;

    /** ---memorystart --- */
    memcpy(buffer, &memoryStartSize, sizeof(size_t));
    buffer += sizeof(size_t);

    size_t memoryStartBuffer[memoryStartSize];
    for (uint i = 0; i < memoryStartSize; i++)
    {
        memoryStartBuffer[i] = blockInfo.MemoryStart.data()[i];
    }

    memcpy(buffer, memoryStartBuffer, memoryStartLen);
    buffer += memoryStartLen;

    /** ---memorycount --- */
    memcpy(buffer, &memoryCountSize, sizeof(size_t));
    buffer += sizeof(size_t);

    size_t memoryCountBuffer[memoryCountSize];
    for (uint i = 0; i < memoryCountSize; i++)
    {
        memoryCountBuffer[i] = blockInfo.MemoryCount.data()[i];
    }

    memcpy(buffer, memoryCountBuffer, memoryCountLen);
    buffer += memoryCountLen;

    /** --- no more vectors from here on --- */
    memcpy(buffer, &variable.m_Min, minLen); // Min
    buffer += minLen;

    memcpy(buffer, &variable.m_Max, maxLen); // Max
    buffer += maxLen;

    memcpy(buffer, &variable.m_Value, valueLen); // Value
    buffer += valueLen;

    memcpy(buffer, &stepsStart, sizeof(size_t)); // stepsStart
    buffer += sizeof(size_t);
    // std::cout << "stepsStart" << stepsStart << std::endl;
    // std::cout << "stepsCount" << stepsCount << std::endl;

    memcpy(buffer, &stepsCount, sizeof(size_t)); // stepsCount
    buffer += sizeof(size_t);

    memcpy(buffer, &blockID, sizeof(size_t)); // blockID
    buffer += sizeof(size_t);
    // std::cout << "blockID: " << blockID << std::endl;

    memcpy(buffer, &isValue, sizeof(bool)); // isValue

    // rewind buffer
    buffer -= len - sizeof(bool);
    return (gpointer)buffer;
}

template <class T>
void SetMinMax(Variable<T> &variable, const T *data)
{
    T min;
    T max;

    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    adios2::helper::GetMinMax(data, number_elements, min, max);
    variable.m_Min = min;
    variable.m_Max = max;
}

template <class T>
void ParseAttributeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata)
{
    // std::cout << "-- ParseAttributeToBSON ------ " << std::endl;
    unsigned int dataSize = 0;

    bson_append_int64(bsonMetadata, "number_elements", -1,
                      attribute.m_Elements);
    bson_append_bool(bsonMetadata, "is_single_value", -1,
                     attribute.m_IsSingleValue);
    if (attribute.m_IsSingleValue)
    {
        dataSize = sizeof(attribute.m_DataSingleValue);
        // std::cout << "-- dataSize single value = " << dataSize << std::endl;
    }
    else
    {
        dataSize = attribute.m_DataArray.size() * sizeof(T);
    }

    bson_append_int64(bsonMetadata, "complete_data_size", -1, dataSize);
    // std::cout << "-- bsonMetadata length: " << bsonMetadata->len <<
    // std::endl;
}

template <>
void ParseAttributeToBSON<std::string>(Attribute<std::string> &attribute,
                                       bson_t *bsonMetadata)
{
    std::cout << "-- ParseAttributeToBSON ------ " << std::endl;
    unsigned int completeSize = 0;
    unsigned int entrySize = 0;
    gchar *key;

    bson_append_int64(bsonMetadata, "number_elements", -1,
                      attribute.m_Elements); // TODO: still needed?
    bson_append_bool(bsonMetadata, "is_single_value", -1,
                     attribute.m_IsSingleValue);
    if (attribute.m_IsSingleValue)
    {
        completeSize = attribute.m_DataSingleValue.length() + 1;
        // std::cout << "-- dataSize single value = " << completeSize <<
        // std::endl;
    }
    else
    {
        // bson_append_int64(bsonMetadata, "array_size", -1,
        //                   attribute.m_DataArray.size());
        for (size_t i = 0; i < attribute.m_DataArray.size(); ++i)
        {
            entrySize = attribute.m_DataArray.data()[i].length() + 1;
            // std::cout << "entry_size_: " << entrySize << std::endl;

            completeSize = completeSize + entrySize;
            // std::cout << "complete_data_size: " << completeSize << std::endl;
            key = g_strdup_printf("entry_size_%d", i);
            // std::cout << "key: " << key << std::endl;
            bson_append_int64(bsonMetadata, key, -1, entrySize);
            g_free(key);
        }
    }
    bson_append_int64(bsonMetadata, "complete_data_size", -1, completeSize);
    // std::cout << "-- complete_data_size: " << completeSize << std::endl;
    // std::cout << "-- bsonMetadata length: " << bsonMetadata->len <<
    // std::endl;
}

template <class T>
void ParseAttrTypeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata)
{
    // std::cout << "-- ParseAttrTypeToBSON ------" << std::endl;
    int type = -1;

    if (helper::GetType<T>() == "string")
    {
        type = STRING;
    }
    else if (helper::GetType<T>() == "int8_t")
    {
        type = INT8;
    }
    else if (helper::GetType<T>() == "uint8_t")
    {
        type = UINT8;
    }
    else if (helper::GetType<T>() == "int16_t")
    {
        type = INT16;
    }
    else if (helper::GetType<T>() == "uint16_t")
    {
        type = UINT16;
    }
    else if (helper::GetType<T>() == "int32_t")
    {
        type = INT32;
    }
    else if (helper::GetType<T>() == "uint32_t")
    {
        type = UINT32;
    }
    else if (helper::GetType<T>() == "int64_t")
    {
        type = INT64;
    }
    else if (helper::GetType<T>() == "uint64_t")
    {
        type = UINT64;
    }
    else if (helper::GetType<T>() == "float")
    {
        type = FLOAT;
    }
    else if (helper::GetType<T>() == "double")
    {
        type = DOUBLE;
    }
    else if (helper::GetType<T>() == "long double")
    {
        type = LONG_DOUBLE;
    }

    bson_append_int32(bsonMetadata, "attr_type", -1, type);
    // std::cout << "-- bsonMetadata length: " << bsonMetadata->len <<
    // std::endl; std::cout << "-- type: " << attribute.m_Type << std::endl;
}

#define variable_template_instantiation(T)                                     \
    template void SetMinMax(Variable<T> &variable, const T *data);             \
    template gpointer SerializeVariableMetadata(                               \
        Variable<T> &variable, guint32 &buffer_len, size_t step);              \
    template gpointer SerializeBlockMetadata(                                  \
        Variable<T> &variable, guint32 &buffer_len, size_t step, size_t block, \
        const typename Variable<T>::Info &blockInfo);

ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

#define attribute_template_instantiation(T)                                    \
    template void ParseAttributeToBSON(Attribute<T> &attribute,                \
                                       bson_t *bsonMetadata);                  \
    template void ParseAttrTypeToBSON(Attribute<T> &attribute,                 \
                                      bson_t *bsonMetadata);

ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
#endif /* ADIOS_ENGINE_JULEAFORMATWRITER_ */
