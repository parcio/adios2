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
void SetMetadata(Variable<T> &variable, JuleaKVWriter::Metadata<T> &md,
                 size_t step, size_t block)
{
    std::cout << "DEBUG ---- SetMetadata ---- " << std::endl;

    md.Shape = variable.m_Shape;
    md.Start = variable.m_Start;
    md.Count = variable.m_Count;

    md.MemoryStart = variable.m_MemoryStart;
    md.MemoryCount = variable.m_MemoryCount;
    md.Operations = variable.m_Operations;

    // std::vector<Operation> Operations;
    // std::vector<T> Values;
    // std::vector<T> MinMaxs; // sub-block level min-max

    // md.Step = variable.m_Step;
    md.StepsStart = variable.m_StepsStart;
    md.StepsCount = variable.m_StepsCount;
    md.BlockID = variable.m_BlockID;

    md.CurrentStep = step;
    md.BlockNumber = block;

    md.Min = variable.m_Min;
    md.Max = variable.m_Max;
    // md.Value = variable.m_Value;

    // md.WriterID = variable.m_BlocksInfo[step].WriterID;

    md.IsReadAsJoined = variable.m_ReadAsJoined;
    md.IsReadAsLocalValue = variable.m_ReadAsLocalValue;
    md.IsRandomAccess = variable.m_RandomAccess;

    md.IsValue = variable.m_SingleValue;
    // md.IsReverseDims = variable.m_BlocksInfo[step].IsReverseDims;
}

// template <class T>
// JuleaKVWriter::Metadata<T> *GetMetadataBuffer(Variable<T> &variable)
// {
//     // JuleaKVWriter::Metadata
//     return NULL;
// }

template <>
gpointer GetMetadataBuffer(Variable<std::string> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<std::string> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<int8_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<int8_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<uint8_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<uint8_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<int16_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<int16_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
    ;
}

template <>
gpointer GetMetadataBuffer(Variable<uint16_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<uint16_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<int32_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<int32_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<uint32_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<uint32_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<int64_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<int64_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<uint64_t> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<uint64_t> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<float> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<float> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<double> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<double> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<long double> &variable, guint32 &buffer_len,
                           size_t step, size_t block)
{
    JuleaKVWriter::Metadata<long double> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<std::complex<float>> &variable,
                           guint32 &buffer_len, size_t step, size_t block)
{
    JuleaKVWriter::Metadata<std::complex<float>> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
}

template <>
gpointer GetMetadataBuffer(Variable<std::complex<double>> &variable,
                           guint32 &buffer_len, size_t step, size_t block)
{
    JuleaKVWriter::Metadata<std::complex<double>> md;
    void *buf = NULL;
    buffer_len = sizeof(md);

    std::cout << "DEBUG ---- GetMetadataBuffer ---- type: " << variable.m_Type
              << std::endl;
    std::cout << "buffer_len: " << buffer_len << std::endl;

    SetMetadata(variable, md, step, block);
    std::cout << "min =  " << md.Min << std::endl;

    buf = g_memdup(&md, buffer_len);
    return buf;
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
    template gpointer GetMetadataBuffer(Variable<T> &variable,                 \
                                        guint32 &buffer_len, size_t step,      \
                                        size_t block);

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
