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
// #include "JuleaKVReader.h"

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

void GetVariableMetadataForInitFromBSON(const std::string nameSpace,
                                        const std::string varName,
                                        bson_t *bsonMetadata, int *type,
                                        Dims *shape, Dims *start, Dims *count,
                                        bool *constantDims)
{
    bson_iter_t b_iter;
    gchar *key;
    std::cout << "------- GetVariableMetadataForInitFromBSON -----------" << std::endl;
    if (bson_iter_init(&b_iter, bsonMetadata))
    {
        std::cout << "++ Julea Format Reader: Bson iterator is valid"
                  << std::endl;
    }
    else
    {
        std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    }

    /* probably not very efficient */
    while (bson_iter_next(&b_iter))
    {
        if (g_strcmp0(bson_iter_key(&b_iter), "shape_size") == 0)
        {
            auto shapeSize = bson_iter_int64(&b_iter);
            std::cout << "-- JADIOS DEBUG PRINT: shapeSize = " << shapeSize << std::endl;
            if (shapeSize > 0)
            {
                for (guint i = 0; i < shapeSize; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("shape_%d", i);

                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        (*shape).push_back(bson_iter_int64(&b_iter));

                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "start_size") == 0)
        {
            auto startSize = bson_iter_int64(&b_iter);
            std::cout << "-- JADIOS DEBUG PRINT: startSize = " << startSize << std::endl;

            if (startSize > 0)
            {
                for (guint i = 0; i < startSize; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("start_%d", i);

                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        (*start).push_back(bson_iter_int64(&b_iter));
                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "count_size") == 0)
        {
            auto countSize = bson_iter_int64(&b_iter);

            if (countSize > 0)
            {

                for (guint i = 0; i < countSize; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("count_%d", i);
                    std::cout << "-- key = " << key << std::endl;

                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        (*count).push_back (bson_iter_int64(&b_iter));
                        std::cout << "-- Test in for loop: count[i] = " <<  bson_iter_int64(&b_iter) << std::endl;
                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "var_type") == 0)
        {
                *type = bson_iter_int32(&b_iter);
                std::cout << "-- TYPE = " << bson_iter_int32(&b_iter) << std::endl;
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_constant_dims") == 0)
        {
                *constantDims = bson_iter_bool(&b_iter);
        }
    } // end while
}

template <class T>
void ParseVariableFromBSON(Variable<T> &variable, bson_t *bsonMetadata,
                           const std::string nameSpace)
{
    bson_iter_t b_iter;
    gchar *key;
    unsigned int size;

    if (bson_iter_init(&b_iter, bsonMetadata))
    {
        std::cout << "++ Julea Format Reader: Bson iterator is valid"
                  << std::endl;
    }
    else
    {
        std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    }

    //TODO: what to do with the of the keys? max_value etc

    /* probably not very efficient */
    while (bson_iter_next(&b_iter))
    {

        if (g_strcmp0(bson_iter_key(&b_iter) , "memory_start_size") == 0)
        {
            size = bson_iter_int64(&b_iter);

            if (size > 0)
            {
                for (guint i = 0; i < size; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("memory_start_%d", i);
                    if (g_strcmp0(bson_iter_key(&b_iter) , key) == 0)
                    {
                        variable.m_MemoryStart[i] = bson_iter_int64(&b_iter);
                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "memory_count_size") == 0)
        {
            size = bson_iter_int64(&b_iter);

            if (size > 0)
            {
                for (guint i = 0; i < size; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("memory_count_%d", i);
                    if (g_strcmp0(bson_iter_key(&b_iter) , key) == 0)
                    {
                        variable.m_MemoryCount[i] = bson_iter_int64(&b_iter);
                    }
                }
            }
        }
        /* unsigned long */
        else if (g_strcmp0(bson_iter_key(&b_iter) , "steps_start") == 0)
        {
            variable.m_StepsStart = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "steps_count") == 0)
        {
            variable.m_StepsCount = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "block_id") == 0)
        {
            variable.m_BlockID = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "index_start") == 0)
        {
            variable.m_IndexStart = bson_iter_int64(&b_iter);
        }
        // else if (g_strcmp0(bson_iter_key(&b_iter) , "element_size" == 0))
        // {
        //     variable.m_ElementSize = bson_iter_int64(&b_iter); //TODO
        //     elementSize read only?!
        // }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "available_steps_start") == 0)
        {
            variable.m_AvailableStepsStart = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "available_steps_count") == 0)
        {
            variable.m_AvailableStepsCount = bson_iter_int64(&b_iter);
        }
        /* boolean */
        // else if (g_strcmp0(bson_iter_key(&b_iter) , "is_value" == 0))
        // {
        //     variable.m_is_value = (bool)bson_iter_bool(&b_iter);
        // }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "is_single_value") == 0)
        {
            variable.m_SingleValue = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "is_constant_dims") == 0)
        {
            bool constantDims = (bool)bson_iter_bool(&b_iter);

            if (constantDims)
            {
                variable.SetConstantDims();
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "is_read_as_joined") == 0)
        {
            variable.m_ReadAsJoined = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "is_read_as_local_value") == 0)
        {
            variable.m_ReadAsLocalValue = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "is_random_access") == 0)
        {
            variable.m_RandomAccess = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter) , "is_first_streaming_step") == 0)
        {
            variable.m_FirstStreamingStep = (bool)bson_iter_bool(&b_iter);
        }
        /* value_type*/
        else if (g_strcmp0(bson_iter_key(&b_iter) , "min_value") == 0)
        {
            // if (variable.m_var_type == INT32)
            // if (variable.GetType() == INT32)
            // {
            // variable.m_Min = bson_iter_int32(&b_iter);
            variable.m_Min = 42;
            // }
            // else if (variable.m_var_type == INT64)
            // {
            //     variable.m_min_value.integer_64 = bson_iter_int64(&b_iter);
            // }
            // // else if(variable.m_var_type == UNSIGNED_LONG_LONG_INT) //FIXME
            // // data type
            // // {
            // //  bson_iter_decimal128(&b_iter, (bson_decimal128_t*)
            // // variable.m_min_value.ull_integer);
            // // }
            // else if (variable.m_var_type == FLOAT)
            // {
            //     variable.m_min_value.real_float = bson_iter_double(&b_iter);
            // }
            // else if (variable.m_var_type == DOUBLE)
            // {
            //     variable.m_min_value.real_double = bson_iter_double(&b_iter);
            // }
        }
        // else if (g_strcmp0(bson_iter_key(&b_iter) , "max_value" == 0))
        // {
        //     if (variable.m_var_type == INT32)
        //     {
        //         variable.m_max_value.integer_32 = bson_iter_int32(&b_iter);
        //     }
        //     else if (variable.m_var_type == INT64)
        //     {
        //         variable.m_max_value.integer_64 = bson_iter_int64(&b_iter);
        //     }
        //     // else if(variable.m_var_type == UNSIGNED_LONG_LONG_INT)
        //     // {
        //     //  bson_iter_decimal128(&b_iter,(bson_decimal128_t*)
        //     // variable.m_max_value.ull_integer);
        //     // }
        //     else if (variable.m_var_type == FLOAT)
        //     {
        //         variable.m_max_value.real_float =
        //             (float)bson_iter_double(&b_iter);
        //     }
        //     else if (variable.m_var_type == DOUBLE)
        //     {
        //         variable.m_max_value.real_double = bson_iter_double(&b_iter);
        //     }
        // }
        // else if (g_strcmp0(bson_iter_key(&b_iter) , "curr_value" == 0))
        // {
        //     if (variable.m_var_type == INT32)
        //     {
        //         variable.m_curr_value.integer_32 = bson_iter_int32(&b_iter);
        //     }
        //     else if (variable.m_var_type == INT64)
        //     {
        //         variable.m_curr_value.integer_64 = bson_iter_int64(&b_iter);
        //     }
        //     // else if(variable.m_var_type == UNSIGNED_LONG_LONG_INT)
        //     // {
        //     //  bson_iter_decimal128(&b_iter,(bson_decimal128_t*)
        //     // variable.m_curr_value.ull_integer);
        //     // }
        //     else if (variable.m_var_type == FLOAT)
        //     {
        //         variable.m_curr_value.real_float =
        //             (float)bson_iter_double(&b_iter);
        //     }
        //     else if (variable.m_var_type == DOUBLE)
        //     {
        //         variable.m_curr_value.real_double =
        //         bson_iter_double(&b_iter);
        //     }
        // }
        // else
        // {
        //     std::cout << "Unknown key " << bson_iter_key(&b_iter)
        //               << " when retrieving metadata for variable "
        //               << variable.m_Name << std::endl;
        // }

    } // end while
}

// template <class T>
// void SetMinMax(Variable<T> &var)
// {
//     T min;
//     T max;

//     auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
//     adios2::helper::GetMinMax(data, number_elements, min, max);
//     variable.m_Min = min;
//     variable.m_Max = max;
// }

// template <class T>
// void ParseAttributeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata)
// {
//     // name is key in kv
//     unsigned int dataSize = -1;

//     bson_append_int64(bsonMetadata, "number_elements", -1,
//                       attribute.m_Elements);
//     bson_append_bool(bsonMetadata, "is_single_value", -1,
//                      attribute.m_IsSingleValue);
//     if (attribute.m_IsSingleValue)
//     {
//         // TODO: check if this is correct
//         dataSize = sizeof(attribute.m_DataSingleValue);
//     }
//     else
//     {
//         dataSize = attribute.m_DataArray.size();
//     }

//     bson_append_int64(bsonMetadata, "data_size", -1, dataSize);
// }

template <>
void ParseVarTypeFromBSON<std::string>(Variable<std::string> &variable,
                                       bson_iter_t *b_iter)
{
    // FIXME: set min, max for string?
    // bson_append_int32(bsonMetadata, "var_type", -1, STRING);
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);

    std::cout << "ParseVarTypeFromBSON String: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<int8_t>(Variable<int8_t> &variable,
                                  bson_iter_t *b_iter)
{
    variable.m_Min = bson_iter_int32(b_iter);
    // bson_append_int32(bsonMetadata, "var_type", -1, INT8);
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);

    std::cout << "ParseVarTypeFromBSON int8_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<uint8_t>(Variable<uint8_t> &variable,
                                   bson_iter_t *b_iter)
{
    // bson_append_int32(bsonMetadata, "var_type", -1, UINT8);
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);

    std::cout << "ParseVarTypeFromBSON uint8_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<int16_t>(Variable<int16_t> &variable,
                                   bson_iter_t *b_iter)
{
    // bson_append_int32(bsonMetadata, "var_type", -1, INT16);
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON int16_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<uint16_t>(Variable<uint16_t> &variable,
                                    bson_iter_t *b_iter)
{
    // bson_append_int32(bsonMetadata, "var_type", -1, UINT16);
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON uint16_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<int32_t>(Variable<int32_t> &variable,
                                   bson_iter_t *b_iter)
{
    // bson_append_int32(bsonMetadata, "var_type", -1, INT32);
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON int32_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<uint32_t>(Variable<uint32_t> &variable,
                                    bson_iter_t *b_iter)
{
    // bson_append_int32(bsonMetadata, "var_type", -1,
    //                   UINT32); // FIXME: does int32 suffice?
    // bson_append_int32(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int32(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int32(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON uint32_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<int64_t>(Variable<int64_t> &variable,
                                   bson_iter_t *b_iter)
{
    // bson_append_int64(bsonMetadata, "var_type", -1, INT64);
    // bson_append_int64(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int64(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int64(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON int64_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<uint64_t>(Variable<uint64_t> &variable,
                                    bson_iter_t *b_iter)
{
    // bson_append_int64(bsonMetadata, "var_type", -1, UINT64);
    // bson_append_int64(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_int64(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_int64(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON uint64_t: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<float>(Variable<float> &variable,
                                  bson_iter_t *b_iter)
{
    // bson_append_double(bsonMetadata, "var_type", -1, FLOAT);
    // bson_append_double(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_double(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_double(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON float: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<double>(Variable<double> &variable,
                                  bson_iter_t *b_iter)
{
    // bson_append_double(bsonMetadata, "var_type", -1, DOUBLE);
    // bson_append_double(bsonMetadata, "min_value", -1, variable.Min());
    // bson_append_double(bsonMetadata, "max_value", -1, variable.Max());
    // bson_append_double(bsonMetadata, "curr_value", -1, variable.m_Value);
    std::cout << "ParseVarTypeFromBSON double: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<long double>(Variable<long double> &variable,
                                       bson_iter_t *b_iter)
{
    // FIXME: implement!
    // how to store long double in bson file?
    std::cout << "ParseVarTypeFromBSON long double: min = " << variable.Min()
              << std::endl;
}

template <>
void ParseVarTypeFromBSON<std::complex<float>>(
    Variable<std::complex<float>> &variable, bson_iter_t *b_iter)
{
    // FIXME: implement!
    // use two doubles? one for imaginary, one for real part?
    std::cout << "ParseVarTypeFromBSON std::complex<float>: min = "
              << variable.Min() << std::endl;
}

template <>
void ParseVarTypeFromBSON<std::complex<double>>(
    Variable<std::complex<double>> &variable, bson_iter_t *b_iter)
{
    // FIXME: implement!
    // use two doubles? one for imaginary, one for real part?
    std::cout << "ParseVarTypeFromBSON std::complex<double>: min = "
              << variable.Min() << std::endl;
}

// template void SetMinMax(Variable<T> &var);             \

// template <class T>
// void TESTGetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata, const std::string nameSpace)
// {
//     std::cout << "Trying to fix template issues while linking" << std::endl;
// }

    // template void TESTGetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata, const std::string nameSpace);\


#define variable_template_instantiation(T)                                     \
    template void ParseVariableFromBSON(core::Variable<T> &,                   \
                                        bson_t *bsonMetadata,                  \
                                        const std::string nameSpace);          \
    template void ParseVarTypeFromBSON(Variable<T> &variable,                  \
                                       bson_iter_t *b_iter); \

ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

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
#endif /* ADIOS2_ENGINE_JULEAFORMATREADER_ */
