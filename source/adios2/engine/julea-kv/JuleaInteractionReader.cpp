/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaInteractionReader.h"
#include "JuleaMetadata.h"

#include <julea-config.h>

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
#include <julea-internal.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

/** -------------------------------------------------------------------------**/
/** -------------- TESTING GENERIC FUNCTIONS --------------------------------**/
/** -------------------------------------------------------------------------**/
// template <class T>
// void GetAllVariableNamesFromJulea(Variable<T> &variable, bson_t
// *bsonMetadata,
//                                  std::string nameSpace)

// void ExtractVariableFromBSON(const std::string nameSpace, const std::string
// varName, bson_t *bsonMetadata, int type, Dims shape, Dims start, Dims
// count,bool constantDims)
// {

// }

// void GetVariableBSONFromJulea(const std::string nameSpace, const std::string
// varName, bson_t *bsonMetadata)
// {
// 	// JBatch *batch;
//     // gchar *string_metadata_kv;
//     // gchar *key;
//     // bson_t* bson_metadata;
//     // bson_t bson_metadata;
//     bson_iter_t b_iter;
//     guint32 value_len = 0;

//     // g_autoptr(JKV) kv_object = NULL;
//     void *meta_data_buf = NULL;
//     auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
//     auto batch = j_batch_new(semantics);

//     auto string_metadata_kv = g_strdup_printf("variables_%s",
//     nameSpace.c_str()); auto kv_object = j_kv_new(string_metadata_kv,
//     varName.c_str());
//     // bson_metadata = bson_new();

//     j_kv_get(kv_object, &meta_data_buf, &value_len, batch);
//     j_batch_execute(batch);

//     if (value_len == 0)
//     {
//         // bson_names = bson_new();
//         printf("WARNING: The variable key-value store is empty! \n");
//     }
//     else
//     {
//         bson_init_static(bsonMetadata, (uint8_t *)meta_data_buf, value_len);
//     }

//     // bson_iter_init(&b_iter, bson_metadata);
//     // if(bson_iter_init(&b_iter, bson_metadata))
//     if (bson_iter_init(&b_iter, bsonMetadata))
//     {
//         std::cout << "++ Julea Client Logic: Bson iterator is valid"
//                   << std::endl;
//     }
//     else
//     {
//         std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
//     }

// }

void GetNamesBSONFromJulea(const std::string nameSpace, bson_t *bsonNames,
                           unsigned int *varCount)
{
    // bson_t *bson_names;
    // bson_iter_t b_iter;
    guint32 value_len = 0;

    // g_autoptr(JKV) kv_object = NULL;
    void *names_buf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    // printf("-- JADIOS DEBUG PRINT: get_all_var_names_from_kv \n");

    auto kv_object = j_kv_new("variable_names", nameSpace.c_str());

    j_kv_get(kv_object, &names_buf, &value_len, batch);
    j_batch_execute(batch);

    if (value_len == 0)
    {
        // bsonNames = bson_new();
        printf("WARNING: The names key-value store is empty! \n");
        *varCount = 0;
    }
    else
    {
        // bson_names = bson_new_from_data((const uint8_t *)names_buf,
        // value_len);
        bsonNames = bson_new_from_data((const uint8_t *)names_buf, value_len);
    }

    *varCount = bson_count_keys(bsonNames);
    printf("-- JADIOS DEBUG PRINT: count_names %d\n", *varCount);

    // *names = (char **)g_slice_alloc(*count_names * sizeof(char *));
    // *types = (int *)g_slice_alloc(*count_names * sizeof(int));
    // bson_iter_init(&b_iter, bsonNames);

    // for (unsigned int i = 0; i < *count_names; i++)
    // {
    //     if (!bson_iter_next(&b_iter))
    //     {
    //         printf("ERROR: count of names does not match \n");
    //     }
    //     (*names)[i] = g_strdup(bson_iter_key(&b_iter));
    //     (*types)[i] = bson_iter_int32(&b_iter);
    //     // printf("-- JADIOS DEBUG PRINT: get_all_var_names_from_kv DEBUG
    //     PRINT:
    //     // %s\n", (*names)[i]); printf("-- JADIOS DEBUG PRINT: types DEBUG
    //     // PRINT: %d\n", (*types)[i]);
    // }
    // if (value_len > 0)
    // {
    //     bson_destroy(bson_names);
    // }
    j_kv_unref(kv_object);
    j_batch_unref(batch);
}

void GetVariableBSONFromJulea(const std::string nameSpace,
                              const std::string varName, bson_t *bsonMetadata)
{
    // JBatch *batch;
    // gchar *string_metadata_kv;
    // gchar *key;
    // bson_t* bson_metadata;
    // bson_t bson_metadata;
    // bson_iter_t b_iter;
    guint32 value_len = 0;

    // g_autoptr(JKV) kv_object = NULL;
    void *meta_data_buf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto string_metadata_kv =
        g_strdup_printf("variables_%s", nameSpace.c_str());
    auto kv_object = j_kv_new(string_metadata_kv, varName.c_str());
    // bson_metadata = bson_new();

    j_kv_get(kv_object, &meta_data_buf, &value_len, batch);
    j_batch_execute(batch);

    if (value_len == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The variable key-value store is empty! \n");
    }
    else
    {
        bson_init_static(bsonMetadata, (uint8_t *)meta_data_buf, value_len);
    }
}

template <class T>
void GetVariableMetadataFromJulea(Variable<T> &variable, bson_t *bsonMetadata,
                                  const std::string nameSpace)
{
	auto bsonNames = bson_new();
    // auto bsonMetadata = bson_new();
    // auto nameSpace = m_JuleaInfo->nameSpace;
    unsigned int varCount = 0;
    int type;
    Dims shape;
    Dims start;
    Dims count;
    bool constantDims;

    // GetVariableMetadataFromJulea();
    // GetVariableDataFromJulea();

    // GetNamesBSONFromJulea(nameSpace, bsonNames, &varCount);

    // GetVariableBSONFromJulea(nameSpace,variable.m_Name, bsonMetadata);
}

/** ------------------------- DATA ------------------------------------------**/

#define declare_template_instantiation(T)                                      \
    template void GetVariableDataFromJulea(                                    \
        Variable<T> &variable, const T *data, const std::string nameSpace);    \
    template void GetVariableMetadataFromJulea(Variable<T> &variable,          \
                                               bson_t *bsonMetadata,           \
                                               const std::string nameSpace);   \
    template void GetAllAttributeNamesFromJulea(                               \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    template void GetAttributeDataFromJulea(                                   \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    template void GetAttributeMetadataFromJulea(Attribute<T> &attribute,       \
                                                bson_t *bsonMetadata,          \
                                                const std::string nameSpace);  \
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
