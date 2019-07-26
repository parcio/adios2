/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 16, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaDBDBClientLogic.h"
#include "JuleaDBMetadata.h"

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

// void var_metadata_to_bson(Metadata* metadata, bson_t* bson_meta_data);
// void attr_metadata_to_bson(AttributeMetadata* attr_metadata, bson_t*
// bson_meta_data);

/**
 * Put metadata to passed bson file
 *
 * \param [r] metadata   		metadata to be stored
 * \param [r] bson_meta_data 	bson file of kv store
 */
void var_metadata_to_bson(Metadata *metadata, bson_t *bson_meta_data)
{
    gchar *key;
    // bson_append_int64(bson_meta_data, "shape_size", -1,
    // metadata->shape_size);
    g_assert_true(bson_append_int64(bson_meta_data, "shape_size", -1,
                                    metadata->shape_size));
    for (guint i = 0; i < metadata->shape_size; i++)
    {
        key = g_strdup_printf("shape_%d", i);
        g_assert_true(
            bson_append_int64(bson_meta_data, key, -1, metadata->shape[i]));
    }
    // std::cout << "var_metadata_to_bson: bson_meta_data->len "
    // << bson_meta_data->len << std::endl;

    g_assert_true(bson_append_int64(bson_meta_data, "start_size", -1,
                                    metadata->start_size));
    for (guint i = 0; i < metadata->start_size; i++)
    {
        key = g_strdup_printf("start_%d", i);
        g_assert_true(
            bson_append_int64(bson_meta_data, key, -1, metadata->start[i]));
    }

    g_assert_true(bson_append_int64(bson_meta_data, "count_size", -1,
                                    metadata->count_size));
    for (guint i = 0; i < metadata->count_size; i++)
    {
        key = g_strdup_printf("count_%d", i);
        g_assert_true(
            bson_append_int64(bson_meta_data, key, -1, metadata->count[i]));
    }

    g_assert_true(bson_append_int64(bson_meta_data, "memory_start_size", -1,
                                    metadata->memory_start_size));
    for (guint i = 0; i < metadata->memory_start_size; i++)
    {
        key = g_strdup_printf("memory_start_%d", i);
        g_assert_true(bson_append_int64(bson_meta_data, key, -1,
                                        metadata->memory_start[i]));
    }

    g_assert_true(bson_append_int64(bson_meta_data, "memory_count_size", -1,
                                    metadata->memory_count_size));
    for (guint i = 0; i < metadata->memory_count_size; i++)
    {
        key = g_strdup_printf("memory_count_%d", i);
        g_assert_true(bson_append_int64(bson_meta_data, key, -1,
                                        metadata->memory_count[i]));
    }

    g_assert_true(bson_append_int64(bson_meta_data, "steps_start", -1,
                                    metadata->steps_start));
    g_assert_true(bson_append_int64(bson_meta_data, "steps_count", -1,
                                    metadata->steps_count));
    g_assert_true(
        bson_append_int64(bson_meta_data, "block_id", -1, metadata->block_id));
    g_assert_true(bson_append_int64(bson_meta_data, "index_start", -1,
                                    metadata->index_start));
    g_assert_true(bson_append_int64(bson_meta_data, "element_size", -1,
                                    metadata->element_size));
    g_assert_true(bson_append_int64(bson_meta_data, "available_steps_start", -1,
                                    metadata->available_steps_start));
    g_assert_true(bson_append_int64(bson_meta_data, "available_steps_count", -1,
                                    metadata->available_steps_count));

    g_assert_true(
        bson_append_int64(bson_meta_data, "var_type", -1, metadata->var_type));

    g_assert_true(bson_append_int64(bson_meta_data, "data_size", -1,
                                    metadata->data_size));

    // g_assert_true(
    //     bson_append_bool(bson_meta_data, "is_value", -1,
    //     metadata->is_value));
    g_assert_true(bson_append_bool(bson_meta_data, "is_single_value", -1,
                                   metadata->is_single_value));
    g_assert_true(bson_append_bool(bson_meta_data, "is_single_value", -1,
                                   metadata->is_single_value));
    g_assert_true(bson_append_bool(bson_meta_data, "is_constant_dims", -1,
                                   metadata->is_constant_dims));
    g_assert_true(bson_append_bool(bson_meta_data, "is_read_as_joined", -1,
                                   metadata->is_read_as_joined));
    g_assert_true(bson_append_bool(bson_meta_data, "is_read_as_local_value", -1,
                                   metadata->is_read_as_local_value));
    g_assert_true(bson_append_bool(bson_meta_data, "is_random_access", -1,
                                   metadata->is_random_access));
    g_assert_true(bson_append_bool(bson_meta_data, "is_first_streaming_step",
                                   -1, metadata->is_first_streaming_step));

    /* now comes the part for "min_value" of type T in C++ */
    if (metadata->var_type == STRING) // FIXME data types
    {
        // TODO: implement
    }
    else if (metadata->var_type == INT32)
    {
        g_assert_true(bson_append_int32(bson_meta_data, "min_value", -1,
                                        metadata->min_value.integer_32));
        g_assert_true(bson_append_int32(bson_meta_data, "max_value", -1,
                                        metadata->max_value.integer_32));
        g_assert_true(bson_append_int32(bson_meta_data, "curr_value", -1,
                                        metadata->curr_value.integer_32));
    }
    // else if(metadata->var_type == SIGNED_CHAR)
    // {
    // 	//TODO: implement
    // }
    // else if(metadata->var_type == UNSIGNED_CHAR)
    // {
    // 	//TODO: implement
    // }
    // else if(metadata->var_type == SHORT)
    // {
    // 	//TODO: implement
    // }
    // else if(metadata->var_type == UNSIGNED_SHORT)
    // {
    // 	//TODO: implement
    // }
    else if (metadata->var_type == INT64)
    {
        g_assert_true(bson_append_int64(bson_meta_data, "min_value", -1,
                                        metadata->min_value.integer_64));
        g_assert_true(bson_append_int64(bson_meta_data, "max_value", -1,
                                        metadata->max_value.integer_64));
        g_assert_true(bson_append_int64(bson_meta_data, "curr_value", -1,
                                        metadata->curr_value.integer_64));
    }
    // else if(metadata->var_type == UNSIGNED_INT)
    // {
    // 	//TODO: implement
    // }
    // else if(metadata->var_type == LONG_INT)
    // {
    // 	//TODO: implement
    // }
    // else if(metadata->var_type == UNSIGNED_LONG_INT)
    // {
    // 	//TODO: implement
    // }
    // else if(metadata->var_type == LONG_LONG_INT){
    // 	//TODO: implement
    // }
    // else if(metadata->var_type == UNSIGNED_LONG_LONG_INT)
    // {
    // 	g_assert_true(bson_append_decimal128(bson_meta_data, "min_value", -1,
    // 		(void*) metadata->min_value.ull_integer));
    // 	g_assert_true(bson_append_decimal128(bson_meta_data, "max_value", -1,
    // 		(void*) metadata->max_value.ull_integer));
    // 	g_assert_true(bson_append_decimal128(bson_meta_data, "curr_value", -1,
    // 		(void*) metadata->curr_value.ull_integer));
    // }
    else if (metadata->var_type == FLOAT)
    {
        g_assert_true(bson_append_double(bson_meta_data, "min_value", -1,
                                         metadata->min_value.real_float));
        g_assert_true(bson_append_double(bson_meta_data, "max_value", -1,
                                         metadata->max_value.real_float));
        g_assert_true(bson_append_double(bson_meta_data, "curr_value", -1,
                                         metadata->curr_value.real_float));
    }
    else if (metadata->var_type == DOUBLE)
    {
        g_assert_true(bson_append_double(bson_meta_data, "min_value", -1,
                                         metadata->min_value.real_double));
        g_assert_true(bson_append_double(bson_meta_data, "max_value", -1,
                                         metadata->max_value.real_double));
        g_assert_true(bson_append_double(bson_meta_data, "curr_value", -1,
                                         metadata->curr_value.real_double));
    }
    else if (metadata->var_type == LONG_DOUBLE)
    {
        // TODO: implement
    }
    else if (metadata->var_type == COMPLEX_FLOAT)
    {
        // TODO: implement
    }
    else if (metadata->var_type == COMPLEX_DOUBLE)
    {
        // TODO: implement
    }
    g_free(key);
}

/**
 * Put attribute metadata to passed bson file
 *
 * \param [r] metadata   		metadata to be stored
 * \param [r] bson_meta_data 	bson file of kv store
 */
void attr_metadata_to_bson(AttributeMetadata *attr_metadata,
                           bson_t *bson_meta_data)
{
    g_assert_true(bson_append_int64(bson_meta_data, "var_type", -1,
                                    attr_metadata->attr_type));
    g_assert_true(bson_append_int64(bson_meta_data, "number_elements", -1,
                                    attr_metadata->number_elements));
    g_assert_true(bson_append_bool(bson_meta_data, "is_single_value", -1,
                                   attr_metadata->is_single_value));
}

/**
 * Init
 * TODO:
 *
 * \code
 * \endcode
 *
 * \param juleainfo
 **/
// void
// j_adios_init(JuleaDBInfo* julea_info)
// {
// 	printf("---* JuleaDB Adios Client: Init\n");

// 	//TODO:create scheme for db
// 	// julea_info->semantics = j_semantics_new (J_SEMANTICS_TEMPLATE_POSIX);
// }

// void
// j_adios_finish(void)
// {
// 	printf("YOU MANAGED TO GET TO J GMM FINISH :) WUHU \n");
// 	//PSEUDO create new kv
// 	//create new object store
// 	//DESIGN: additional parameters needed?
// }

/**
 * Put the data and the according metadata of an ADIOS2 variable. There is no
 * separate function for putting only the metadata. The data is stored in an
 * object store while the metadata is stored in the structured metadata backend
 * (SMD).
 *
 * \param [r] name_space   unique name of engine in m_IO
 * \param [r] metadata       metadata struct (stored in kv/smd)
 * \param [r] data_pointer 	data to be stored in object store
 * \param [r] batch 	 	batch to execute the operation in
 * \param [r] use_batch    	pass false when using deferred/asynchronous I/O;
 * true for synchronous I/O
 */
void PutVariableToJuleaDB(char *name_space, Metadata *metadata,
                        const void *data_pointer, JBatch *batch)
{
    guint64 bytes_written = 0;
    guint32 value_len = 0;

    bson_iter_t b_iter;
    bson_t *bson_names;

    void *names_buf = NULL;
    void *meta_data_buf = NULL;

    auto batch_2 = j_batch_new(j_batch_get_semantics(batch));

    /* names_kv = kv holding all variable names */
    auto kv_object_names = j_kv_new("variable_names", name_space);
    j_kv_get(kv_object_names, &names_buf, &value_len, batch_2);
    j_batch_execute(batch_2);

    if (value_len == 0)
    {
        bson_names = bson_new();
    }
    else
    {
        bson_names = bson_new_from_data((const uint8_t *)names_buf, value_len);
    }

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&b_iter, bson_names, metadata->name))
    {
        printf("Init b_iter successfull \n");
        bson_append_int32(bson_names, metadata->name, -1, metadata->var_type);
    }
    else
    {
        printf("++ JuleaDB Client Logic: Variable %s already in kv "
               "store. \n",
               metadata->name);
        // TODO: update variable -> is there anything else necessary to do?
    }

    /* Write metadata struct to kv store*/
    auto string_metadata_kv = g_strdup_printf("variables_%s", name_space);
    auto kv_object_metadata = j_kv_new(string_metadata_kv, metadata->name);

    auto bson_meta_data = bson_new();
    var_metadata_to_bson(metadata, bson_meta_data);

    meta_data_buf =
        g_memdup(bson_get_data(bson_meta_data), bson_meta_data->len);
    names_buf = g_memdup(bson_get_data(bson_names), bson_names->len);

    j_kv_put(kv_object_metadata, meta_data_buf, bson_meta_data->len, g_free,
             batch);
    j_kv_put(kv_object_names, names_buf, bson_names->len, g_free, batch);

    /* Write data pointer to object store*/
    auto string_data_object =
        g_strdup_printf("%s_variables_%s", name_space, metadata->name);
    auto data_object = j_object_new(string_data_object, metadata->name);

    j_object_create(data_object, batch);
    j_object_write(data_object, data_pointer, metadata->data_size, 0,
                   &bytes_written, batch);

    j_batch_execute(batch); // Writing data and metadata

    if (bytes_written == metadata->data_size)
    {
        printf("++ JuleaDB Client Logic: Data written for variable "
               "'%s' \n",
               metadata->name);
    }
    else
    {
        printf("WARNING: only %ld bytes written instead of %d bytes! \n",
               bytes_written, metadata->data_size);
    }
    g_free(string_metadata_kv);
    g_free(string_data_object);

    j_kv_unref(kv_object_names);
    j_kv_unref(kv_object_metadata);
    j_object_unref(data_object);
    j_batch_unref(batch_2);
    bson_destroy(bson_names);
    bson_destroy(bson_meta_data);

    printf("++ JuleaDB Client Logic: Put Variable \n");
}

/**
 * Put the data and the according metadata of an ADIOS2 attribute. There is no
 separate function for putting only the metadata. The data is stored in an
 object store while the metadata is stored in the structured metadata backend
 (SMD).
 *
 * \param [r] name_space    defined by io.open("namespace")
 * \param [r] attr_metadata attribute metadata struct containing the information
                                                        to store in SMD backend
 * \param [r] data_pointer  attribute data to be stored in object store
 * \param [r] batch         batch to execute the operation in
 * \param [r] use_batch     pass false when using deferred/asynchronous I/O;
 true for synchronous I/O
 */
void PutAttributeToJuleaDB(char *name_space, AttributeMetadata *attr_metadata,
                         void *data_pointer, JBatch *batch)
{
    // JBatch *batch_2;
    guint64 bytes_written = 0;
    guint32 value_len = 0;

    bson_iter_t b_iter;
    // bson_t *bson_meta_data;
    bson_t *bson_names;

    // g_autoptr(JKV) kv_object_metadata = NULL;
    // g_autoptr(JKV) kv_object_names = NULL;
    // g_autoptr(JObject) data_object = NULL;

    // gchar *string_metadata_kv;
    // gchar *string_data_object;

    void *names_buf = NULL;
    void *meta_data_buf = NULL;

    auto batch_2 = j_batch_new(j_batch_get_semantics(batch));

    auto string_data_object =
        g_strdup_printf("%s_attributes_%s", name_space, attr_metadata->name);
    auto data_object = j_object_new(string_data_object, attr_metadata->name);

    j_object_create(data_object, batch);
    j_object_write(data_object, data_pointer, attr_metadata->data_size, 0,
                   &bytes_written, batch);

    auto string_metadata_kv = g_strdup_printf("attributes_%s", name_space);
    auto kv_object_metadata = j_kv_new(string_metadata_kv, attr_metadata->name);
    auto kv_object_names = j_kv_new("attribute_names", name_space);

    j_kv_get(kv_object_names, &names_buf, &value_len, batch_2);
    j_batch_execute(batch_2);

    // /* check if variable name is already in kv store */
    if (value_len == 0)
    {
        bson_names = bson_new();
    }
    else
    {
        bson_names = bson_new_from_data((const uint8_t *)names_buf, value_len);
    }
    // g_free(names_buf);

    /* check if attribute name is already in kv store */
    if (!bson_iter_init_find(&b_iter, bson_names, attr_metadata->name))
    {
        printf("Init b_iter successfull \n");
        bson_append_int32(bson_names, attr_metadata->name, -1,
                          attr_metadata->attr_type);
    }
    else
    {
        printf("++ JuleaDB Client Logic: Variable %s already in kv "
               "store. \n",
               attr_metadata->name);
    }

    auto bson_meta_data = bson_new();
    attr_metadata_to_bson(attr_metadata, bson_meta_data);

    j_kv_put(kv_object_metadata, meta_data_buf, bson_meta_data->len, g_free,
             batch);
    j_kv_put(kv_object_names, names_buf, bson_names->len, g_free, batch);

    j_batch_execute(batch);

    if (bytes_written == attr_metadata->data_size)
    {
        printf("++ JuleaDB Client Logic: Data written for "
               "attribute '%s' \n",
               attr_metadata->name);
    }
    else
    {
        printf("WARNING: only %ld bytes written instead of %d bytes! \n",
               bytes_written, attr_metadata->data_size);
    }

    g_free(string_metadata_kv);
    g_free(string_data_object);
    j_kv_unref(kv_object_metadata);
    j_kv_unref(kv_object_names);
    j_object_unref(data_object);
    j_batch_unref(batch_2);
    bson_destroy(bson_names);
    bson_destroy(bson_meta_data);

    printf("++ JuleaDB Client Logic: Put Attribute \n");
}

/**
 * Get all variable names from kv store for the passed namespace. If there are
 * no variables in the kv store then count_names will return 0 (not an error
 * code) and there will be no memory allocation for the parameters "names" and
 * "types".
 *
 * NOTE: WILL BE DEPRECATED WHEN SMD BACKEND IS FINISHED!
 *
 * \param [r] name_space  defined by io.open("namespace")
 * \param [w] names       array to store the retrieved names
 * \param [w] types       array to store the retrieved variable types
 * \param [w] count_names number of names to retrieve
 * \param [r] semantics   semantics to be used
 */
void GetAllVarNamesFromKV(char *name_space, char ***names, int **types,
                          unsigned int *count_names, JSemantics *semantics)
{
    bson_t *bson_names;
    bson_iter_t b_iter;
    guint32 value_len = 0;

    // g_autoptr(JKV) kv_object = NULL;
    void *names_buf = NULL;

    auto batch = j_batch_new(semantics);
    // printf("-- JADIOS DEBUG PRINT: get_all_var_names_from_kv \n");

    auto kv_object = j_kv_new("variable_names", name_space);

    j_kv_get(kv_object, &names_buf, &value_len, batch);
    j_batch_execute(batch);

    if (value_len == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The names key-value store is empty! \n");
        *count_names = 0;
    }
    else
    {
        bson_names = bson_new_from_data((const uint8_t *)names_buf, value_len);
    }

    *count_names = bson_count_keys(bson_names);
    printf("-- JADIOS DEBUG PRINT: count_names %d\n", *count_names);

    *names = (char **)g_slice_alloc(*count_names * sizeof(char *));
    *types = (int *)g_slice_alloc(*count_names * sizeof(int));
    bson_iter_init(&b_iter, bson_names);

    for (unsigned int i = 0; i < *count_names; i++)
    {
        if (!bson_iter_next(&b_iter))
        {
            printf("ERROR: count of names does not match \n");
        }
        (*names)[i] = g_strdup(bson_iter_key(&b_iter));
        (*types)[i] = bson_iter_int32(&b_iter);
        // printf("-- JADIOS DEBUG PRINT: get_all_var_names_from_kv DEBUG PRINT:
        // %s\n", (*names)[i]); printf("-- JADIOS DEBUG PRINT: types DEBUG
        // PRINT: %d\n", (*types)[i]);
    }
    if (value_len > 0)
    {
        bson_destroy(bson_names);
    }
    j_kv_unref(kv_object);
    j_batch_unref(batch);
}

/**
 * Get the metadata from the kv store for the passed variable.
 *
 * NOTE: WILL BE DEPRECATED WHEN SMD BACKEND IS FINISHED!
 *
 * \param [r] name_space    defined by io.open("namespace")
 * \param [r] var_name 		variable name
 * \param [w] metadata   	metadata information struct; needs to be
 * allocated
 * \param [r] semantics  	semantics to be used
 */
void GetVarMetadataFromKV(char *name_space, char *var_name, Metadata *metadata,
                          JSemantics *semantics)
{
    // JBatch *batch;
    // gchar *string_metadata_kv;
    gchar *key;
    // bson_t* bson_metadata;
    bson_t bson_metadata;
    bson_iter_t b_iter;
    guint32 value_len = 0;

    // g_autoptr(JKV) kv_object = NULL;
    void *meta_data_buf = NULL;
    auto batch = j_batch_new(semantics);

    auto string_metadata_kv = g_strdup_printf("variables_%s", name_space);
    auto kv_object = j_kv_new(string_metadata_kv, var_name);
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
        bson_init_static(&bson_metadata, (uint8_t *)meta_data_buf, value_len);
    }

    // bson_iter_init(&b_iter, bson_metadata);
    // if(bson_iter_init(&b_iter, bson_metadata))
    if (bson_iter_init(&b_iter, &bson_metadata))
    {
        std::cout << "++ JuleaDB Client Logic: Bson iterator is valid"
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
            metadata->shape_size = bson_iter_int64(&b_iter);
            // printf("-- JADIOS DEBUG PRINT: shape_size = %ld \n",
            // metadata->shape_size);
            if (metadata->shape_size > 0)
            {
                for (guint i = 0; i < metadata->shape_size; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("shape_%d", i);
                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        metadata->shape[i] = bson_iter_int64(&b_iter);
                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "start_size") == 0)
        {
            metadata->start_size = bson_iter_int64(&b_iter);
            // printf("-- JADIOS DEBUG PRINT: start_size = %ld \n",
            // metadata->start_size);

            if (metadata->start_size > 0)
            {
                for (guint i = 0; i < metadata->start_size; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("start_%d", i);
                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        metadata->start[i] = bson_iter_int64(&b_iter);
                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "count_size") == 0)
        {
            metadata->count_size = bson_iter_int64(&b_iter);
            // printf("-- JADIOS DEBUG PRINT: count_size = %ld \n",
            // metadata->count_size);

            if (metadata->count_size > 0)
            {
                for (guint i = 0; i < metadata->count_size; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("count_%d", i);
                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        metadata->count[i] = bson_iter_int64(&b_iter);
                        // printf("-- JADIOS DEBUG PRINT: count[%d] = %ld \n",i,
                        // metadata->count[i]);
                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "memory_start_size") == 0)
        {
            metadata->memory_start_size = bson_iter_int64(&b_iter);

            if (metadata->memory_start_size > 0)
            {
                for (guint i = 0; i < metadata->memory_start_size; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("memory_start_%d", i);
                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        metadata->memory_start[i] = bson_iter_int64(&b_iter);
                    }
                }
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "memory_count_size") == 0)
        {
            metadata->memory_count_size = bson_iter_int64(&b_iter);

            if (metadata->memory_count_size > 0)
            {
                for (guint i = 0; i < metadata->memory_count_size; i++)
                {
                    bson_iter_next(&b_iter);
                    key = g_strdup_printf("memory_count_%d", i);
                    if (g_strcmp0(bson_iter_key(&b_iter), key) == 0)
                    {
                        metadata->memory_count[i] = bson_iter_int64(&b_iter);
                    }
                }
            }
        }
        /* unsigned long */
        else if (g_strcmp0(bson_iter_key(&b_iter), "steps_start") == 0)
        {
            metadata->steps_start = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "steps_count") == 0)
        {
            metadata->steps_count = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "block_id") == 0)
        {
            metadata->block_id = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "index_start") == 0)
        {
            metadata->index_start = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "element_size") == 0)
        {
            metadata->element_size = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "available_steps_start") ==
                 0)
        {
            metadata->available_steps_start = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "available_steps_count") ==
                 0)
        {
            metadata->available_steps_count = bson_iter_int64(&b_iter);
        }
        /* variable_type */
        else if (g_strcmp0(bson_iter_key(&b_iter), "var_type") == 0)
        {
            metadata->var_type = (variable_type)bson_iter_int64(&b_iter);
        }
        /* unsigned int */
        else if (g_strcmp0(bson_iter_key(&b_iter), "data_size") == 0)
        {
            metadata->data_size = bson_iter_int64(&b_iter);
        }
        /* boolean */
        // else if (g_strcmp0(bson_iter_key(&b_iter), "is_value") == 0)
        // {
        //     metadata->is_value = (bool)bson_iter_bool(&b_iter);
        // }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_single_value") == 0)
        {
            metadata->is_single_value = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_constant_dims") == 0)
        {
            metadata->is_constant_dims = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_read_as_joined") == 0)
        {
            metadata->is_read_as_joined = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_read_as_local_value") ==
                 0)
        {
            metadata->is_read_as_local_value = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_random_access") == 0)
        {
            metadata->is_random_access = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_first_streaming_step") ==
                 0)
        {
            metadata->is_first_streaming_step = (bool)bson_iter_bool(&b_iter);
        }
        /* value_type*/
        else if (g_strcmp0(bson_iter_key(&b_iter), "min_value") == 0)
        {
            if (metadata->var_type == INT32)
            {
                metadata->min_value.integer_32 = bson_iter_int32(&b_iter);
            }
            else if (metadata->var_type == INT64)
            {
                metadata->min_value.integer_64 = bson_iter_int64(&b_iter);
            }
            // else if(metadata->var_type == UNSIGNED_LONG_LONG_INT) //FIXME
            // data type
            // {
            // 	bson_iter_decimal128(&b_iter, (bson_decimal128_t*)
            // metadata->min_value.ull_integer);
            // }
            else if (metadata->var_type == FLOAT)
            {
                metadata->min_value.real_float = bson_iter_double(&b_iter);
            }
            else if (metadata->var_type == DOUBLE)
            {
                metadata->min_value.real_double = bson_iter_double(&b_iter);
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "max_value") == 0)
        {
            if (metadata->var_type == INT32)
            {
                metadata->max_value.integer_32 = bson_iter_int32(&b_iter);
            }
            else if (metadata->var_type == INT64)
            {
                metadata->max_value.integer_64 = bson_iter_int64(&b_iter);
            }
            // else if(metadata->var_type == UNSIGNED_LONG_LONG_INT)
            // {
            // 	bson_iter_decimal128(&b_iter,(bson_decimal128_t*)
            // metadata->max_value.ull_integer);
            // }
            else if (metadata->var_type == FLOAT)
            {
                metadata->max_value.real_float =
                    (float)bson_iter_double(&b_iter);
            }
            else if (metadata->var_type == DOUBLE)
            {
                metadata->max_value.real_double = bson_iter_double(&b_iter);
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "curr_value") == 0)
        {
            if (metadata->var_type == INT32)
            {
                metadata->curr_value.integer_32 = bson_iter_int32(&b_iter);
            }
            else if (metadata->var_type == INT64)
            {
                metadata->curr_value.integer_64 = bson_iter_int64(&b_iter);
            }
            // else if(metadata->var_type == UNSIGNED_LONG_LONG_INT)
            // {
            // 	bson_iter_decimal128(&b_iter,(bson_decimal128_t*)
            // metadata->curr_value.ull_integer);
            // }
            else if (metadata->var_type == FLOAT)
            {
                metadata->curr_value.real_float =
                    (float)bson_iter_double(&b_iter);
            }
            else if (metadata->var_type == DOUBLE)
            {
                metadata->curr_value.real_double = bson_iter_double(&b_iter);
            }
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_constant_dims") == 0)
        {
            metadata->is_constant_dims = (bool)bson_iter_bool(&b_iter);
        }
        else
        {
            printf(
                "Unknown key '%s' when retrieving metadata for variable %s\n",
                bson_iter_key(&b_iter), metadata->name);
        }
    }
    g_free(string_metadata_kv);
    g_free(key);
    if (value_len > 0)
    {
        bson_destroy(&bson_metadata);
    }
    j_kv_unref(kv_object);
    j_batch_unref(batch);
    printf("++ JuleaDB Client Logic: Get Variable Metadata \n");
}

/**
 * Get all attribute names from kv store for the passed namespace.
 *
 * NOTE: WILL BE DEPRECATED WHEN SMD BACKEND IS FINISHED!
 *
 * \param [r] name_space  defined by io.open("namespace")
 * \param [w] names       array to store the retrieved names
 * \param [w] types       array to store the retrieved variable types
 * \param [w] count_names number of names to retrieve
 * \param [r] semantics   semantics to be used
 */
void GetAllAttrNamesFromKV(char *name_space, char ***names, int **types,
                           unsigned int *count_names, JSemantics *semantics)
{
    bson_t *bson_names;
    bson_iter_t b_iter;
    guint32 value_len = 0;

    void *names_buf = NULL;

    auto batch = j_batch_new(semantics);
    auto kv_object = j_kv_new("attribute_names", name_space);

    j_kv_get(kv_object, &names_buf, &value_len, batch);
    j_batch_execute(batch);

    if (value_len == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The attribute names key-value store is empty! \n");
        *count_names = 0;
    }
    else
    {
        bson_names = bson_new_from_data((const uint8_t *)names_buf, value_len);
    }

    *count_names = bson_count_keys(bson_names);

    *names = (char **)g_slice_alloc(*count_names * sizeof(char *));
    *types = (int *)g_slice_alloc(*count_names * sizeof(int));
    bson_iter_init(&b_iter, bson_names);

    for (unsigned int i = 0; i < *count_names; i++)
    {
        if (!bson_iter_next(&b_iter))
        {
            printf("ERROR: count of names does not match \n");
        }
        (*names)[i] = g_strdup(bson_iter_key(&b_iter));
        (*types)[i] = bson_iter_int32(&b_iter);
    }
    j_kv_unref(kv_object);
    if (value_len > 0)
    {
        bson_destroy(bson_names);
    }
    j_batch_unref(batch);
}

/**
 * Get the attribute metadata from the kv store for the attribute.
 *
 * NOTE: WILL BE DEPRECATED WHEN SMD BACKEND IS FINISHED!
 *
 * \param [r] name_space    defined by io.open("namespace")
 * \param [r] var_name 		namespace of attr = unique engine name in m_IO
 * \param [w] metadata   	metadata information struct; needs to be
 * allocated
 * \param [r] semantics  	semantics to be used
 */
void GetAttrMetadataFromKV(char *name_space, char *attr_name,
                           AttributeMetadata *attr_metadata,
                           JSemantics *semantics)
{
    bson_t bson_metadata;
    bson_iter_t b_iter;
    guint32 value_len = 0;

    void *meta_data_buf = NULL;
    auto batch = j_batch_new(semantics);

    auto string_metadata_kv = g_strdup_printf("attributes_%s", name_space);
    auto kv_object = j_kv_new(string_metadata_kv, attr_name);

    j_kv_get(kv_object, &meta_data_buf, &value_len, batch);
    j_batch_execute(batch);

    if (value_len == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The attribute key-value store is empty! \n");
    }
    else
    {
        bson_init_static(&bson_metadata, (uint8_t *)meta_data_buf, value_len);
    }

    bson_iter_init(&b_iter, &bson_metadata);

    while (bson_iter_next(&b_iter))
    {
        if (g_strcmp0(bson_iter_key(&b_iter), "attr_type") == 0)
        {
            attr_metadata->attr_type = (variable_type)bson_iter_int32(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "number_elements") == 0)
        {
            attr_metadata->number_elements = bson_iter_int64(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "is_single_value") == 0)
        {
            attr_metadata->is_single_value = (bool)bson_iter_bool(&b_iter);
        }
        else if (g_strcmp0(bson_iter_key(&b_iter), "data_size") == 0)
        {
            attr_metadata->data_size = bson_iter_int64(&b_iter);
        }
        else
        {
            printf(
                "Unknown key '%s' when retrieving metadata for attribute %s\n",
                bson_iter_key(&b_iter), attr_metadata->name);
        }
    }

    g_free(string_metadata_kv);
    j_kv_unref(kv_object);
    j_batch_unref(batch);
    bson_destroy(&bson_metadata);
    printf("---* JuleaDB Adios Client: Get Attribute Metadata \n");
}

/**
 * Get the data for the passed variable from the object store.
 *
 * \param [r] name_space    defined by io.open("namespace")
 * \param [r] variable_name name of variable
 * \param [r] length        number of bytes to read
 * \param [w] data_pointer  pointer to return data
 * \param [r] batch         batch to execute this operation in
 * \param [r] use_batch     pass false when using deferred/asynchronous I/O;
 * true for synchronous I/O
 */
void GetVarDataFromJuleaDB(char *name_space, char *variable_name,
                         unsigned int length, void *data_pointer, JBatch *batch)
{
    guint64 bytes_read = 0;

    auto string_data_object =
        g_strdup_printf("%s_variables_%s", name_space, variable_name);
    auto data_object = j_object_new(string_data_object, variable_name);

    j_object_read(data_object, data_pointer, length, 0, &bytes_read, batch);

    j_batch_execute(batch);
    printf("++ JuleaDB Client Logic: Batch execute \n");

    if (bytes_read == length)
    {
        printf("++ JuleaDB Client Logic: Read data for variable "
               "'%s' \n",
               variable_name);
    }
    else
    {
        printf("WARNING: only %ld bytes read instead of %d bytes! \n",
               bytes_read, length);
    }

    g_free(string_data_object);
    j_object_unref(data_object);
}

/**
 * Get the data for the passed attribute from the object store.
 *
 * \param [r] name_space    defined in io.open("namespace")
 * \param [r] variable_name name of variable
 * \param [r] length        number of bytes to read
 * \param [w] data_pointer  pointer to return data
 * \param [r] batch         batch to execute this operation in
 * \param [r] use_batch     pass false when using deferred/asynchronous I/O;
 * true for synchronous I/O \return
 * returns 0 on success
 */
void GetAttrDataFromJuleaDB(char *name_space, char *attribute_name,
                          unsigned int length, void *data_pointer,
                          JBatch *batch)
{
    guint64 bytes_read = 0;

    auto string_data_object =
        g_strdup_printf("%s_attributes_%s", name_space, attribute_name);

    auto data_object = j_object_new(string_data_object, attribute_name);
    j_object_read(data_object, data_pointer, length, 0, &bytes_read, batch);
    if (bytes_read == length)
    {
        printf("++ JuleaDB Client Logic: Read data for variable "
               "'%s' \n",
               attribute_name);
    }
    else
    {
        printf("WARNING: only %ld bytes read instead of %d bytes! \n",
               bytes_read, length);
    }

    j_batch_execute(batch); // DESIGN: where should this be? how often?
    printf("++ JuleaDB Client Logic: Batch execute \n");

    g_free(string_data_object);
    j_object_unref(data_object);
}

/**
 * Delete variable and according metadata.
 *
 * \param [r] name_space [description]
 * \param [r] metadata   [description]
 * \param [r] batch      [description]
 */
void DeleteVariable(char *name_space, char *var_name, JBatch *batch)
{
    auto string_data_object =
        g_strdup_printf("%s_variables_%s", name_space, var_name);
    auto string_metadata_kv = g_strdup_printf("variables_%s", name_space);

    auto data_object = j_object_new(string_data_object, var_name);
    auto kv_object_metadata = j_kv_new(string_metadata_kv, var_name);
    auto kv_object_names = j_kv_new("variable_names", name_space);

    j_object_delete(data_object, batch);
    j_kv_delete(kv_object_metadata, batch);
    j_kv_delete(kv_object_names, batch);

    j_batch_execute(batch);

    g_free(string_metadata_kv);
    g_free(string_data_object);
    j_kv_unref(kv_object_metadata);
    j_kv_unref(kv_object_names);
    j_object_unref(data_object);

    printf("---* JuleaDB Adios Client: Delete variable %s \n", var_name);
}

/**
 * Delete attribute and according metadata.
 *
 * \param [r] name_space [description]
 * \param [r] metadata   [description]
 * \param [r] batch      [description]
 */
void DeleteAttribute(char *name_space, char *var_name, JBatch *batch)
{
    auto string_data_object =
        g_strdup_printf("%s_attributes_%s", name_space, var_name);
    auto string_metadata_kv = g_strdup_printf("attributes_%s", name_space);

    auto data_object = j_object_new(string_data_object, var_name);
    auto kv_object_metadata = j_kv_new(string_metadata_kv, var_name);
    auto kv_object_names = j_kv_new("attribute_names", name_space);

    j_object_delete(data_object, batch);
    j_kv_delete(kv_object_metadata, batch);
    j_kv_delete(kv_object_names, batch);

    j_batch_execute(batch);

    g_free(string_metadata_kv);
    g_free(string_data_object);
    j_kv_unref(kv_object_metadata);
    j_kv_unref(kv_object_names);
    j_object_unref(data_object);

    printf("---* JuleaDB Adios Client: Delete attribute %s \n", var_name);
}

} // end namespace engine
} // end namespace core
} // end namespace adios2
