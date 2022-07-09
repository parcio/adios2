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
#include "JuleaDBInteractionWriter.tcc"
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"
// #include "JuleaDBInteractionReader.h"
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

// Set Daily values; also set monthly, yearly values if -1 is passed for month
// and/or day
void SetDailyValues(JDBEntry *entry, interop::JuleaCDO &JuleaCDO,
                    const std::string varName, int year, int month, int day)
{
    // daily values
    if (day >= 0)
    {
        if (varName == JuleaCDO.m_TemperatureName)
        {
            j_db_entry_set_field(entry, "daily_globalMin", &JuleaCDO.m_DTempMin,
                                 sizeof(JuleaCDO.m_DTempMin), NULL);
            j_db_entry_set_field(entry, "daily_globalMax", &JuleaCDO.m_DTempMax,
                                 sizeof(JuleaCDO.m_DTempMax), NULL);
            j_db_entry_set_field(entry, "daily_globalMean",
                                 &JuleaCDO.m_DTempMean,
                                 sizeof(JuleaCDO.m_DTempMean), NULL);
            j_db_entry_set_field(entry, "daily_globalSum",
                                 &JuleaCDO.m_NotComputedValue,
                                 sizeof(JuleaCDO.m_NotComputedValue), NULL);
            j_db_entry_set_field(entry, "daily_globalVar", &JuleaCDO.m_DTempVar,
                                 sizeof(JuleaCDO.m_DTempVar), NULL);
        }
        if (varName == JuleaCDO.m_PrecipitationName)
        {
            j_db_entry_set_field(entry, "daily_globalMin", &JuleaCDO.m_DPrecMin,
                                 sizeof(JuleaCDO.m_DPrecMin), NULL);
            j_db_entry_set_field(entry, "daily_globalMax", &JuleaCDO.m_DPrecMax,
                                 sizeof(JuleaCDO.m_DPrecMax), NULL);
            j_db_entry_set_field(entry, "daily_globalMean",
                                 &JuleaCDO.m_DPrecMean,
                                 sizeof(JuleaCDO.m_DPrecMean), NULL);
            j_db_entry_set_field(entry, "daily_globalSum", &JuleaCDO.m_DPrecSum,
                                 sizeof(JuleaCDO.m_DPrecSum), NULL);
            j_db_entry_set_field(entry, "daily_globalVar", &JuleaCDO.m_DPrecVar,
                                 sizeof(JuleaCDO.m_DPrecVar), NULL);
        }
        // monthly values
        else if (day == JuleaCDO.m_StoreMonthlyValue)

        {
            if (varName == JuleaCDO.m_TemperatureName)
            {
                j_db_entry_set_field(entry, "daily_globalMin",
                                     &JuleaCDO.m_MTempMin,
                                     sizeof(JuleaCDO.m_MTempMin), NULL);
                j_db_entry_set_field(entry, "daily_globalMax",
                                     &JuleaCDO.m_MTempMax,
                                     sizeof(JuleaCDO.m_MTempMax), NULL);
                j_db_entry_set_field(entry, "daily_globalMean",
                                     &JuleaCDO.m_MTempMean,
                                     sizeof(JuleaCDO.m_MTempMean), NULL);
                j_db_entry_set_field(entry, "daily_globalSum",
                                     &JuleaCDO.m_NotComputedValue,
                                     sizeof(JuleaCDO.m_NotComputedValue), NULL);
                j_db_entry_set_field(entry, "daily_globalVar",
                                     &JuleaCDO.m_MTempVar,
                                     sizeof(JuleaCDO.m_MTempVar), NULL);
            }
            if (varName == JuleaCDO.m_PrecipitationName)
            {
                j_db_entry_set_field(entry, "daily_globalMin",
                                     &JuleaCDO.m_MPrecMin,
                                     sizeof(JuleaCDO.m_MPrecMin), NULL);
                j_db_entry_set_field(entry, "daily_globalMax",
                                     &JuleaCDO.m_MPrecMax,
                                     sizeof(JuleaCDO.m_MPrecMax), NULL);
                j_db_entry_set_field(entry, "daily_globalMean",
                                     &JuleaCDO.m_MPrecMean,
                                     sizeof(JuleaCDO.m_MPrecMean), NULL);
                j_db_entry_set_field(entry, "daily_globalSum",
                                     &JuleaCDO.m_MPrecSum,
                                     sizeof(JuleaCDO.m_MPrecSum), NULL);
                j_db_entry_set_field(entry, "daily_globalVar",
                                     &JuleaCDO.m_MPrecVar,
                                     sizeof(JuleaCDO.m_MPrecVar), NULL);
            }
            // yearly values
            else if (day == JuleaCDO.m_StoreYearlyValue)
            {
                if (varName == JuleaCDO.m_TemperatureName)
                {
                    j_db_entry_set_field(entry, "daily_globalMin",
                                         &JuleaCDO.m_YTempMin,
                                         sizeof(JuleaCDO.m_YTempMin), NULL);
                    j_db_entry_set_field(entry, "daily_globalMax",
                                         &JuleaCDO.m_YTempMax,
                                         sizeof(JuleaCDO.m_YTempMax), NULL);
                    j_db_entry_set_field(entry, "daily_globalMean",
                                         &JuleaCDO.m_YTempMean,
                                         sizeof(JuleaCDO.m_YTempMean), NULL);
                    j_db_entry_set_field(
                        entry, "daily_globalSum", &JuleaCDO.m_NotComputedValue,
                        sizeof(JuleaCDO.m_NotComputedValue), NULL);
                    j_db_entry_set_field(entry, "daily_globalVar",
                                         &JuleaCDO.m_YTempVar,
                                         sizeof(JuleaCDO.m_YTempVar), NULL);
                }
                if (varName == JuleaCDO.m_PrecipitationName)
                {
                    j_db_entry_set_field(entry, "daily_globalMin",
                                         &JuleaCDO.m_YPrecMin,
                                         sizeof(JuleaCDO.m_YPrecMin), NULL);
                    j_db_entry_set_field(entry, "daily_globalMax",
                                         &JuleaCDO.m_YPrecMax,
                                         sizeof(JuleaCDO.m_YPrecMax), NULL);
                    j_db_entry_set_field(entry, "daily_globalMean",
                                         &JuleaCDO.m_YPrecMean,
                                         sizeof(JuleaCDO.m_YPrecMean), NULL);
                    j_db_entry_set_field(entry, "daily_globalSum",
                                         &JuleaCDO.m_YPrecSum,
                                         sizeof(JuleaCDO.m_YPrecSum), NULL);
                    j_db_entry_set_field(entry, "daily_globalVar",
                                         &JuleaCDO.m_YPrecVar,
                                         sizeof(JuleaCDO.m_YPrecVar), NULL);
                }
            }
        }
    }
}

JuleaDBInteractionWriter::JuleaDBInteractionWriter(helper::Comm const &comm)
: JuleaInteraction(std::move(comm))
{
    // std::cout << "This is the constructor of the writer" << std::endl;
}

void JuleaDBInteractionWriter::AddFieldsForTagTable(JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *stepIndex[] = {"step", NULL};
    gchar const *blockIndex[] = {"block", NULL};

    // j_db_schema_add_field(schema, "projectNamespace", J_DB_TYPE_STRING,
    // NULL);

    // j_db_schema_add_field(schema, "tagName", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "step", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "block", J_DB_TYPE_UINT64, NULL);

    // could be useful to have the entryID directly in tag table
    j_db_schema_add_field(schema, "entryID", J_DB_TYPE_UINT64, NULL);

    // FIXME: is JDAIStatistic not string
    //  j_db_schema_add_field(schema, "statisticName", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "stat", J_DB_TYPE_UINT32, NULL);
    // j_db_schema_add_field(schema, "operator", J_DB_TYPE_UINT32, NULL);
    // j_db_schema_add_field(schema, "granularity", J_DB_TYPE_UINT32, NULL);

    // j_db_schema_add_field(schema, "threshold_i", J_DB_TYPE_UINT32, NULL);
    // j_db_schema_add_field(schema, "threshold_d", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
}

void InitTagsFromConfigDB(std::string projectNamespace, gchar **tagName,
                          gchar **fileName, gchar **variableName,
                          JDAIGranularity *granularity,
                          JDAIStatistic *statistic, JDAIOperator *op,
                          double *threshold)
{
    size_t err = 0;
    size_t db_length = 0;
    JDBType type;
    // gchar* db_field = NULL;
    gchar *completeNamespace = NULL;

    g_autoptr(JBatch) batch = NULL;
    g_autoptr(JSemantics) semantics = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    g_autoptr(JDBSelector) selector = NULL;

    semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    batch = j_batch_new(semantics);

    completeNamespace =
        g_strdup_printf("%s_%s", "adios2-", projectNamespace.c_str());
    auto tag_schema = j_db_schema_new(completeNamespace, "tags", NULL);

    // complete_namespace = g_strdup_printf(
    // 	"%s_%s", "adios2-", name_space);

    // schema = j_db_schema_new(complete_namespace, "variable-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    if (err == 0)
    {
        // TODO error handling
    }
    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "projectNamespace",
                            J_DB_SELECTOR_OPERATOR_EQ, projectNamespace.c_str(),
                            strlen(projectNamespace.c_str()) + 1, NULL);
    // j_db_selector_add_field(selector, "file",
    // 			J_DB_SELECTOR_OPERATOR_EQ, fileName.c_str(),
    // 			strlen(fileName.c_str()) + 1, NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);

    while (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "tagName", &type,
                                (gpointer *)&tagName, &db_length, NULL);
        j_db_iterator_get_field(iterator, "file", &type, (gpointer *)&fileName,
                                &db_length, NULL);
        j_db_iterator_get_field(iterator, "variableName", &type,
                                (gpointer *)variableName, &db_length, NULL);
        j_db_iterator_get_field(iterator, "granularity", &type,
                                (gpointer *)&granularity, &db_length, NULL);
        j_db_iterator_get_field(iterator, "statistic", &type, (gpointer *)&stat,
                                &db_length, NULL);
        j_db_iterator_get_field(iterator, "operator", &type, (gpointer *)&op,
                                &db_length, NULL);
        j_db_iterator_get_field(iterator, "threshold_d", &type,
                                (gpointer *)&threshold, &db_length, NULL);
    }

    j_db_schema_unref(schema);
    j_db_selector_unref(selector);
}

void JuleaDBInteractionWriter::InitTagTables(std::string projectNamespace)
{
    // std::cout << "--- InitDBSchemas ---" << std::endl;
    int err = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    gchar *fileName = NULL;
    gchar *tagName = NULL;
    gchar *variableName = NULL;
    // gchar* statisticName = NULL;

    JDAIStatistic stat;
    JDAIOperator op;
    JDAIGranularity granularity;
    double threshold = 0;

    g_autoptr(JDBSchema) varSchema = NULL;

    InitTagsFromConfigDB(projectNamespace, &tagName, &fileName, &variableName,
                         &granularity, &stat, &op, &threshold);

    auto completeNamespace =
        g_strdup_printf("%s_%s", "adios2-", projectNamespace.c_str());
    auto tagSchema = j_db_schema_new(completeNamespace, tagName, NULL);
    // auto tag_schema = j_db_schema_new(completeNamespace, "tags", NULL);

    j_db_schema_get(tagSchema, batch, NULL);
    bool existsVar = j_batch_execute(batch);

    if (existsVar == 0)
    {
        // std::cout << "variable schema does not exist" << std::endl;
        tagSchema = j_db_schema_new(completeNamespace, tagName, NULL);
        AddFieldsForTagTable(tagSchema);
        // AddEntriesForTagTable(tagSchema);
        j_db_schema_create(varSchema, batch, NULL);
        g_assert_true(j_batch_execute(batch) == true);
    }
    j_batch_unref(batch);
}

/**
 * ---------------------------- Original format
 * ------------------------------------
 */
void JuleaDBInteractionWriter::AddFieldsForVariableMD_Original(
    JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *minFloatIndex[] = {"variableName", NULL};
    gchar const *maxFloatIndex[] = {"variableName", NULL};
    gchar const *minDoubleIndex[] = {"variableName", NULL};
    gchar const *maxDoubleIndex[] = {"variableName", NULL};
    gchar const *meanDoubleIndex[] = {"variableName", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "isConstantDims", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isReadAsJoined", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isReadAsLocalValue", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isRandomAccess", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isSingleValue", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "shapeID", J_DB_TYPE_UINT32, NULL);
    // TODO: needed?
    // j_db_schema_add_field(schema, "typeString", J_DB_TYPE_STRING,
    // NULL); j_db_schema_add_field(schema, "typeLen", J_DB_TYPE_UINT64,
    // NULL);

    // TODO: Check whether this renaming screws up anything in init
    j_db_schema_add_field(schema, "typeInt", J_DB_TYPE_UINT32, NULL);

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

    // add min/max/value for every type for performance improvement of
    // querying
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

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING,
    // NULL); j_db_schema_add_field(schema, "max_string",
    // J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, minFloatIndex, NULL);
    j_db_schema_add_index(schema, maxFloatIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
}

void JuleaDBInteractionWriter::AddFieldsForBlockMD_Original(JDBSchema *schema)
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

    j_db_schema_add_field(schema, "min_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "max_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "value_blob", J_DB_TYPE_BLOB, NULL);

    // add min/max/value for every type for performance improvement of
    // querying
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
    // j_db_schema_add_field(schema, "std_float64", J_DB_TYPE_FLOAT64, NULL);

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING,
    // NULL); j_db_schema_add_field(schema, "max_string",
    // J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "stepsStart", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "stepsCount", J_DB_TYPE_UINT64, NULL);
    // j_db_schema_add_field(schema, "blockID", J_DB_TYPE_UINT64, NULL);

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

/**
 * ---------------------------- Table only contains certain types
 * ------------------------------------
 */

void JuleaDBInteractionWriter::AddFieldsForVariableMD_OriginalDouble(
    JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
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

    // TODO: Check whether this renaming screws up anything in init
    j_db_schema_add_field(schema, "typeInt", J_DB_TYPE_UINT32, NULL);

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

    j_db_schema_add_field(schema, "min_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "max_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "value_float64", J_DB_TYPE_FLOAT64, NULL);

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING,
    // NULL); j_db_schema_add_field(schema, "max_string",
    // J_DB_TYPE_STRING, NULL);
    // j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
}

void JuleaDBInteractionWriter::AddFieldsForBlockMD_OriginalDouble(
    JDBSchema *schema)
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

    // add min/max/value for every type for performance improvement of
    // querying
    j_db_schema_add_field(schema, "min_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "max_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "value_float64", J_DB_TYPE_FLOAT64, NULL);

    j_db_schema_add_field(schema, "stepsStart", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "stepsCount", J_DB_TYPE_UINT64, NULL);
    // j_db_schema_add_field(schema, "blockID", J_DB_TYPE_UINT64, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, stepIndex, NULL);
    j_db_schema_add_index(schema, blockIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
}

/**
 * ---------------------------- Table contains all types  + mean, sum, var
 * ------------------------------------
 */

void JuleaDBInteractionWriter::AddFieldsForBlockMD_AllTypes_AdditionalStats(
    JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *stepIndex[] = {"step", NULL};
    gchar const *blockIndex[] = {"block", NULL};
    gchar const *minFloatIndex[] = {"variableName", NULL};
    gchar const *maxFloatIndex[] = {"variableName", NULL};
    gchar const *minDoubleIndex[] = {"variableName", NULL};
    gchar const *maxDoubleIndex[] = {"variableName", NULL};
    gchar const *meanDoubleIndex[] = {"variableName", NULL};
    //    gchar const *minIndex[] = {"min_blob", NULL};
    //    gchar const *maxIndex[] = {"max_blob", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "step", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "block", J_DB_TYPE_UINT64, NULL);
    // j_db_schema_add_field(schema, "X", J_DB_TYPE_UINT64, NULL);
    // j_db_schema_add_field(schema, "Y", J_DB_TYPE_UINT64, NULL);

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
    j_db_schema_add_field(schema, "year", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "month", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "day", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "min_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "max_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "value_blob", J_DB_TYPE_BLOB, NULL);

    // add min/max/value for every type for performance improvement of
    // querying
    j_db_schema_add_field(schema, "min_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "max_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "value_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "mean_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "sum_sint32", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "variance_sint32", J_DB_TYPE_SINT32, NULL);

    j_db_schema_add_field(schema, "min_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "max_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "value_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "mean_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "sum_uint32", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "variance_uint32", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "min_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "max_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "value_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "mean_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "sum_sint64", J_DB_TYPE_SINT64, NULL);
    j_db_schema_add_field(schema, "variance_sint64", J_DB_TYPE_SINT64, NULL);

    j_db_schema_add_field(schema, "min_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "max_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "value_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "mean_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "sum_uint64", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "variance_uint64", J_DB_TYPE_UINT64, NULL);

    j_db_schema_add_field(schema, "min_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "max_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "value_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "mean_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "sum_float32", J_DB_TYPE_FLOAT32, NULL);
    j_db_schema_add_field(schema, "variance_float32", J_DB_TYPE_FLOAT32, NULL);

    j_db_schema_add_field(schema, "min_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "max_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "value_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "mean_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "sum_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "variance_float64", J_DB_TYPE_FLOAT64, NULL);
    // j_db_schema_add_field(schema, "std_float64", J_DB_TYPE_FLOAT64, NULL);

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING,
    // NULL); j_db_schema_add_field(schema, "max_string",
    // J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "stepsStart", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "stepsCount", J_DB_TYPE_UINT64, NULL);
    // j_db_schema_add_field(schema, "blockID", J_DB_TYPE_UINT64, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, stepIndex, NULL);
    j_db_schema_add_index(schema, blockIndex, NULL);
    j_db_schema_add_index(schema, minFloatIndex, NULL);
    j_db_schema_add_index(schema, maxFloatIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
    j_db_schema_add_index(schema, meanDoubleIndex, NULL);
    //    j_db_schema_add_index(schema, minIndex, NULL);
    //    j_db_schema_add_index(schema, maxIndex, NULL);
}

/**
 * ---------------------------- Climate indices tables + global stats
 * ------------------------------------
 */

/**
 * One table for all climate indeces that are computed for evaluation
 */
void JuleaDBInteractionWriter::AddFieldsForClimateIndexTable(JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    // gchar const *minDoubleIndex[] = {"min_float64", NULL};
    // gchar const *maxDoubleIndex[] = {"max_float64", NULL};
    // gchar const *meanDoubleIndex[] = {"mean_float64", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "year", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "precip_days_1mm", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "precip_days_10mm", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "precip_days_20mm", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "frost_days", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "summer_days", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "tropical_nights", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "icing_days", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
}

/**
 * Table for the daily min/max/mean/sum/var with "global" resolution
 * global resolution = daily values for the complete step -> over all
 * processes monthly stats: day set to -1 yearly stats: month and day
 * set to -1
 */
void JuleaDBInteractionWriter::AddFieldsForDailyGlobalStatsTable(
    JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *yearIndex[] = {"year", NULL};
    gchar const *monthIndex[] = {"month", NULL};
    gchar const *dayIndex[] = {"day", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "year", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "month", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "day", J_DB_TYPE_SINT32, NULL);

    j_db_schema_add_field(schema, "daily_globalMin", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "daily_globalMax", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "daily_globalMean", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "daily_globalSum", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "daily_globalVar", J_DB_TYPE_FLOAT64, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, yearIndex, NULL);
    j_db_schema_add_index(schema, monthIndex, NULL);
    j_db_schema_add_index(schema, dayIndex, NULL);
}

/**
 * ---------------------------- Climate indices tables + global stats
 * ------------------------------------
 */

void JuleaDBInteractionWriter::AddFieldsForVariableMD_Eval(JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *minDoubleIndex[] = {"min_float64", NULL};
    gchar const *maxDoubleIndex[] = {"max_float64", NULL};
    gchar const *meanDoubleIndex[] = {"mean_float64", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "isConstantDims", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isReadAsJoined", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isReadAsLocalValue", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isRandomAccess", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "isSingleValue", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "shapeID", J_DB_TYPE_UINT32, NULL);
    // TODO: Check whether this renaming screws up anything in init
    j_db_schema_add_field(schema, "typeInt", J_DB_TYPE_UINT32, NULL);

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

    j_db_schema_add_field(schema, "min_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "max_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "mean_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "sum_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "var_float64", J_DB_TYPE_FLOAT64, NULL);

    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
    j_db_schema_add_index(schema, meanDoubleIndex, NULL);
}

void JuleaDBInteractionWriter::AddFieldsForBlockMD_Eval(JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *stepIndex[] = {"step", NULL};
    gchar const *blockIndex[] = {"block", NULL};
    gchar const *minDoubleIndex[] = {"min_float64", NULL};
    gchar const *maxDoubleIndex[] = {"max_float64", NULL};
    gchar const *meanDoubleIndex[] = {"mean_float64", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "step", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "block", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "X", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "Y", J_DB_TYPE_UINT64, NULL);

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
    j_db_schema_add_field(schema, "year", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "month", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "day", J_DB_TYPE_UINT32, NULL);

    j_db_schema_add_field(schema, "min_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "max_float64", J_DB_TYPE_FLOAT64, NULL);
    // j_db_schema_add_field(schema, "value_float64", J_DB_TYPE_FLOAT64,
    // NULL);
    j_db_schema_add_field(schema, "mean_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "sum_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "variance_float64", J_DB_TYPE_FLOAT64, NULL);
    // j_db_schema_add_field(schema, "std_float64", J_DB_TYPE_FLOAT64, NULL);

    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "stepsStart", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "stepsCount", J_DB_TYPE_UINT64, NULL);
    // j_db_schema_add_field(schema, "blockID", J_DB_TYPE_UINT64, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, stepIndex, NULL);
    j_db_schema_add_index(schema, blockIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
    j_db_schema_add_index(schema, meanDoubleIndex, NULL);
}

/**
 * ---------------------------- Add metadata to tables
 * ------------------------------------
 */
void JuleaDBInteractionWriter::AddEntriesForClimateIndexTable(
    const std::string projectNamespace, const std::string fileName,
    const std::string varName, size_t currentStep, interop::JuleaCDO &JuleaCDO)
{

    int err = 0;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    JDBType jdbType;
    guint64 db_length = 0;
    uint32_t *tmpID;
    size_t year = 0;
    // size_t month = 0;
    // size_t day = 0;
    // uint32_t entryID = 0;

    if (currentStep >= JuleaCDO.m_StepsPerYear)
    {
        year = (size_t)currentStep / JuleaCDO.m_StepsPerYear;
    }
    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto completeNamespace =
        g_strdup_printf("%s_%s", "adios2-", projectNamespace.c_str());
    schema = j_db_schema_new(completeNamespace, "climate-indices", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    entry = j_db_entry_new(schema, NULL);
    j_db_entry_set_field(entry, "projectNamespace", projectNamespace.c_str(),
                         strlen(projectNamespace.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "file", fileName.c_str(),
                         strlen(fileName.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "variableName", varName.c_str(),
                         strlen(varName.c_str()) + 1, NULL);

    j_db_entry_set_field(entry, "year", &year, sizeof(size_t), NULL);
    // j_db_entry_set_field(entry, "year", &stepID, sizeof(stepID),
    // NULL); j_db_entry_set_field(entry, "year", &stepID,
    // sizeof(stepID), NULL);
    j_db_entry_set_field(entry, "precip_days_1mm", &JuleaCDO.m_PrecipDays1mm,
                         sizeof(size_t), NULL);
    j_db_entry_set_field(entry, "precip_days_10mm", &JuleaCDO.m_PrecipDays10mm,
                         sizeof(size_t), NULL);
    j_db_entry_set_field(entry, "precip_days_20mm", &JuleaCDO.m_PrecipDays20mm,
                         sizeof(size_t), NULL);
    j_db_entry_set_field(entry, "frost_days", &JuleaCDO.m_FrostDays,
                         sizeof(size_t), NULL);
    j_db_entry_set_field(entry, "summer_days", &JuleaCDO.m_SummerDays,
                         sizeof(size_t), NULL);
    j_db_entry_set_field(entry, "tropical_nights", &JuleaCDO.m_TropicalNights,
                         sizeof(size_t), NULL);
    j_db_entry_set_field(entry, "icing_days", &JuleaCDO.m_IcingDays,
                         sizeof(size_t), NULL);
}

void JuleaDBInteractionWriter::AddEntriesForDailyGlobalStatsTable(
    const std::string projectNamespace, const std::string fileName,
    const std::string varName, size_t currentStep, interop::JuleaCDO &JuleaCDO,
    int writerRank, int year, int month, int day)
{
    int err = 0;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    JDBType jdbType;
    guint64 db_length = 0;
    uint32_t *tmpID;

    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto completeNamespace =
        g_strdup_printf("%s_%s", "adios2-", projectNamespace.c_str());
    schema =
        j_db_schema_new(completeNamespace, "daily-global-statistics", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    entry = j_db_entry_new(schema, NULL);
    j_db_entry_set_field(entry, "projectNamespace", projectNamespace.c_str(),
                         strlen(projectNamespace.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "file", fileName.c_str(),
                         strlen(fileName.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "variableName", varName.c_str(),
                         strlen(varName.c_str()) + 1, NULL);

    j_db_entry_set_field(entry, "year", &year, sizeof(year), NULL);
    j_db_entry_set_field(entry, "month", &month, sizeof(month), NULL);
    j_db_entry_set_field(entry, "day", &day, sizeof(day), NULL);

    SetDailyValues(entry, JuleaCDO, varName, year, month, day);
}

void JuleaDBInteractionWriter::InitDBSchemas(std::string projectNamespace,
                                             bool isOriginalFormat)
{
    // std::cout << "--- InitDBSchemas ---" << std::endl;
    int err = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);
    auto batch3 = j_batch_new(semantics);
    auto batch4 = j_batch_new(semantics);
    g_autoptr(JDBSchema) varSchema = NULL;
    g_autoptr(JDBSchema) blockSchema = NULL;
    g_autoptr(JDBSchema) cIndexSchema = NULL;  // climate indices table
    g_autoptr(JDBSchema) dGlobalSchema = NULL; // daily global statistics table

    auto completeNamespace =
        g_strdup_printf("%s_%s", "adios2-", projectNamespace.c_str());
    auto tag_schema = j_db_schema_new(completeNamespace, "tags", NULL);
    varSchema = j_db_schema_new(completeNamespace, "variable-metadata", NULL);
    blockSchema = j_db_schema_new(completeNamespace, "block-metadata", NULL);

    // TODO: check whether tables are required by user
    // readDAISettings
    cIndexSchema = j_db_schema_new(completeNamespace, "climate-indices", NULL);
    dGlobalSchema =
        j_db_schema_new(completeNamespace, "daily-global-statistics", NULL);

    j_db_schema_get(varSchema, batch, NULL);
    bool existsVar = j_batch_execute(batch);

    j_db_schema_get(blockSchema, batch, NULL);
    bool existsBlock = j_batch_execute(batch);

    j_db_schema_get(cIndexSchema, batch, NULL);
    bool existsClimaIndex = j_batch_execute(batch);

    j_db_schema_get(dGlobalSchema, batch, NULL);
    bool existsDailyGlobal = j_batch_execute(batch);

    // FIXME: differentiate between original tables and new ones

    if (existsVar == 0)
    {
        // std::cout << "variable schema does not exist" << std::endl;
        varSchema =
            j_db_schema_new(completeNamespace, "variable-metadata", NULL);
        // AddFieldsForVariableMDEval(varSchema);
        AddFieldsForVariableMD_Original(varSchema);
        j_db_schema_create(varSchema, batch, NULL);
        g_assert_true(j_batch_execute(batch) == true);
    }

    if (existsBlock == 0)
    {

        // std::cout << "block schema does not exist" << std::endl;
        blockSchema =
            j_db_schema_new(completeNamespace, "block-metadata", NULL);
        // AddFieldsForBlockMDEval(blockSchema);
        AddFieldsForBlockMD_Original(blockSchema);
        j_db_schema_create(blockSchema, batch2, NULL);
        g_assert_true(j_batch_execute(batch2) == true);
    }

    if (existsClimaIndex == 0)
    {
        cIndexSchema =
            j_db_schema_new(completeNamespace, "climate-indices", NULL);
        AddFieldsForClimateIndexTable(cIndexSchema);
        j_db_schema_create(cIndexSchema, batch3, NULL);
        g_assert_true(j_batch_execute(batch3) == true);
    }

    if (existsDailyGlobal == 0)
    {
        dGlobalSchema =
            j_db_schema_new(completeNamespace, "daily-global-statistics", NULL);
        AddFieldsForDailyGlobalStatsTable(dGlobalSchema);
        j_db_schema_create(dGlobalSchema, batch4, NULL);
        g_assert_true(j_batch_execute(batch4) == true);
    }

    // g_assert_true(j_batch_execute(batch2) == true);
    // j_db_schema_unref(varSchema);
    // j_db_schema_unref(blockSchema);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_batch_unref(batch3);
    j_batch_unref(batch4);
    j_semantics_unref(semantics);
}

#define declare_template_instantiation(T)                                      \
    template void JuleaDBInteractionWriter::AddEntriesForTagTable(             \
        const std::string fileName, const std::string varName,                 \
        size_t currentStep, size_t block, const T data);                       \
    template void JuleaDBInteractionWriter::PutVariableMetadataToJulea(        \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName,                 \
        size_t currStep, size_t block, bool original);                         \
    template void JuleaDBInteractionWriter::PutBlockMetadataToJulea(           \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName, size_t step,    \
        size_t block, const typename core::Variable<T>::Info &blockInfo,       \
        T &blockMin, T &blockMax, T &blockMean, T &blockSum, T &blockVar,      \
        uint32_t &entryID, bool original);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios2
