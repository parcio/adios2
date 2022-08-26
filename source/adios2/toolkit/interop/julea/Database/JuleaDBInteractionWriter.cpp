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
#include "adios2/toolkit/interop/julea/JuleaCDO.h"
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
            j_db_entry_set_field(entry, "daily_globalMin",
                                 &JuleaCDO.m_DTempMin[day],
                                 sizeof(JuleaCDO.m_DTempMin[day]), NULL);
            j_db_entry_set_field(entry, "daily_globalMax",
                                 &JuleaCDO.m_DTempMax[day],
                                 sizeof(JuleaCDO.m_DTempMax[day]), NULL);
            j_db_entry_set_field(entry, "daily_globalMean",
                                 &JuleaCDO.m_DTempMean[day],
                                 sizeof(JuleaCDO.m_DTempMean[day]), NULL);
            j_db_entry_set_field(entry, "daily_globalSum",
                                 &JuleaCDO.m_NotComputedValue,
                                 sizeof(JuleaCDO.m_NotComputedValue), NULL);
            j_db_entry_set_field(
                entry, "daily_globalVar", &JuleaCDO.m_DTempVar,
                sizeof(JuleaCDO.m_DTempVar),
                NULL); // TODO: error when daily value is accessed
        }
        if (varName == JuleaCDO.m_PrecipitationName)
        {
            j_db_entry_set_field(entry, "daily_globalMin",
                                 &JuleaCDO.m_DPrecMin[day],
                                 sizeof(JuleaCDO.m_DPrecMin[day]), NULL);
            j_db_entry_set_field(entry, "daily_globalMax",
                                 &JuleaCDO.m_DPrecMax[day],
                                 sizeof(JuleaCDO.m_DPrecMax[day]), NULL);
            j_db_entry_set_field(entry, "daily_globalMean",
                                 &JuleaCDO.m_DPrecMean[day],
                                 sizeof(JuleaCDO.m_DPrecMean[day]), NULL);
            j_db_entry_set_field(entry, "daily_globalSum",
                                 &JuleaCDO.m_DPrecSum[day],
                                 sizeof(JuleaCDO.m_DPrecSum[day]), NULL);
            // j_db_entry_set_field(entry, "daily_globalVar",
            //  &JuleaCDO.m_DPrecVar[day],
            //  sizeof(JuleaCDO.m_DPrecVar[day]), NULL); //TODO: some error...
        }
        // // monthly values
        // else if (day == JuleaCDO.m_StoreMonthlyValue)

        // {
        //     if (varName == JuleaCDO.m_TemperatureName)
        //     {
        //         j_db_entry_set_field(entry, "daily_globalMin",
        //                              &JuleaCDO.m_MTempMin,
        //                              sizeof(JuleaCDO.m_MTempMin), NULL);
        //         j_db_entry_set_field(entry, "daily_globalMax",
        //                              &JuleaCDO.m_MTempMax,
        //                              sizeof(JuleaCDO.m_MTempMax), NULL);
        //         j_db_entry_set_field(entry, "daily_globalMean",
        //                              &JuleaCDO.m_MTempMean,
        //                              sizeof(JuleaCDO.m_MTempMean), NULL);
        //         j_db_entry_set_field(entry, "daily_globalSum",
        //                              &JuleaCDO.m_NotComputedValue,
        //                              sizeof(JuleaCDO.m_NotComputedValue),
        //                              NULL);
        //         j_db_entry_set_field(entry, "daily_globalVar",
        //                              &JuleaCDO.m_MTempVar,
        //                              sizeof(JuleaCDO.m_MTempVar), NULL);
        //     }
        //     if (varName == JuleaCDO.m_PrecipitationName)
        //     {
        //         j_db_entry_set_field(entry, "daily_globalMin",
        //                              &JuleaCDO.m_MPrecMin,
        //                              sizeof(JuleaCDO.m_MPrecMin), NULL);
        //         j_db_entry_set_field(entry, "daily_globalMax",
        //                              &JuleaCDO.m_MPrecMax,
        //                              sizeof(JuleaCDO.m_MPrecMax), NULL);
        //         j_db_entry_set_field(entry, "daily_globalMean",
        //                              &JuleaCDO.m_MPrecMean,
        //                              sizeof(JuleaCDO.m_MPrecMean), NULL);
        //         j_db_entry_set_field(entry, "daily_globalSum",
        //                              &JuleaCDO.m_MPrecSum,
        //                              sizeof(JuleaCDO.m_MPrecSum), NULL);
        //         j_db_entry_set_field(entry, "daily_globalVar",
        //                              &JuleaCDO.m_MPrecVar,
        //                              sizeof(JuleaCDO.m_MPrecVar), NULL);
        //     }
        //     // yearly values
        //     else if (day == JuleaCDO.m_StoreYearlyValue)
        //     {
        //         if (varName == JuleaCDO.m_TemperatureName)
        //         {
        //             j_db_entry_set_field(entry, "daily_globalMin",
        //                                  &JuleaCDO.m_YTempMin,
        //                                  sizeof(JuleaCDO.m_YTempMin), NULL);
        //             j_db_entry_set_field(entry, "daily_globalMax",
        //                                  &JuleaCDO.m_YTempMax,
        //                                  sizeof(JuleaCDO.m_YTempMax), NULL);
        //             j_db_entry_set_field(entry, "daily_globalMean",
        //                                  &JuleaCDO.m_YTempMean,
        //                                  sizeof(JuleaCDO.m_YTempMean), NULL);
        //             j_db_entry_set_field(
        //                 entry, "daily_globalSum",
        //                 &JuleaCDO.m_NotComputedValue,
        //                 sizeof(JuleaCDO.m_NotComputedValue), NULL);
        //             j_db_entry_set_field(entry, "daily_globalVar",
        //                                  &JuleaCDO.m_YTempVar,
        //                                  sizeof(JuleaCDO.m_YTempVar), NULL);
        //         }
        //         if (varName == JuleaCDO.m_PrecipitationName)
        //         {
        //             j_db_entry_set_field(entry, "daily_globalMin",
        //                                  &JuleaCDO.m_YPrecMin,
        //                                  sizeof(JuleaCDO.m_YPrecMin), NULL);
        //             j_db_entry_set_field(entry, "daily_globalMax",
        //                                  &JuleaCDO.m_YPrecMax,
        //                                  sizeof(JuleaCDO.m_YPrecMax), NULL);
        //             j_db_entry_set_field(entry, "daily_globalMean",
        //                                  &JuleaCDO.m_YPrecMean,
        //                                  sizeof(JuleaCDO.m_YPrecMean), NULL);
        //             j_db_entry_set_field(entry, "daily_globalSum",
        //                                  &JuleaCDO.m_YPrecSum,
        //                                  sizeof(JuleaCDO.m_YPrecSum), NULL);
        //             j_db_entry_set_field(entry, "daily_globalVar",
        //                                  &JuleaCDO.m_YPrecVar,
        //                                  sizeof(JuleaCDO.m_YPrecVar), NULL);
        //         }
        //     }
        // }
    }
}

JuleaDBInteractionWriter::JuleaDBInteractionWriter(helper::Comm const &comm)
: JuleaInteraction(std::move(comm))
{
    // std::cout << "This is the constructor of the writer" << std::endl;
}

void JuleaDBInteractionWriter::AddFieldsForTagTable(JDBSchema *schema)
{
    // std::cout << "--- AddFieldsForTagTable ---" << std::endl;
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *stepIndex[] = {"step", NULL};
    gchar const *blockIndex[] = {"block", NULL};

    // j_db_schema_add_field(schema, "projectNamespace", J_DB_TYPE_STRING,
    // NULL);

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "step", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "block", J_DB_TYPE_UINT64, NULL);

    // could be useful to have the entryID directly in tag table
    j_db_schema_add_field(schema, "entryID", J_DB_TYPE_UINT64, NULL);
    j_db_schema_add_field(schema, "stat_i", J_DB_TYPE_UINT32, NULL);
    j_db_schema_add_field(schema, "stat_d", J_DB_TYPE_FLOAT64, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    // std::cout << "--- End of AddFieldsForTagTable ---\n";
}

void JuleaDBInteractionWriter::InitTagTables(std::string projectNamespace)
{
    // std::cout << "--- InitTagTables ---" << std::endl;
    int err = 0;
    size_t db_length = 0;
    gchar *fileName = NULL;
    gchar *tagName = NULL;
    gchar *variableName = NULL;

    JDBType type;
    JDAIStatistic stat;
    JDAIOperator op;
    JDAIGranularity granularity;
    JDAIGranularity *tmpGran = nullptr;
    JDAIStatistic *tmpStat = nullptr;
    JDAIOperator *tmpOp = nullptr;
    double threshold = 0;
    double *tmp = nullptr;

    gchar *completeNamespace = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    completeNamespace = g_strdup_printf("%s_%s_%s", "adios2",
                                        projectNamespace.c_str(), "config");
    auto tagConfigSchema = j_db_schema_new(completeNamespace, "tags", NULL);

    j_db_schema_get(tagConfigSchema, batch, NULL);
    g_assert_true(j_batch_execute(batch) == true);

    auto selector =
        j_db_selector_new(tagConfigSchema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "projectNamespace",
                            J_DB_SELECTOR_OPERATOR_EQ, projectNamespace.c_str(),
                            strlen(projectNamespace.c_str()) + 1, NULL);
    auto iterator = j_db_iterator_new(tagConfigSchema, selector, NULL);

    while (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "tagName", &type,
                                (gpointer *)&tagName, &db_length, NULL);
        j_db_iterator_get_field(iterator, "file", &type, (gpointer *)&fileName,
                                &db_length, NULL);
        j_db_iterator_get_field(iterator, "variableName", &type,
                                (gpointer *)&variableName, &db_length, NULL);
        j_db_iterator_get_field(iterator, "granularity", &type,
                                (gpointer *)&tmpGran, &db_length, NULL);
        j_db_iterator_get_field(iterator, "statistic", &type,
                                (gpointer *)&tmpStat, &db_length, NULL);
        j_db_iterator_get_field(iterator, "operator", &type, (gpointer *)&tmpOp,
                                &db_length, NULL);
        j_db_iterator_get_field(iterator, "threshold_d", &type,
                                (gpointer *)&tmp, &db_length, NULL);

        threshold = *tmp;
        granularity = *tmpGran;
        stat = *tmpStat;
        op = *tmpOp;
        std::string strFileName = fileName;
        std::string strVarName = variableName;

        std::pair<std::string, std::string> key(strFileName, strVarName);
        adios2::interop::JuleaCDO::Tag currentTag;
        currentTag.m_TagName = tagName;
        currentTag.m_Threshold_d = threshold;
        currentTag.m_Granularity = granularity;
        currentTag.m_Statistic = stat;
        currentTag.m_Operator = op;

        std::list<adios2::interop::JuleaCDO::Tag> tags;
        tags.push_back(currentTag);
        m_Tags.insert({key, tags});

        // create tables for every tag
        auto completeNamespace =
            g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
        auto tagSchema = j_db_schema_new(completeNamespace, tagName, NULL);
        // auto tag_schema = j_db_schema_new(completeNamespace, "tags", NULL);

        j_db_schema_get(tagSchema, batch, NULL);
        bool existsVar = j_batch_execute(batch);

        if (existsVar == 0)
        {
            std::cout << "tag table does not exist yet" << std::endl;
            tagSchema = j_db_schema_new(completeNamespace, tagName, NULL);
            AddFieldsForTagTable(tagSchema);
            j_db_schema_create(tagSchema, batch, NULL);
            g_assert_true(j_batch_execute(batch) == true);
        }
        j_db_schema_unref(tagSchema);
    }
    g_free(tmpGran);
    g_free(tmp);
    g_free(tmpStat);
    g_free(tmpOp);
    j_batch_unref(batch);
    j_db_schema_unref(tagConfigSchema);
    j_db_selector_unref(selector);
    j_db_iterator_unref(iterator);
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
    size_t stepsPerYear = JuleaCDO.m_StepsPerDay * JuleaCDO.m_DaysPerMonth *
                          JuleaCDO.m_MonthsPerYear;
    // size_t month = 0;
    // size_t day = 0;
    // uint32_t entryID = 0;

    if (currentStep >= stepsPerYear)
    {
        year = (size_t)currentStep / stepsPerYear;
    }
    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto completeNamespace =
        g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
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

    j_db_entry_insert(entry, batch2, NULL);
    g_assert_true(j_batch_execute(batch2) == true);
    // j_db_schema_unref(schema);
    // j_db_entry_unref(entry);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}

void JuleaDBInteractionWriter::AddEntriesForDailyGlobalStatsTable(
    const std::string projectNamespace, const std::string fileName,
    const std::string varName, size_t currentStep, interop::JuleaCDO &JuleaCDO,
    int year, int month, int day)
{
    int err = 0;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    // g_autoptr(JDBSelector) selector = NULL;
    // g_autoptr(JDBIterator) iterator = NULL;
    JDBType jdbType;
    guint64 db_length = 0;
    uint32_t *tmpID;

    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto completeNamespace =
        g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
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

    j_db_entry_insert(entry, batch2, NULL);
    g_assert_true(j_batch_execute(batch2) == true);
    // j_db_schema_unref(schema);
    // j_db_entry_unref(entry);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
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
        g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
    // auto tag_schema = j_db_schema_new(completeNamespace, "tags", NULL);
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
        // AddFieldsForVariableMD_Original(varSchema);
        AddFieldsForVariableMD_Eval(varSchema);
        j_db_schema_create(varSchema, batch, NULL);
        g_assert_true(j_batch_execute(batch) == true);
    }

    if (existsBlock == 0)
    {

        // std::cout << "block schema does not exist" << std::endl;
        blockSchema =
            j_db_schema_new(completeNamespace, "block-metadata", NULL);
        // AddFieldsForBlockMD_Original(blockSchema);
        AddFieldsForBlockMD_Eval(blockSchema);
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
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_batch_unref(batch3);
    j_batch_unref(batch4);
    j_semantics_unref(semantics);
}

#define declare_template_instantiation(T)                                      \
    template void JuleaDBInteractionWriter::AddEntriesForTagTable(             \
        const std::string projectNamespace, const std::string tagName,         \
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
