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

JuleaDBInteractionWriter::JuleaDBInteractionWriter(helper::Comm const &comm)
: JuleaInteraction(std::move(comm))
{
    // std::cout << "This is the constructor of the writer" << std::endl;
}

/**
 * One table for all climate indeces that are computed for evaluation
 */
void JuleaDBInteractionWriter::AddFieldsForClimateIndexTable(
    JDBSchema *schema)
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
 * Table for the yearly min/max/mean/sum/var with "local" resolution
 * local resolution = yearly values at the block level
 * global stats: x/y set to -1/-1
 */
void JuleaDBInteractionWriter::AddFieldsForYearlyLocalStatsTable(
    JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *x[] = {"x", NULL};
    gchar const *y[] = {"y", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "year", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "x", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "y", J_DB_TYPE_SINT32, NULL);

    j_db_schema_add_field(schema, "yearly_blockMin", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "yearly_blockMax", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "yearly_blockMean", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "yearly_blockSum", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "yearly_blockVar", J_DB_TYPE_FLOAT64, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, x, NULL);
    j_db_schema_add_index(schema, y, NULL);
}

/**
 * Table for the daily min/max/mean/sum/var with "global" resolution
 * global resolution = daily values for the complete step -> over all processes
 * monthly stats: day set to -1
 * yearly stats: month and day set to -1
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
 * Both local and daily resolution for statistics
 *
 */
void JuleaDBInteractionWriter::AddFieldsForDailyLocalStatsTable(
    JDBSchema *schema)
{
    // FIXME: index choice might not be great
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *x[] = {"x", NULL};
    gchar const *y[] = {"y", NULL};
    gchar const *yearIndex[] = {"year", NULL};
    gchar const *monthIndex[] = {"month", NULL};
    gchar const *dayIndex[] = {"day", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "x", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "y", J_DB_TYPE_SINT32, NULL);

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
    j_db_schema_add_index(schema, x, NULL);
    j_db_schema_add_index(schema, y, NULL);
    j_db_schema_add_index(schema, yearIndex, NULL);
    j_db_schema_add_index(schema, monthIndex, NULL);
    j_db_schema_add_index(schema, dayIndex, NULL);
}

void JuleaDBInteractionWriter::AddFieldsForVariableMDSmall(JDBSchema *schema)
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

    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
    j_db_schema_add_index(schema, meanDoubleIndex, NULL);
}

void JuleaDBInteractionWriter::AddFieldsForBlockMDSmall(JDBSchema *schema)
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
    // j_db_schema_add_field(schema, "value_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "mean_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "sum_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "variance_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "std_float64", J_DB_TYPE_FLOAT64, NULL);

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

void JuleaDBInteractionWriter::AddFieldsForVariableMD(JDBSchema *schema)
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
    // j_db_schema_add_field(schema, "typeString", J_DB_TYPE_STRING, NULL);
    // j_db_schema_add_field(schema, "typeLen", J_DB_TYPE_UINT64, NULL);

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

    // add min/max/value for every type for performance improvement of querying
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
    j_db_schema_add_field(schema, "mean_float64", J_DB_TYPE_FLOAT64, NULL);

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING, NULL);
    // j_db_schema_add_field(schema, "max_string", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "value_string", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, minFloatIndex, NULL);
    j_db_schema_add_index(schema, maxFloatIndex, NULL);
    j_db_schema_add_index(schema, minDoubleIndex, NULL);
    j_db_schema_add_index(schema, maxDoubleIndex, NULL);
    j_db_schema_add_index(schema, meanDoubleIndex, NULL);
}

void JuleaDBInteractionWriter::AddFieldsForBlockMD(JDBSchema *schema)
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

    j_db_schema_add_field(schema, "min_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "max_blob", J_DB_TYPE_BLOB, NULL);
    j_db_schema_add_field(schema, "value_blob", J_DB_TYPE_BLOB, NULL);

    // add min/max/value for every type for performance improvement of querying
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
    j_db_schema_add_field(schema, "mean_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "sum_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "variance_float64", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "std_float64", J_DB_TYPE_FLOAT64, NULL);

    // j_db_schema_add_field(schema, "min_string", J_DB_TYPE_STRING, NULL);
    // j_db_schema_add_field(schema, "max_string", J_DB_TYPE_STRING, NULL);
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

void JuleaDBInteractionWriter::AddEntriesForCDOStatistics(
    const std::string nameSpace, const std::string varName, size_t currentStep,
    interop::JuleaCDO &JuleaCDO)
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
    size_t month = 0;
    size_t day = 0;
    // uint32_t entryID = 0;

    if (currentStep >= JuleaCDO.m_StepsPerYear)
    {
        year = (size_t)currentStep / JuleaCDO.m_StepsPerYear;
    }

    // if(currentStep >= m_StepsPerMonth)
    // {
    //     month = (size_t) currentStep / m_StepsPerMonth;
    // }

    // if(current >= m_StepsPerDay)
    // {
    //     day = (size_t) currentStep / m_StepsPerDay;
    // }

    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    // FIXME: chose correct schema
    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    entry = j_db_entry_new(schema, NULL);
    j_db_entry_set_field(entry, "file", nameSpace.c_str(),
                         strlen(nameSpace.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "variableName", varName.c_str(),
                         strlen(varName.c_str()) + 1, NULL);

    j_db_entry_set_field(entry, "year", &year, sizeof(size_t), NULL);
    // j_db_entry_set_field(entry, "year", &stepID, sizeof(stepID), NULL);
    // j_db_entry_set_field(entry, "year", &stepID, sizeof(stepID), NULL);
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

void JuleaDBInteractionWriter::AddEntriesForYearlyLocalStatsTable(
    const std::string nameSpace, const std::string varName, size_t currentStep,
    interop::JuleaCDO &JuleaCDO, int writerRank)
    {
        int err = 0;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    JDBType jdbType;
    guint64 db_length = 0;
    uint32_t *tmpID;
    // size_t year = 0;
    // size_t month = 0;
    // size_t day = 0;
    // uint32_t entryID = 0;
    int x = 0;
    int y = 0;
    int year = 0;

    //TODO: find correct x/y coordinates
    JuleaCDO.computeCoordinatesFromRank(writerRank, x, y);
        if (currentStep >= JuleaCDO.m_StepsPerYear)
    {
        year = (size_t)currentStep / JuleaCDO.m_StepsPerYear;
    }

    // ComputeYearlyLocalStats();

    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    // FIXME: chose correct schema
    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    entry = j_db_entry_new(schema, NULL);
    j_db_entry_set_field(entry, "file", nameSpace.c_str(),
                         strlen(nameSpace.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "variableName", varName.c_str(),
                         strlen(varName.c_str()) + 1, NULL);

    j_db_entry_set_field(entry, "year", &year, sizeof(year), NULL);
    j_db_entry_set_field(entry, "x", &x, sizeof(x), NULL);
    j_db_entry_set_field(entry, "y", &y, sizeof(y), NULL);

    // j_db_entry_set_field(entry, "yearly_blockMin", &variable, sizeof(), NULL);
    // j_db_entry_set_field(entry, "yearly_blockMax", &variable, sizeof(), NULL);
    // j_db_entry_set_field(entry, "yearly_blockMean", &variable, sizeof(), NULL);
    // j_db_entry_set_field(entry, "yearly_blockSum", &variable, sizeof(), NULL);
    // j_db_entry_set_field(entry, "yearly_blockVar", &variable, sizeof(), NULL);
    }

void JuleaDBInteractionWriter::InitDBSchemas()
{
    // std::cout << "--- InitDBSchemas ---" << std::endl;
    int err = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);
    g_autoptr(JDBSchema) varSchema = NULL;
    g_autoptr(JDBSchema) blockSchema = NULL;

    varSchema = j_db_schema_new("adios2", "variable-metadata", NULL);
    blockSchema = j_db_schema_new("adios2", "block-metadata", NULL);

    j_db_schema_get(varSchema, batch, NULL);
    bool existsVar = j_batch_execute(batch);
    j_db_schema_get(blockSchema, batch, NULL);

    bool existsBlock = j_batch_execute(batch);
    // std::cout << "existsVar: " << existsVar << " existsBlock: " <<
    // existsBlock
    // << std::endl;

    if (existsVar == 0)
    {
        // std::cout << "variable schema does not exist" << std::endl;
        varSchema = j_db_schema_new("adios2", "variable-metadata", NULL);
        // DAIaddFieldsForVariableMDSmall(varSchema);
        AddFieldsForVariableMD(varSchema);
        j_db_schema_create(varSchema, batch, NULL);
        g_assert_true(j_batch_execute(batch) == true);
    }

    if (existsBlock == 0)
    {

        // std::cout << "block schema does not exist" << std::endl;
        blockSchema = j_db_schema_new("adios2", "block-metadata", NULL);
        // DAIaddFieldsForBlockMDSmall(blockSchema);
        AddFieldsForBlockMD(blockSchema);
        j_db_schema_create(blockSchema, batch2, NULL);
        g_assert_true(j_batch_execute(batch2) == true);
    }

    // g_assert_true(j_batch_execute(batch2) == true);
    // j_db_schema_unref(varSchema);
    // j_db_schema_unref(blockSchema);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}

#define declare_template_instantiation(T)                                      \
    template void JuleaDBInteractionWriter::PutVariableMetadataToJulea(        \
        core::Variable<T> &variable, const std::string nameSpace,              \
        const std::string varName, size_t currStep, size_t block);             \
    template void JuleaDBInteractionWriter::PutBlockMetadataToJulea(           \
        core::Variable<T> &variable, const std::string nameSpace,              \
        const std::string varName, size_t step, size_t block,                  \
        const typename core::Variable<T>::Info &blockInfo, T &blockMin,        \
        T &blockMax, T &blockMean, uint32_t &entryID);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios2
