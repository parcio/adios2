/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaDB-DO-InteractionReader.h"

// #include "JuleaFormatReader.h" //for ParseVariableFromBSON
#include "JuleaDB-DO-Reader.h"

#include <bson.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include <julea-db.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

void DB_DO_setMinMaxValueFields(std::string *minField, std::string *maxField,
                                std::string *valueField, const char *varType)
{

    if ((strcmp(varType, "char") == 0) || (strcmp(varType, "int8_t") == 0) ||
        (strcmp(varType, "uint8_t") == 0) ||
        (strcmp(varType, "int16_t") == 0) ||
        (strcmp(varType, "uint16_t") == 0) || (strcmp(varType, "int32_t") == 0))
    {
        *minField = "min_sint32";
        *maxField = "max_sint32";
        *valueField = "value_sint32";
    }
    else if (strcmp(varType, "uint32_t") == 0)
    {
        *minField = "min_uint32";
        *maxField = "max_uint32";
        *valueField = "value_uint32";
    }
    else if (strcmp(varType, "int64_t") == 0)
    {
        *minField = "min_sint64";
        *maxField = "max_sint64";
        *valueField = "value_sint64";
    }
    else if (strcmp(varType, "uint64_t") == 0)
    {
        *minField = "min_uint64";
        *maxField = "max_uint64";
        *valueField = "value_uint64";
    }
    else if (strcmp(varType, "float") == 0)
    {
        *minField = "min_float32";
        *maxField = "max_float32";
        *valueField = "value_float32";
    }
    else if (strcmp(varType, "double") == 0)
    {
        *minField = "min_float64";
        *maxField = "max_float64";
        *valueField = "value_float64";
    }
    else if (strcmp(varType, "string") == 0)
    {
        *valueField = "value_sint32";
    }

    else if ((strcmp(varType, "long double") == 0) ||
             (strcmp(varType, "float complex") == 0) ||
             (strcmp(varType, "double complex") == 0))
    {
        *minField = "min_blob";
        *maxField = "max_blob";
        *valueField = "value_blob";
    }
}

void DB_DO_InitVariable(core::IO *io, core::Engine &engine,
                        std::string nameSpace, std::string varName,
                        size_t *blocks, size_t numberSteps, ShapeID shapeID,
                        bool isReadAsJoined, bool isReadAsLocalValue,
                        bool isRandomAccess, bool isSingleValue)
{
    // std::cout << "----- InitVariable --- " << varName << std::endl;
    const std::string type(io->InquireVariableType(varName));

    int err = 0;
    uint32_t entryID = 0;
    uint32_t *tmpID;
    size_t step;
    size_t block;

    JDBType jdbType;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;

    g_autoptr(JDBSchema) blockSchema = NULL;
    g_autoptr(JDBSchema) varSchema = NULL;
    // JDBSchema *schema = NULL;
    // JDBEntry *entry = NULL;
    // JDBIterator *iterator = NULL;
    // g_autoptr(JDBIterator) iterator = NULL;
    // JDBSelector *selector = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    blockSchema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(blockSchema, batch, NULL);
    err = j_batch_execute(batch);
    varSchema = j_db_schema_new("adios2", "variable-metadata", NULL);
    j_db_schema_get(varSchema, batch, NULL);
    err = j_batch_execute(batch);
    std::string minField;
    std::string maxField;
    std::string valueField;

    /** AvailableStepBlockIndexOffsets stores the entries (= blocks) _id (= line
     * in the sql table) */
    if (type == "compound")
    {
    }
#define declare_type(T)                                                        \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        auto var = io->InquireVariable<T>(varName);                            \
        var->m_ShapeID = shapeID;                                              \
        for (size_t i = 0; i < numberSteps; i++)                               \
        {                                                                      \
            for (size_t j = 0; j < blocks[i]; j++)                             \
            {                                                                  \
                step = i;                                                      \
                block = j;                                                     \
                g_autoptr(JDBSelector) selector = j_db_selector_new(           \
                    blockSchema, J_DB_SELECTOR_MODE_AND, NULL);                \
                j_db_selector_add_field(                                       \
                    selector, "file", J_DB_SELECTOR_OPERATOR_EQ,               \
                    nameSpace.c_str(), strlen(nameSpace.c_str()) + 1, NULL);   \
                j_db_selector_add_field(                                       \
                    selector, "variableName", J_DB_SELECTOR_OPERATOR_EQ,       \
                    varName.c_str(), strlen(varName.c_str()) + 1, NULL);       \
                j_db_selector_add_field(selector, "step",                      \
                                        J_DB_SELECTOR_OPERATOR_EQ, &step,      \
                                        sizeof(step), NULL);                   \
                j_db_selector_add_field(selector, "block",                     \
                                        J_DB_SELECTOR_OPERATOR_EQ, &block,     \
                                        sizeof(block), NULL);                  \
                g_autoptr(JDBIterator) iterator =                              \
                    j_db_iterator_new(blockSchema, selector, NULL);            \
                while (j_db_iterator_next(iterator, NULL))                     \
                {                                                              \
                    j_db_iterator_get_field(iterator, "_id", &jdbType,         \
                                            (gpointer *)&tmpID, &db_length,    \
                                            NULL);                             \
                    entryID = *tmpID;                                          \
                }                                                              \
                var->m_AvailableStepBlockIndexOffsets[i + 1].push_back(        \
                    entryID);                                                  \
                g_free(tmpID);                                                 \
            }                                                                  \
            var->m_AvailableStepsCount++;                                      \
        }                                                                      \
                                                                               \
        DB_DO_setMinMaxValueFields(&minField, &maxField, &valueField,          \
                                   type.c_str());                              \
        g_autoptr(JDBSelector) selector =                                      \
            j_db_selector_new(varSchema, J_DB_SELECTOR_MODE_AND, NULL);        \
                                                                               \
        j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,   \
                                nameSpace.c_str(),                             \
                                strlen(nameSpace.c_str()) + 1, NULL);          \
        j_db_selector_add_field(selector, "variableName",                      \
                                J_DB_SELECTOR_OPERATOR_EQ, varName.c_str(),    \
                                strlen(varName.c_str()) + 1, NULL);            \
                                                                               \
        g_autoptr(JDBIterator) iterator =                                      \
            j_db_iterator_new(varSchema, selector, NULL);                      \
        while (j_db_iterator_next(iterator, NULL))                             \
        {                                                                      \
            T *min;                                                            \
            T *max;                                                            \
            j_db_iterator_get_field(iterator, minField.c_str(), &jdbType,      \
                                    (gpointer *)&min, &db_length, NULL);       \
            var->m_Min = *min;                                                 \
            j_db_iterator_get_field(iterator, maxField.c_str(), &jdbType,      \
                                    (gpointer *)&max, &db_length, NULL);       \
            var->m_Max = *max;                                                 \
            g_free(min);                                                       \
            g_free(max);                                                       \
        }                                                                      \
                                                                               \
        var->m_AvailableStepsStart = 0;                                        \
        var->m_StepsStart = 0;                                                 \
        var->m_Engine = &engine;                                               \
        var->m_FirstStreamingStep = true;                                      \
        var->m_ReadAsJoined = isReadAsJoined;                                  \
        var->m_ReadAsLocalValue = isReadAsLocalValue;                          \
        var->m_RandomAccess = isRandomAccess;                                  \
        var->m_SingleValue = isSingleValue;                                    \
                                                                               \
        if (var->m_ShapeID == ShapeID::LocalValue)                             \
        {                                                                      \
            var->m_ShapeID = ShapeID::GlobalArray;                             \
            var->m_SingleValue = true;                                         \
        }                                                                      \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    // the ShapeID is set to global array so that displaying it in bpls looks
    // like the rest
    // std::cout << "This is the tricky part! " << std::endl;             \

    j_batch_unref(batch);
    // j_db_schema_unref(schema);
    // j_db_selector_unref(selector);
    // j_db_entry_unref(entry);
    // j_db_iterator_unref(iterator);

    j_semantics_unref(semantics);
}

void DB_DO_DefineVariableInEngineIO(core::IO *io, const std::string varName,
                                    std::string type, ShapeID shapeID,
                                    Dims shape, Dims start, Dims count,
                                    bool constantDims, bool isLocalValue)
{
    // variable->m_AvailableShapes[characteristics.Statistics.Step] = \
                //     variable->m_Shape;                                         \

    if (type == "compound")
    {
    }
#define declare_type(T)                                                        \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        core::Variable<T> *variable = nullptr;                                 \
        {                                                                      \
            switch (shapeID)                                                   \
            {                                                                  \
            case (ShapeID::GlobalValue): {                                     \
                variable = &io->DefineVariable<T>(varName);                    \
                break;                                                         \
            }                                                                  \
            case (ShapeID::GlobalArray): {                                     \
                variable = &io->DefineVariable<T>(                             \
                    varName, shape, Dims(shape.size(), 0), shape);             \
                break;                                                         \
            }                                                                  \
            case (ShapeID::LocalValue): {                                      \
                variable = &io->DefineVariable<T>(varName, {1}, {0}, {1});     \
                variable->m_ShapeID = ShapeID::LocalValue;                     \
                break;                                                         \
            }                                                                  \
            case (ShapeID::LocalArray): {                                      \
                variable = &io->DefineVariable<T>(varName, {}, {}, count);     \
                break;                                                         \
            }                                                                  \
            default:                                                           \
                throw std::runtime_error("ERROR: invalid ShapeID or not yet "  \
                                         "supported for variable " +           \
                                         varName + ", in call to Open\n");     \
            }                                                                  \
        }                                                                      \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
}

void DB_DO_DefineVariableInInit(core::IO *io, const std::string varName,
                                std::string stringType, Dims shape, Dims start,
                                Dims count, bool constantDims,
                                bool isLocalValue)
{
    const char *type = stringType.c_str();
    // std::cout << "------ DefineVariableInInit ----------" << std::endl;
    // std::cout << "------ type  ---------- " << type << std::endl;
    // std::cout << "------ constantDims  ---------- " << constantDims
    // << std::endl;

    if (strcmp(type, "unknown") == 0)
    {
        // TODO
    }
    else if (strcmp(type, "compound") == 0)
    {
    }
    else if (strcmp(type, "string") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<std::string>(varName, shape, start,
                                                        count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<std::string>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "int8_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int8_t>(varName, shape, start, count,
                                                   constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int8_t>(varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "uint8_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint8_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint8_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "int16_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int16_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int16_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "uint16_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint16_t>(varName, shape, start,
                                                     count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint16_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "int32_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int32_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int32_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "uint32_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint32_t>(varName, shape, start,
                                                     count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint32_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "int64_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<int64_t>(varName, shape, start,
                                                    count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<int64_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "uint64_t") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<uint64_t>(varName, shape, start,
                                                     count, constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<uint64_t>(varName, {adios2::LocalValueDim});
        }
    }
    else if (strcmp(type, "float") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<float>(varName, shape, start, count,
                                                  constantDims);
        }
        else
        {
            auto &var =
                io->DefineVariable<float>(varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "double") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<double>(varName, shape, start, count,
                                                   constantDims);
        }
        else
        {
            // std::cout << "Single Value double " << std::endl;
            auto &var = io->DefineVariable<double>(varName, {1}, {0}, {1});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "long double") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<long double>(varName, shape, start,
                                                        count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<long double>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex float") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<std::complex<float>>(
                varName, shape, start, count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<std::complex<float>>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }
    else if (strcmp(type, "complex double") == 0)
    {
        if (!isLocalValue)
        {
            auto &var = io->DefineVariable<std::complex<double>>(
                varName, shape, start, count, constantDims);
        }
        else
        {
            auto &var = io->DefineVariable<std::complex<double>>(
                varName, {adios2::LocalValueDim});
        }
        // std::cout << "Defined variable of type: " << type << std::endl;
    }

    // std::map<std::string, Params> varMap = io->GetAvailableVariables();

    // for (std::map<std::string, Params>::iterator it = varMap.begin();
    //      it != varMap.end(); ++it)
    // {

    //     // std::cout << "first: " << it->first << " => " <<
    //     it->second.begin()
    //     // << '\n';
    //     // std::cout << "first: " << it->first << '\n';
    // }
}

void DB_DO_CheckSchemas()
{
    // std::cout << "--- CheckSchemas" << std::endl;
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

    if ((existsVar == 0) || (existsBlock == 0))
    {
        std::cout << "ERROR: database adios2 schemas do not exist" << std::endl;
    }
}

void InitVariablesFromDB_DO(const std::string nameSpace, core::IO *io,
                            core::Engine &engine)
{
    // std::cout << "--- InitVariablesFromDB ---" << std::endl;
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    g_autoptr(JDBSelector) selector = NULL;

    char *varName;
    char *varTypePtr;
    std::string varType;

    // bool localValue;
    bool *isConstantDims;
    bool *isReadAsJoined;
    bool *isReadAsLocalValue;
    bool *isRandomAccess;
    bool *isSingleValue;

    // size_t typeLen;
    size_t *blocks;
    size_t *numberSteps;
    size_t *shapeSize;
    size_t *startSize;
    size_t *countSize;

    ShapeID *shapeID;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "variable-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
                            nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
                            NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);

    while (j_db_iterator_next(iterator, NULL))
    {
        Dims shape;
        Dims start;
        Dims count;
        // localValue = false;
        j_db_iterator_get_field(iterator, "variableName", &type,
                                (gpointer *)&varName, &db_length, NULL);

        j_db_iterator_get_field(iterator, "isConstantDims", &type,
                                (gpointer *)&isConstantDims, &db_length, NULL);
        j_db_iterator_get_field(iterator, "isReadAsJoined", &type,
                                (gpointer *)&isReadAsJoined, &db_length, NULL);
        j_db_iterator_get_field(iterator, "isReadAsLocalValue", &type,
                                (gpointer *)&isReadAsLocalValue, &db_length,
                                NULL);
        j_db_iterator_get_field(iterator, "isRandomAccess", &type,
                                (gpointer *)&isRandomAccess, &db_length, NULL);
        j_db_iterator_get_field(iterator, "isSingleValue", &type,
                                (gpointer *)&isSingleValue, &db_length, NULL);

        j_db_iterator_get_field(iterator, "shapeID", &type,
                                (gpointer *)&shapeID, &db_length, NULL);
        j_db_iterator_get_field(iterator, "type", &type,
                                (gpointer *)&varTypePtr, &db_length, NULL);
        std::string varType(varTypePtr);

        j_db_iterator_get_field(iterator, "shapeSize", &type,
                                (gpointer *)&shapeSize, &db_length, NULL);
        if (*shapeSize > 0)
        {
            size_t *tmpShapeBuffer;
            j_db_iterator_get_field(iterator, "shape", &type,
                                    (gpointer *)&tmpShapeBuffer, &db_length,
                                    NULL);
            Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + *shapeSize);
            shape = tmpShape;
            g_free(tmpShapeBuffer);
        }

        j_db_iterator_get_field(iterator, "startSize", &type,
                                (gpointer *)&startSize, &db_length, NULL);
        if (*startSize > 0)
        {
            size_t *tmpStartBuffer;
            j_db_iterator_get_field(iterator, "start", &type,
                                    (gpointer *)&tmpStartBuffer, &db_length,
                                    NULL);
            Dims tmpStart(tmpStartBuffer, tmpStartBuffer + *startSize);
            start = tmpStart;
            g_free(tmpStartBuffer);
        }
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t *tmpCountBuffer;
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);

            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
            count = tmpCount;
            g_free(tmpCountBuffer);
        }
        j_db_iterator_get_field(iterator, "numberSteps", &type,
                                (gpointer *)&numberSteps, &db_length, NULL);
        size_t *tmpblocks[*numberSteps];
        if (*numberSteps > 0)
        {
            j_db_iterator_get_field(iterator, "blockArray", &type,
                                    (gpointer *)tmpblocks, &db_length, NULL);
            blocks = *tmpblocks;
            // memcpy(blocks, *tmpblocks, sizeof(*tmpblocks));
        }

        if (false)
        {
            // std::cout << "numberSteps: " << blocks[0] << std::endl;
            // std::cout << "numberSteps: " << blocks[1] << std::endl;
            std::cout << "\nvarName = " << varName << std::endl;
            std::cout << "length: " << db_length << std::endl;
            std::cout << "constantDims: " << *isConstantDims << std::endl;
            std::cout << "isReadAsJoined: " << *isReadAsJoined << std::endl;
            std::cout << "isReadAsLocalValue: " << *isReadAsLocalValue
                      << std::endl;
            std::cout << "isRandomAccess: " << *isRandomAccess << std::endl;
            std::cout << "isSingleValue: " << *isSingleValue << std::endl;
            std::cout << "shapeID: " << *shapeID << std::endl;
            std::cout << "varType2: " << varTypePtr << std::endl;
            std::cout << "shapeSize: " << *shapeSize << std::endl;
            std::cout << "startSize: " << *startSize << std::endl;
            std::cout << "countSize: " << *countSize << std::endl;
            std::cout << "count: " << count.front() << std::endl;
            std::cout << "numberSteps: " << *numberSteps << std::endl;
        }

        if (*shapeID == ShapeID::LocalValue)
        {
            // std::cout << " SHAPEID: LOCAL VALUE" << std::endl;
            // localValue = true;
        }
        // // FIXME: localValueDim is screwing everything up
        // if (strcmp(varName, "time") == 0)
        // {
        //     std::cout << "\n FIXME: time\n" << std::endl;
        // }

        DB_DO_DefineVariableInEngineIO(io, varName, varType, *shapeID, shape,
                                       start, count, *isConstantDims,
                                       *isSingleValue);
        // DB_DO_DefineVariableInInit(io, varName, varType, shape, start, count,
        //                            *isConstantDims, *isSingleValue);
        DB_DO_InitVariable(io, engine, nameSpace, varName, blocks, *numberSteps,
                           *shapeID, *isReadAsJoined, *isReadAsLocalValue,
                           *isRandomAccess, *isSingleValue);
        if (*numberSteps > 0)
        {
            g_free(*tmpblocks);
        }
        g_free(isConstantDims);
        g_free(isReadAsJoined);
        g_free(isReadAsLocalValue);
        g_free(isRandomAccess);
        g_free(isSingleValue);
        g_free(varName);
        g_free(shapeID);
        g_free(varTypePtr);
        g_free(shapeSize);
        g_free(startSize);
        g_free(countSize);
        g_free(numberSteps);
    }
    // j_db_iterator_unref(iterator);
    // j_db_entry_unref(entry);
    // j_db_schema_unref(schema);
    // j_db_selector_unref(selector);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void DB_DO_GetCountFromBlockMetadata(const std::string nameSpace,
                                     const std::string varName, size_t step,
                                     size_t block, Dims *count, size_t entryID,
                                     bool isLocalValue, T *value)
{
    // std::cout << "------ GetCountFromBlockMetadata ----------" << std::endl;
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBSelector) selectorShort = NULL;
    std::string valueField;
    const char *varType;

    size_t *countSize;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    // selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    // j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
    //                         nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
    //                         NULL);
    // j_db_selector_add_field(selector, "variableName",
    // J_DB_SELECTOR_OPERATOR_EQ,
    //                         varName.c_str(), strlen(varName.c_str()) + 1,
    //                         NULL);
    // j_db_selector_add_field(selector, "step", J_DB_SELECTOR_OPERATOR_EQ,
    // &step,
    //                         sizeof(step), NULL);
    // j_db_selector_add_field(selector, "block", J_DB_SELECTOR_OPERATOR_EQ,
    //                         &block, sizeof(block), NULL);
    // iterator = j_db_iterator_new(schema, selector, NULL);

    selectorShort = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selectorShort, "_id", J_DB_SELECTOR_OPERATOR_EQ,
                            &entryID, sizeof(entryID), NULL);

    iterator = j_db_iterator_new(schema, selectorShort, NULL);
    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t *tmpCountBuffer;
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);
            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
            *count = tmpCount;
            g_free(tmpCountBuffer);
        }
        // if(isLocalValue)
        // {
        //     //FIXME: not yet tested!
        //     setMinMaxValueFields(NULL, NULL,valueField, varType );
        //     std::cout << "valueField: " << valueField << std::endl;
        //     j_db_iterator_get_field(iterator, valueField.c_str(), &type,
        //                             (gpointer *)&value, &db_length,
        //                             NULL);
        // }
    }
    g_free(countSize);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void DB_DO_GetBlockMetadataNEW(Variable<T> &variable,
                               typename core::Variable<T>::Info &blockInfo,
                               size_t entryID)
{
    // std::cout << "--- DBGetBlockMetadata ---" << std::endl;
    // std::unique_ptr<typename Variable<T>::Info> blockInfo(
    // new (typename Variable<T>::Info));
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    // g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    // g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBSelector) selectorShort = NULL;

    // const char *varName = variableName.c_str();
    const char *varName = variable.m_Name.c_str();

    bool *isValue;
    T *min;
    T *max;
    T *value;
    size_t *blockID;
    size_t *shapeSize;
    size_t *startSize;
    size_t *countSize;
    size_t *memoryStartSize;
    size_t *memoryCountSize;
    size_t *stepsStart;
    size_t *stepsCount;

    Dims shape;
    Dims start;
    Dims count;
    Dims memoryStart;
    Dims memoryCount;
    ShapeID *shapeID;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    selectorShort = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selectorShort, "_id", J_DB_SELECTOR_OPERATOR_EQ,
                            &entryID, sizeof(entryID), NULL);

    iterator = j_db_iterator_new(schema, selectorShort, NULL);
    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "shapeSize", &type,
                                (gpointer *)&shapeSize, &db_length, NULL);
        if (*shapeSize > 0)
        {
            size_t *tmpShapeBuffer;
            j_db_iterator_get_field(iterator, "shape", &type,
                                    (gpointer *)&tmpShapeBuffer, &db_length,
                                    NULL);
            Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + *shapeSize);
            blockInfo.Shape = tmpShape;
            g_free(tmpShapeBuffer);
        }

        j_db_iterator_get_field(iterator, "startSize", &type,
                                (gpointer *)&startSize, &db_length, NULL);
        if (*startSize > 0)
        {
            size_t *tmpStartBuffer;
            j_db_iterator_get_field(iterator, "start", &type,
                                    (gpointer *)&tmpStartBuffer, &db_length,
                                    NULL);
            Dims tmpStart(tmpStartBuffer, tmpStartBuffer + *startSize);
            blockInfo.Start = tmpStart;
            g_free(tmpStartBuffer);
        }
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t *tmpCountBuffer;
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);
            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
            blockInfo.Count = tmpCount;
            g_free(tmpCountBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryStartSize", &type,
                                (gpointer *)&memoryStartSize, &db_length, NULL);
        if (*memoryStartSize > 0)
        {
            size_t *tmpMemoryStartBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryStartBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryStart(tmpMemoryStartBuffer,
                                tmpMemoryStartBuffer + *memoryStartSize);
            blockInfo.MemoryStart = tmpMemoryStart;
            g_free(tmpMemoryStartBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryCountSize", &type,
                                (gpointer *)&memoryCountSize, &db_length, NULL);
        if (*memoryCountSize > 0)
        {
            size_t *tmpMemoryCountBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryCountBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryCount(tmpMemoryCountBuffer,
                                tmpMemoryCountBuffer + *memoryCountSize);
            blockInfo.MemoryCount = tmpMemoryCount;
            g_free(tmpMemoryCountBuffer);
        }

        // std::string variableType = variable.m_Type;
        // const char *varType = variableType.c_str();
        const char *varType = variable.m_Type.c_str();
        std::string minField;
        std::string maxField;
        std::string valueField;

        DB_DO_setMinMaxValueFields(&minField, &maxField, &valueField, varType);

        j_db_iterator_get_field(iterator, minField.c_str(), &type,
                                (gpointer *)&min, &db_length, NULL);
        blockInfo.Min = *min;
        j_db_iterator_get_field(iterator, maxField.c_str(), &type,
                                (gpointer *)&max, &db_length, NULL);
        blockInfo.Max = *max;
        j_db_iterator_get_field(iterator, "isValue", &type,
                                (gpointer *)&isValue, &db_length, NULL);

        blockInfo.IsValue = *isValue;
        if (isValue)
        {
            // std::cout << "Get Value from DB" << std::endl;
            j_db_iterator_get_field(iterator, valueField.c_str(), &type,
                                    (gpointer *)&value, &db_length, NULL);
            blockInfo.Value = *value;
        }
        j_db_iterator_get_field(iterator, "stepsStart", &type,
                                (gpointer *)&stepsStart, &db_length, NULL);
        blockInfo.StepsStart = *stepsStart;
        j_db_iterator_get_field(iterator, "stepsCount", &type,
                                (gpointer *)&stepsCount, &db_length, NULL);
        blockInfo.StepsCount = *stepsCount;
        j_db_iterator_get_field(iterator, "blockID", &type,
                                (gpointer *)&blockID, &db_length, NULL);
        blockInfo.BlockID = *blockID;

        if (false)
        {
            // std::cout << "shapeSize: " << *shapeSize << std::endl;
            // std::cout << "startSize: " << *startSize << std::endl;
            // std::cout << "countSize: " << *countSize << std::endl;
            // std::cout << "memoryStartSize: " << *memoryStartSize <<
            // std::endl; std::cout << "memoryCountSize: " << *memoryCountSize
            // << std::endl; std::cout << "info->Min: " << info->Min <<
            // std::endl; std::cout << "info->Max: " << info->Max << std::endl;
            // std::cout << "info->Value: " << info->Value << std::endl;
            // std::cout << "info->StepsStart: " << info->StepsStart <<
            // std::endl; std::cout << "info->StepsCount: " << info->StepsCount
            // << std::endl; std::cout << "info->BlockID: " << info->BlockID <<
            // std::endl; std::cout << "info->IsValue: " << info->IsValue <<
            // std::endl;
        }
        if (isValue)
        {
            g_free(value);
        }
        g_free(isValue);
        g_free(min);
        g_free(max);
        g_free(shapeSize);
        g_free(startSize);
        g_free(countSize);
        g_free(memoryStartSize);
        g_free(memoryCountSize);
        g_free(stepsStart);
        g_free(stepsCount);
        g_free(blockID);
        j_batch_unref(batch);
        j_semantics_unref(semantics);
    }
}

// TODO: remove step, block from parameter list
template <class T>
std::unique_ptr<typename core::Variable<T>::Info> DB_DO_GetBlockMetadata(
    const core::Variable<T> &variable,
    // const std::string nameSpace, size_t step, size_t block,
    size_t entryID)
{
    // std::cout << "--- DBGetBlockMetadata ---" << std::endl;
    std::unique_ptr<typename Variable<T>::Info> info(
        new (typename Variable<T>::Info));
    int err = 0;
    JDBType type;
    guint64 db_length = 0;
    g_autofree gchar *db_field = NULL;
    g_autoptr(JDBSchema) schema = NULL;
    // g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    // g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBSelector) selectorShort = NULL;

    const char *varName = variable.m_Name.c_str();

    bool *isValue;
    T *min;
    T *max;
    T *value;
    size_t *blockID;
    size_t *shapeSize;
    size_t *startSize;
    size_t *countSize;
    size_t *memoryStartSize;
    size_t *memoryCountSize;
    size_t *stepsStart;
    size_t *stepsCount;

    Dims shape;
    Dims start;
    Dims count;
    Dims memoryStart;
    Dims memoryCount;
    ShapeID *shapeID;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    // selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    // j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
    //                         nameSpace.c_str(), strlen(nameSpace.c_str()) + 1,
    //                         NULL);
    // j_db_selector_add_field(selector, "variableName",
    // J_DB_SELECTOR_OPERATOR_EQ,
    //                         varName, strlen(varName) + 1, NULL);
    // j_db_selector_add_field(selector, "step", J_DB_SELECTOR_OPERATOR_EQ,
    // &step,
    //                         sizeof(step), NULL);
    // j_db_selector_add_field(selector, "block", J_DB_SELECTOR_OPERATOR_EQ,
    //                         &block, sizeof(block), NULL);
    // iterator = j_db_iterator_new(schema, selector, NULL);

    selectorShort = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selectorShort, "_id", J_DB_SELECTOR_OPERATOR_EQ,
                            &entryID, sizeof(entryID), NULL);

    iterator = j_db_iterator_new(schema, selectorShort, NULL);
    if (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "shapeSize", &type,
                                (gpointer *)&shapeSize, &db_length, NULL);
        if (*shapeSize > 0)
        {
            size_t *tmpShapeBuffer;
            j_db_iterator_get_field(iterator, "shape", &type,
                                    (gpointer *)&tmpShapeBuffer, &db_length,
                                    NULL);
            Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + *shapeSize);
            info->Shape = tmpShape;
            g_free(tmpShapeBuffer);
        }

        j_db_iterator_get_field(iterator, "startSize", &type,
                                (gpointer *)&startSize, &db_length, NULL);
        if (*startSize > 0)
        {
            size_t *tmpStartBuffer;
            j_db_iterator_get_field(iterator, "start", &type,
                                    (gpointer *)&tmpStartBuffer, &db_length,
                                    NULL);
            Dims tmpStart(tmpStartBuffer, tmpStartBuffer + *startSize);
            info->Start = tmpStart;
            g_free(tmpStartBuffer);
        }
        j_db_iterator_get_field(iterator, "countSize", &type,
                                (gpointer *)&countSize, &db_length, NULL);
        if (*countSize > 0)
        {
            size_t *tmpCountBuffer;
            j_db_iterator_get_field(iterator, "count", &type,
                                    (gpointer *)&tmpCountBuffer, &db_length,
                                    NULL);
            Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
            info->Count = tmpCount;
            g_free(tmpCountBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryStartSize", &type,
                                (gpointer *)&memoryStartSize, &db_length, NULL);
        if (*memoryStartSize > 0)
        {
            size_t *tmpMemoryStartBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryStartBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryStart(tmpMemoryStartBuffer,
                                tmpMemoryStartBuffer + *memoryStartSize);
            info->MemoryStart = tmpMemoryStart;
            g_free(tmpMemoryStartBuffer);
        }
        j_db_iterator_get_field(iterator, "memoryCountSize", &type,
                                (gpointer *)&memoryCountSize, &db_length, NULL);
        if (*memoryCountSize > 0)
        {
            size_t *tmpMemoryCountBuffer;
            j_db_iterator_get_field(iterator, "memoryStart", &type,
                                    (gpointer *)&tmpMemoryCountBuffer,
                                    &db_length, NULL);
            Dims tmpMemoryCount(tmpMemoryCountBuffer,
                                tmpMemoryCountBuffer + *memoryCountSize);
            info->MemoryCount = tmpMemoryCount;
            g_free(tmpMemoryCountBuffer);
        }

        // std::string variableType = variable.m_Type;
        const char *varType = variable.m_Type.c_str();
        std::string minField;
        std::string maxField;
        std::string valueField;

        // if ((strcmp(varType, "char") == 0) ||
        //     (strcmp(varType, "int8_t") == 0) ||
        //     (strcmp(varType, "uint8_t") == 0) ||
        //     (strcmp(varType, "int16_t") == 0) ||
        //     (strcmp(varType, "uint16_t") == 0) ||
        //     (strcmp(varType, "int32_t") == 0))
        // {
        //     minField = "min_sint32";
        //     maxField = "max_sint32";
        //     valueField = "value_sint32";
        // }
        // else if (strcmp(varType, "uint32_t") == 0)
        // {
        //     minField = "min_uint32";
        //     maxField = "max_uint32";
        //     valueField = "value_uint32";
        // }
        // else if (strcmp(varType, "int64_t") == 0)
        // {
        //     minField = "min_sint64";
        //     maxField = "max_sint64";
        //     valueField = "value_sint64";
        // }
        // else if (strcmp(varType, "uint64_t") == 0)
        // {
        //     minField = "min_uint64";
        //     maxField = "max_uint64";
        //     valueField = "value_uint64";
        // }
        // else if (strcmp(varType, "float") == 0)
        // {
        //     minField = "min_float32";
        //     maxField = "max_float32";
        //     valueField = "value_float32";
        // }
        // else if (strcmp(varType, "double") == 0)
        // {
        //     minField = "min_float64";
        //     maxField = "max_float64";
        //     valueField = "value_float64";
        // }
        // else if (strcmp(varType, "string") == 0)
        // {
        //     valueField = "value_sint32";
        // }

        // else if ((strcmp(varType, "long double") == 0) ||
        //          (strcmp(varType, "float complex") == 0) ||
        //          (strcmp(varType, "double complex") == 0))
        // {
        //     minField = "min_blob";
        //     maxField = "max_blob";
        //     valueField = "value_blob";
        // }
        DB_DO_setMinMaxValueFields(&minField, &maxField, &valueField, varType);
        // std::cout << "minField: " << minField << std::endl;
        // std::cout << "maxField: " << maxField << std::endl;
        // std::cout << "valueField: " << valueField << std::endl;
        // std::cout << "varType: " << varType << std::endl;

        j_db_iterator_get_field(iterator, minField.c_str(), &type,
                                (gpointer *)&min, &db_length, NULL);
        info->Min = *min;
        j_db_iterator_get_field(iterator, maxField.c_str(), &type,
                                (gpointer *)&max, &db_length, NULL);
        info->Max = *max;
        j_db_iterator_get_field(iterator, "isValue", &type,
                                (gpointer *)&isValue, &db_length, NULL);

        info->IsValue = *isValue;
        if (isValue)
        {
            // std::cout << "Get Value from DB" << std::endl;
            j_db_iterator_get_field(iterator, valueField.c_str(), &type,
                                    (gpointer *)&value, &db_length, NULL);
            info->Value = *value;
        }
        j_db_iterator_get_field(iterator, "stepsStart", &type,
                                (gpointer *)&stepsStart, &db_length, NULL);
        info->StepsStart = *stepsStart;
        j_db_iterator_get_field(iterator, "stepsCount", &type,
                                (gpointer *)&stepsCount, &db_length, NULL);
        info->StepsCount = *stepsCount;
        j_db_iterator_get_field(iterator, "blockID", &type,
                                (gpointer *)&blockID, &db_length, NULL);
        info->BlockID = *blockID;

        if (false)
        {
            std::cout << "shapeSize: " << *shapeSize << std::endl;
            std::cout << "startSize: " << *startSize << std::endl;
            std::cout << "countSize: " << *countSize << std::endl;
            std::cout << "memoryStartSize: " << *memoryStartSize << std::endl;
            std::cout << "memoryCountSize: " << *memoryCountSize << std::endl;
            std::cout << "info->Min: " << info->Min << std::endl;
            std::cout << "info->Max: " << info->Max << std::endl;
            std::cout << "info->Value: " << info->Value << std::endl;
            std::cout << "info->StepsStart: " << info->StepsStart << std::endl;
            std::cout << "info->StepsCount: " << info->StepsCount << std::endl;
            std::cout << "info->BlockID: " << info->BlockID << std::endl;
            std::cout << "info->IsValue: " << info->IsValue << std::endl;
        }
        if (isValue)
        {
            g_free(value);
        }
        g_free(isValue);
        g_free(min);
        g_free(max);
        g_free(shapeSize);
        g_free(startSize);
        g_free(countSize);
        g_free(memoryStartSize);
        g_free(memoryCountSize);
        g_free(stepsStart);
        g_free(stepsCount);
        g_free(blockID);
        j_batch_unref(batch);
        j_semantics_unref(semantics);
    }
    return info;
}

template <class T>
void DB_DO_GetVariableDataFromJulea(Variable<T> &variable, T *data,
                                    const std::string nameSpace, size_t offset,
                                    size_t dataSize, uint32_t entryID)
{
    // std::cout << "-- GetVariableDataFromJulea ----- " << std::endl;

    guint64 bytesRead = 0;
    const char *varName = variable.m_Name.c_str();

    std::string objName = "variableblocks";
    auto stringDataObject =
        g_strdup_printf("%s_%s_%s", nameSpace.c_str(), variable.m_Name.c_str(),
                        objName.c_str());
    // std::cout << "stringDataObject: " << stringDataObject << std::endl;
    auto uniqueID = g_strdup_printf("%d", entryID);

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto dataObject = j_object_new(stringDataObject, uniqueID);
    // auto dataObject = j_object_new(stringDataObject, stepBlockID.c_str());

    j_object_read(dataObject, data, dataSize, offset, &bytesRead, batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesRead == dataSize)
    {
        // std::cout << "++ Julea Interaction Reader: Read data for variable "
        // << varName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesRead << " bytes read instead of "
                  << dataSize << " bytes!" << std::endl;
    }

    j_batch_unref(batch);
    j_semantics_unref(semantics);
    g_free(stringDataObject);
    j_object_unref(dataObject);
}

#define variable_template_instantiation(T)                                     \
    template void DB_DO_GetCountFromBlockMetadata(                             \
        const std::string nameSpace, const std::string varName, size_t step,   \
        size_t block, Dims *count, size_t entryID, bool isLocalValue,          \
        T *value);                                                             \
    template void DB_DO_GetBlockMetadataNEW(                                   \
        Variable<T> &variable, typename core::Variable<T>::Info &blockInfo,    \
        size_t entryID);                                                       \
    template std::unique_ptr<typename core::Variable<T>::Info>                 \
    DB_DO_GetBlockMetadata(const core::Variable<T> &variable, size_t entryID); \
                                                                               \
    template void DB_DO_GetVariableDataFromJulea(                              \
        Variable<T> &variable, T *data, const std::string nameSpace,           \
        size_t offset, long unsigned int dataSize, uint32_t entryID);
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

// const std::string nameSpace, size_t step, size_t block, \


/** ------------------------- Attributes -----------------------------------**/

/** Retrieves all variable names from key-value store. They are all stored in
 * one bson. */
void DB_DO_GetNamesFromJulea(const std::string nameSpace, bson_t **bsonNames,
                             unsigned int *varCount, bool isVariable)
{
    // std::cout << "-- GetNamesFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    int err = 0;
    void *namesBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    std::string kvName;

    if (isVariable)
    {
        kvName = "variable_names";
    }
    else
    {
        kvName = "attribute_names";
    }

    auto kvObject = j_kv_new(kvName.c_str(), nameSpace.c_str());
    std::cout << "kvName :" << kvName << std::endl;

    j_kv_get(kvObject, &namesBuf, &valueLen, batch);
    err = j_batch_execute(batch);

    if (err != 0)
    {
        std::cout << "j_batch_execute failed in GetNamesFromJulea. "
                  << std::endl;
    }

    if (valueLen == 0)
    {
        std::cout << "WARNING: The kv store: " << kvName << " is empty!"
                  << std::endl;

        *varCount = 0;
        free(namesBuf);
        j_semantics_unref(semantics);
        j_kv_unref(kvObject);
        j_batch_unref(batch);
        return;
    }
    else
    {
        *bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }

    *varCount = bson_count_keys(*bsonNames);

    j_semantics_unref(semantics);
    j_kv_unref(kvObject);
    j_batch_unref(batch);
    free(namesBuf);
}

void DB_DO_GetAttributeBSONFromJulea(const std::string nameSpace,
                                     const std::string attrName,
                                     bson_t **bsonMetadata, guint32 *valueLen)
{
    // guint32 valueLen = 0;
    void *metaDataBuf = NULL;
    int err = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    std::cout << "-- GetAttributeBSONFromJulea ---- " << std::endl;

    auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace.c_str());
    auto kvObject = j_kv_new(stringMetadataKV, attrName.c_str());

    j_kv_get(kvObject, &metaDataBuf, valueLen, batch);
    err = j_batch_execute(batch);
    if (err != 0)
    {
        std::cout << "j_batch_execute failed in GetAttributeBSONFromJulea"
                  << std::endl;
    }

    if (valueLen == 0)
    {
        printf("WARNING: The attribute key-value store is empty! \n");
    }
    else
    {
        *bsonMetadata =
            bson_new_from_data((const uint8_t *)metaDataBuf, *valueLen);
    }
    free(metaDataBuf);
    j_kv_unref(kvObject);
    j_semantics_unref(semantics);
    j_batch_unref(batch);
    g_free(stringMetadataKV);
}

void DB_DO_GetAttributeMetadataFromJulea(const std::string attrName,
                                         const std::string nameSpace,
                                         long unsigned int *dataSize,
                                         size_t *numberElements,
                                         bool *IsSingleValue, int *type)
{
    bson_t *bsonMetadata;
    std::cout << "-- GetAttributeMetadataFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    DB_DO_GetAttributeBSONFromJulea(nameSpace, attrName, &bsonMetadata,
                                    &valueLen);

    if (valueLen > 0)
    {
        // ParseAttributeFromBSON(nameSpace, attrName, bsonMetadata, dataSize,
        // numberElements, IsSingleValue, type);
    }
    bson_destroy(bsonMetadata);
}

void DB_DO_GetAttributeMetadataFromJulea(const std::string attrName,
                                         const std::string nameSpace,
                                         long unsigned int *completeSize,
                                         size_t *numberElements,
                                         bool *IsSingleValue, int *type,
                                         unsigned long **dataSizes)
{
    bson_t *bsonMetadata;
    std::cout << "-- GetAttributeMetadataFromJulea ------" << std::endl;
    guint32 valueLen = 0;
    DB_DO_GetAttributeBSONFromJulea(nameSpace, attrName, &bsonMetadata,
                                    &valueLen);

    if (valueLen > 0)
    {
        // ParseAttributeFromBSON(nameSpace, attrName, bsonMetadata,
        // completeSize, numberElements, IsSingleValue, type, dataSizes);
    }
    bson_destroy(bsonMetadata);
}

template <class T>
void DB_DO_GetAttributeDataFromJulea(const std::string attrName, T *data,
                                     const std::string nameSpace,
                                     long unsigned int dataSize)
{
    std::cout << "-- GetAttributeDataFromJulea -----" << std::endl;

    guint64 bytesRead = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    // auto batch = j_batch_new(m_JuleaSemantics);

    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // "%s_attributes_%s", nameSpace.c_str(), attrName.c_str());
    auto dataObject = j_object_new(stringDataObject, attrName.c_str());

    // std::cout << "-- stringDataObject: " << stringDataObject << std::endl;
    // std::cout << "-- Datasize = " << dataSize << std::endl;

    j_object_read(dataObject, data, dataSize, 0, &bytesRead, batch);
    // j_batch_execute(batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesRead == dataSize)
    {
        std::cout << "++ Julea Interaction Reader: Read data for attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesRead << " bytes read instead of "
                  << dataSize << " bytes!" << std::endl;
    }

    j_batch_unref(batch);
    j_semantics_unref(semantics);
    g_free(stringDataObject);
    j_object_unref(dataObject);
}

void DB_DO_GetAttributeStringDataFromJulea(
    const std::string attrName, char *data, const std::string nameSpace,
    long unsigned int completeSize, bool IsSingleValue, size_t numberElements)
{
    std::cout << "-- GetAttributeDataFromJulea -- String version -----"
              << std::endl;

    guint64 bytesRead = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    // void *dataBuf;

    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // "%s_attributes_%s", nameSpace.c_str(), attrName.c_str());
    auto dataObject = j_object_new(stringDataObject, attrName.c_str());

    // std::cout << "-- stringDataObject: " << stringDataObject << std::endl;
    // std::cout << "-- Datasize = " << completeSize << std::endl;

    if (IsSingleValue)
    {
        char *charArray = data;

        j_object_read(dataObject, charArray, completeSize, 0, &bytesRead,
                      batch);
        // j_batch_execute(batch);
        g_assert_true(j_batch_execute(batch) == true);
        // std::cout << "-- charArray = " << charArray << std::endl;
    }
    else
    {
        char *stringArray = data;

        j_object_read(dataObject, stringArray, completeSize, 0, &bytesRead,
                      batch);
        // j_batch_execute(batch);
        g_assert_true(j_batch_execute(batch) == true);
        // std::cout << "string: " << stringArray << std::endl;
    }
    if (bytesRead == completeSize)
    {
        std::cout << "++ Julea Interaction Reader: Read data for attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesRead << " bytes read instead of "
                  << completeSize << " bytes!" << std::endl;
    }

    j_batch_unref(batch);
    j_semantics_unref(semantics);
    g_free(stringDataObject);
    j_object_unref(dataObject);
}

#define attribute_template_instantiation(T)                                    \
    template void DB_DO_GetAttributeDataFromJulea(                             \
        const std::string attrName, T *data, const std::string nameSpace,      \
        long unsigned int dataSize);
ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
