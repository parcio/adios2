/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaKVInteractionReader.h"
#include "JuleaKVInteractionReader.tcc"
// #include "JuleaKVDAIInteractionReader.h"
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

JuleaKVInteractionReader::JuleaKVInteractionReader(helper::Comm const &comm)
: JuleaInteraction(std::move(comm))
{
    // std::cout << "This is the constructor of the reader" << std::endl;
}

void JuleaKVInteractionReader::InitVariable(core::IO *io, core::Engine &engine,
                  const std::string projectNamespace,
                  const std::string fileName, std::string varName,
                  size_t *blocks, size_t numberSteps, ShapeID shapeID,
                  bool isReadAsJoined, bool isReadAsLocalValue,
                  bool isRandomAccess, bool isSingleValue)
{
    // std::cout << "----- InitVariable --- " << varName << std::endl;
    const adios2::DataType type(io->InquireVariableType(varName));

    // std::cout << "type(io->InquireVariableType(varName): " << type <<
    // std::endl;

    int err = 0;
    uint32_t entryID = 0;
    uint32_t *tmpID;
    size_t step;
    size_t block;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto blockNamespace =
        g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
    // auto blockSchema = j_db_schema_new(blockNamespace, "block-metadata",
    // NULL); j_db_schema_get(blockSchema, batch, NULL);
    err = j_batch_execute(batch);

    auto varNamespace =
        g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
    // auto varSchema = j_db_schema_new(varNamespace, "variable-metadata",
    // NULL); j_db_schema_get(varSchema, batch, NULL);
    err = j_batch_execute(batch);

    std::string minField;
    std::string maxField;
    std::string valueField;
    std::string meanField;
    std::string sumField;

    /** AvailableStepBlockIndexOffsets stores the entries (= blocks) _id (= line
     * in the sql table) */
    if (type == DataType::Compound)
    {
    }
#define declare_type(T)                                                        \
    else if (type == helper::GetDataType<T>())                                 \
    {                                                                          \
        auto var = io->InquireVariable<T>(varName);                            \
        if (var)                                                               \
        {                                                                      \
            var->m_ShapeID = shapeID;                                          \
            for (size_t i = 0; i < numberSteps; i++)                           \
            {                                                                  \
                for (size_t j = 0; j < blocks[i]; j++)                         \
                {                                                              \
                    step = i;                                                  \
                    block = j;                                                 \
                }                                                              \
                var->m_AvailableStepsCount++;                                  \
            }                                                                  \
                                                                               \
            JuleaInteraction::SetMinMaxValueFields(&minField, &maxField,       \
                                                   &valueField, &meanField,    \
                                                   &sumField, type);           \
            var->m_AvailableStepsStart = 0;                                    \
            var->m_StepsStart = 0;                                             \
            var->m_Engine = &engine;                                           \
            var->m_FirstStreamingStep = true;                                  \
            var->m_ReadAsJoined = isReadAsJoined;                              \
            var->m_ReadAsLocalValue = isReadAsLocalValue;                      \
            var->m_RandomAccess = isRandomAccess;                              \
            var->m_SingleValue = isSingleValue;                                \
                                                                               \
            if (var->m_ShapeID == ShapeID::LocalValue)                         \
            {                                                                  \
                var->m_ShapeID = ShapeID::GlobalArray;                         \
                var->m_SingleValue = true;                                     \
            }                                                                  \
        }                                                                      \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

void JuleaKVInteractionReader::DefineVariableInEngineIO(core::IO *io, const std::string varName,
                              adios2::DataType type, ShapeID shapeID,
                              Dims shape, Dims start, Dims count,
                              bool constantDims, bool isLocalValue)
{
    if (type == DataType::Compound)
    {
    }
#define declare_type(T)                                                        \
    else if (type == helper::GetDataType<T>())                                 \
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

void JuleaKVInteractionReader::DefineVariableInInit(
    core::IO *io, const std::string varName, std::string stringType, Dims shape,
    Dims start, Dims count, bool constantDims, bool isLocalValue)
{
    const char *type = stringType.c_str();

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

void JuleaKVInteractionReader::InitVariablesFromKV(
    const std::string projectNamespace, const std::string fileName,
    core::IO *io, core::Engine &engine)
{
    // std::cout << "--- InitVariablesFromDB ---" << std::endl;
    // int rank = engine.m_Comm.Rank();
    // int MPISize = engine.m_Comm.Size();

    int err = 0;
    JDBType type;
    char *varName;
    void *mdBuf = NULL;
    guint32 mdLen = 0;
    guint32 nameLen = 0;
    // char *varTypePtr;
    // std::string varType;

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
    adios2::DataType typeInt;
    int *varTypeAsInt;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    // auto completeNamespace =
        // g_strdup_printf("%s_%s", "adios2", projectNamespace.c_str());
    //    auto schema = j_db_schema_new(completeNamespace, "variable-metadata", NULL);
    // j_db_schema_get(schema, batch, NULL);

    auto completeNamespace = g_strdup_printf(
        "%s_%s_%s", "adios2", projectNamespace.c_str(), "variable-metadata");

    // while (j_db_iterator_next(iterator, NULL))
    // {
    Dims shape;
    Dims start;
    Dims count;
   

    //TODO: get names from kv
    // auto kvIterator = j_kv_iterator_new(gchar const * namespace, gchar const *prefix);
    //get variable Name using kv iterator over namespace + prefix
    auto kvIterator = j_kv_iterator_new(completeNamespace, fileName.c_str());


	while (j_kv_iterator_next(kv_iterator))
	{
		gchar const* key;
		gconstpointer value;
		guint32 len;
		gconstpointer adata = NULL;
		gsize alen = 0;

        // get variable name
		key = j_kv_iterator_get(kv_iterator, &value, &nameLen);
        varName = value;
  
        auto fileVar = g_strdup_printf(
        "%s_%s", fileName.c_str(), varName);
        auto varMetadata = j_kv_new(completeNamespace, fileVar);

        j_kv_get(varMetadata, &mdBuf, &mdLen, batch);
        err = j_batch_execute(batch);


     if (mdLen == 0)
    {
        // bson_names = bson_new();
        printf("WARNING: The variable metadata is empty! \n");
    }
    else
    {
        bson_init_static(&bson_metadata, (uint8_t *)meta_data_buf, mdLen);
    }

     if (bson_iter_init(&b_iter, &bson_metadata))
    {
        std::cout << "++ Julea Client Logic: Bson iterator is valid"
                  << std::endl;
    }
    else
    {
        std::cout << "ERROR: Bson iterator is not valid!" << std::endl;
    }
    
    //TODO: get all fields from BSON
    
    // j_db_iterator_get_field(iterator, "isConstantDims", &type,
    //                         (gpointer *)&isConstantDims, &db_length, NULL);
    // j_db_iterator_get_field(iterator, "isReadAsJoined", &type,
    //                         (gpointer *)&isReadAsJoined, &db_length, NULL);
    // j_db_iterator_get_field(iterator, "isReadAsLocalValue", &type,
    //                         (gpointer *)&isReadAsLocalValue, &db_length,
    //                         NULL);
    // j_db_iterator_get_field(iterator, "isRandomAccess", &type,
    //                         (gpointer *)&isRandomAccess, &db_length, NULL);
    // j_db_iterator_get_field(iterator, "isSingleValue", &type,
    //                         (gpointer *)&isSingleValue, &db_length, NULL);

    // j_db_iterator_get_field(iterator, "shapeID", &type,
    //                         (gpointer *)&shapeID, &db_length, NULL);
    // j_db_iterator_get_field(iterator, "typeString", &type,
    // (gpointer *)&varTypePtr, &db_length, NULL);
    // std::string varType(varTypePtr);

    // j_db_iterator_get_field(iterator, "typeInt", &type,
    // (gpointer *)&typeInt, &db_length, NULL);
    // j_db_iterator_get_field(iterator, "typeInt", &type,
    //                         (gpointer *)&varTypeAsInt, &db_length, NULL);
    // j_db_iterator_get_field(iterator, "shapeSize", &type,
    //                         (gpointer *)&shapeSize, &db_length, NULL);

    if (*shapeSize > 0)
    {
        size_t *tmpShapeBuffer;
        // j_db_iterator_get_field(iterator, "shape", &type,
        // (gpointer *)&tmpShapeBuffer, &db_length,
        // NULL);
        Dims tmpShape(tmpShapeBuffer, tmpShapeBuffer + *shapeSize);
        shape = tmpShape;
        g_free(tmpShapeBuffer);
    }

    // j_db_iterator_get_field(iterator, "startSize", &type,
    // (gpointer *)&startSize, &db_length, NULL);
    if (*startSize > 0)
    {
        size_t *tmpStartBuffer;
        // j_db_iterator_get_field(iterator, "start", &type,
        // (gpointer *)&tmpStartBuffer, &db_length,
        // NULL);
        Dims tmpStart(tmpStartBuffer, tmpStartBuffer + *startSize);
        start = tmpStart;
        g_free(tmpStartBuffer);
    }

    // j_db_iterator_get_field(iterator, "countSize", &type,
    // (gpointer *)&countSize, &db_length, NULL);
    if (*countSize > 0)
    {
        size_t *tmpCountBuffer;
        // j_db_iterator_get_field(iterator, "count", &type,
        // (gpointer *)&tmpCountBuffer, &db_length,
        // NULL);

        Dims tmpCount(tmpCountBuffer, tmpCountBuffer + *countSize);
        count = tmpCount;
        g_free(tmpCountBuffer);
    }

    // j_db_iterator_get_field(iterator, "numberSteps", &type,
    // (gpointer *)&numberSteps, &db_length, NULL);
    size_t *tmpblocks[*numberSteps];
    if (*numberSteps > 0)
    {
        // j_db_iterator_get_field(iterator, "blockArray", &type,
        // (gpointer *)tmpblocks, &db_length, NULL);
        blocks = *tmpblocks;
        // memcpy(blocks, *tmpblocks, sizeof(*tmpblocks));
    }

    if (false)
    {
        // std::cout << "numberSteps: " << blocks[0] << std::endl;
        // std::cout << "numberSteps: " << blocks[1] << std::endl;
        std::cout << "\nvarName = " << varName << std::endl;
        // std::cout << "length: " << db_length << std::endl;
        std::cout << "constantDims: " << *isConstantDims << std::endl;
        std::cout << "isReadAsJoined: " << *isReadAsJoined << std::endl;
        std::cout << "isReadAsLocalValue: " << *isReadAsLocalValue << std::endl;
        std::cout << "isRandomAccess: " << *isRandomAccess << std::endl;
        std::cout << "isSingleValue: " << *isSingleValue << std::endl;
        std::cout << "shapeID: " << *shapeID << std::endl;
        std::cout << "varType: " << *varTypeAsInt << std::endl;
        // std::cout << "varType2: " << varTypePtr << std::endl;
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

    adios2::DataType adiosType{static_cast<adios2::DataType>(*varTypeAsInt)};

    // std::cout << "adiosType: " << adiosType << std::endl;

    // DBDefineVariableInEngineIO(io, varName, typeInt, *shapeID, shape,
    // start, DBDefineVariableInEngineIO(io, varName, varTypeAsInt,
    // *shapeID, shape, start,
    DefineVariableInEngineIO(io, varName, adiosType, *shapeID, shape, start,
                             count, *isConstantDims, *isSingleValue);
    // DBDefineVariableInInit(io, varName, varType, shape, start, count,
    //                        *isConstantDims, *isSingleValue);
    InitVariable(io, engine, projectNamespace, fileName, varName, blocks,
                 *numberSteps, *shapeID, *isReadAsJoined, *isReadAsLocalValue,
                 *isRandomAccess, *isSingleValue);
    if (*numberSteps > 0)
    {
        g_free(*tmpblocks);
    }
    g_free(isConstantDims);
    g_free(isReadAsJoined);
    g_free(isReadAsLocalValue);
    g_free(isRandomAccess);
    g_free(varName);
    g_free(shapeID);
    g_free(isSingleValue);
    // g_free(varTypePtr);
    g_free(shapeSize);
    g_free(startSize);
    g_free(countSize);
    g_free(numberSteps);
    }
    // i++;
    // }
    // j_db_iterator_unref(iterator);
    // j_db_entry_unref(entry);
    // j_db_schema_unref(schema);
    // j_db_selector_unref(selector);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

// TODO: projectnamespace
void JuleaKVInteractionReader::GetNamesFromJulea(
    const std::string projectNamespace, const std::string fileName,
    bson_t **bsonNames, unsigned int *varCount, bool isVariable)
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

    auto kvObject = j_kv_new(kvName.c_str(), fileName.c_str());

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

#define variable_template_instantiation(T)                                     \
    template void JuleaKVInteractionReader::GetCountFromBlockMetadata(         \
        const std::string projectNamespace, const std::string fileName,        \
        const std::string varName, size_t step, size_t block, Dims *count,     \
        size_t entryID, bool isLocalValue, T *value);                          \
    template std::unique_ptr<typename core::Variable<T>::Info>                 \
    JuleaKVInteractionReader::GetBlockMetadata(                                \
        const core::Variable<T> &variable, std::string projectNamespace,       \
        size_t entryID) const;
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

} // end namespace interop
} // end namespace adios2
