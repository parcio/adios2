/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaInteractionWriter.h"
#include "JuleaMetadata.h"

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
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

/**
 *  Writes name of variable/attribute to Julea KV. Also checks if name is
 * already in kv. paramName = name of variable/attribute nameSpace = file kvName
 * = variable_names/ attribute_names
 */
void WriteNameToJuleaKV(std::string paramName, std::string nameSpace,
                        std::string kvName)
{
    guint32 valueLen = 0;
    bson_t *bsonNames;
    bson_iter_t bIter;

    void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);
    auto name = strdup(paramName.c_str());

    std::cout << "kvName: " << kvName << std::endl;
    std::cout << "nameSpace: " << nameSpace << std::endl;
    std::cout << "paramName: " << paramName << std::endl;

    /** store all variable names for a file = namespace */
    auto kvObjectNames = j_kv_new(kvName.c_str(), nameSpace.c_str());

    j_kv_get(kvObjectNames, &namesBuf, &valueLen, batch);
    j_batch_execute(batch);

    std::cout << "valueLen: " << valueLen << std::endl;
    if (valueLen == 0)
    {
        bsonNames = bson_new();
    }
    else
    {
        bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }
    free(namesBuf);

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&bIter, bsonNames, name))
    {
        bson_append_int32(bsonNames, name, -1, 42);
    }
    else
    {
        std::cout << "++ Julea Interaction Writer:  " << name
                  << " already in kv store. " << std::endl;
    }

    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch2);
    j_batch_execute(batch2);

    // free(namesBuf); //TODO: why does this lead to segfaults?
    free(name);
    bson_destroy(bsonNames);
    j_kv_unref(kvObjectNames);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    j_semantics_unref(semantics);
}

// /**
//  * Writes either variable or attribute metadata to the julea kv. Examples:
//    kvName = variables/attributes
//    paramName = v0/
//    nameSpace = filename (Julea-SimpleSteps.bp)

//  * @param kvName       [description]
//  * @param paramName    [description]
//  * @param nameSpace    [description]
//  * @param bsonMetaData [description]
//  */
// void WriteMetadataToJuleaKV(std::string kvName, std::string paramName,
//                             std::string nameSpace, bson_t *bsonMetaData,
//                             size_t currStep)
// {
//     // void *namesBuf = NULL;
//     void *metaDataBuf = NULL;

//     auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
//     auto batch = j_batch_new(semantics);

//     std::cout << "kvName " << kvName << std::endl;
//     std::cout << "paramName " << paramName << std::endl;
//     std::cout << "nameSpace " << nameSpace << std::endl;

//     auto stringMetadataKV =
//         g_strdup_printf("%s_%s", nameSpace.c_str(), kvName.c_str());
//     // g_strdup_printf("%s_%s", kvName.c_str(), nameSpace.c_str());
//     std::cout << "stringMetadataKV " << stringMetadataKV << std::endl;
//     auto kvObjectMetadata = j_kv_new(stringMetadataKV, paramName.c_str());

//     metaDataBuf = g_memdup(bson_get_data(bsonMetaData), bsonMetaData->len);
//     j_kv_put(kvObjectMetadata, metaDataBuf, bsonMetaData->len, g_free,
//              batch); // FIXME: reading issue

//     j_batch_execute(batch);

//     // free(metaDataBuf);
//     // free(namesBuf);
//     g_free(stringMetadataKV);
//     j_kv_unref(kvObjectMetadata);
//     j_batch_unref(batch);
//     j_semantics_unref(semantics);
// }

void WriteBlockMetadataToJuleaKV(const std::string nameSpace, gpointer &md,
                                 guint32 valueLen,
                                 const std::string stepBlockID)
{
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV =
        g_strdup_printf("%s_%s", nameSpace.c_str(), "variableblocks");
    std::cout << "stringMetadataKV " << stringMetadataKV << std::endl;

    auto kvObjectMetadata = j_kv_new(stringMetadataKV, stepBlockID.c_str());

    j_kv_put(kvObjectMetadata, md, valueLen, g_free, batch);
    j_batch_execute(batch);

    g_free(stringMetadataKV);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void WriteVarMetadataToJuleaKV(Variable<T> &variable,
                               const std::string nameSpace, size_t currStep,
                               size_t blockID)
{
    std::cout << "--- WriteVarMetadataToJuleaKV ---" << std::endl;

    guint32 valueLen = 0;
    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto stringMetadataKV =
        g_strdup_printf("%s_%s", nameSpace.c_str(), "variables");
    auto kvVarMetadata = j_kv_new(stringMetadataKV, variable.m_Name.c_str());
    // std::cout << "nameSpace " << nameSpace << std::endl;
    std::cout << "stringMetadataKV " << stringMetadataKV << std::endl;
    std::cout << "varName " << variable.m_Name << std::endl;

    // JuleaKVWriter::StepMetadata md =
    // g_slice_new(JuleaKVWriter::StepMetadata);
    // JuleaKVWriter::StepMetadata md;
    // std::cout << "sizeof md" << sizeof(md) << std::endl;

    /* ------------------------------ DEBUG START ---------------------------*/
    // try to write, then read, then write new stuff
    // std::cout << "======== DEBUG =====================" << std::endl;
    // JuleaKVWriter::StepMetadata *md2 =
    // g_slice_new(JuleaKVWriter::StepMetadata);

    // md2->numberSteps = currStep + 1;
    // md2->blocks = (size_t*) g_slice_alloc(md2->numberSteps*sizeof(size_t));

    // for (uint i = 0; i <= currStep; i++)
    // {
    //     // only one element in vector -> number of blocks for this step
    //     md2->blocks[i] = variable.m_AvailableStepBlockIndexOffsets[i].at(0);
    //     std::cout << "md2: i: " << i << "  blocks: " << md2->blocks[i] <<
    //     std::endl;
    // }

    // size_t len = sizeof(JuleaKVWriter::StepMetadata) +
    // (currStep+1)*sizeof(size_t); std::cout << "len " << len << std::endl;

    // metaDataBuf = g_memdup(md2, len);

    // j_kv_put(kvVarMetadata, metaDataBuf, len, g_free, batch);
    // j_batch_execute(batch);

    /* ------------------------------ DEBUG END ---------------------------*/

    // std::cout << "md->numberSteps: " << md.numberSteps << std::endl;
    // j_kv_get(kvVarMetadata, (gpointer *)&md, &valueLen, batch);
    j_kv_get(kvVarMetadata, &metaDataBuf, &valueLen, batch);
    j_batch_execute(batch);

    std::cout << "valueLen: " << valueLen << std::endl;
    // if (valueLen == 0)
    // {
    //     std::cout << "--- DEBUG --- todo something here" << std::endl;

    //     JuleaKVWriter::StepMetadata md2;
    //     //TODO: this is just copied from else... something needs to be
    //     adapted char *buffer = NULL;

    //     bool constantDims = variable.IsConstantDims();
    //     size_t typeLen = sizeof(variable.m_Type.c_str());
    //     const char *type = variable.m_Type.c_str();

    //     size_t shapeSize = variable.m_Shape.size();
    //     size_t shapeLen = shapeSize * sizeof(Dims[0]);

    //     size_t startSize = variable.m_Start.size();
    //     size_t startLen = startSize * sizeof(Dims[0]);

    //     size_t countSize = variable.m_Count.size();
    //     size_t countLen = countSize * sizeof(Dims[0]);

    //     size_t numberSteps = currStep + 1;
    //     size_t blocksLen = numberSteps * sizeof(size_t);
    //     // calculating buffer size
    //     size_t len = sizeof(bool) +
    //                 typeLen  + sizeof(size_t) +
    //                 shapeLen + sizeof(size_t) +   //shapeSize + size for
    //                 counter startLen + sizeof(size_t) + countLen +
    //                 sizeof(size_t) + blocksLen + sizeof(size_t);
    //     std::cout << "len: " << len << std::endl;

    //     buffer = (char*) g_slice_alloc(len);

    //     size_t *blocks = (size_t *)g_slice_alloc(numberSteps *
    //     sizeof(size_t)); for (uint i = 0; i < numberSteps; i++)
    //     {
    //         std::cout << "--- DEBUG ---" << std::endl;
    //         blocks[i] = variable.m_AvailableStepBlockIndexOffsets[i].at(0);
    //         std::cout << "i: " << i << "  blocks: " << blocks[i]
    //                   << std::endl;
    //     }

    //     memcpy(buffer, &constantDims, sizeof(bool));
    //     buffer += sizeof(bool);
    //     memcpy(buffer, &typeLen, sizeof(size_t));
    //     buffer += sizeof(size_t);
    //     memcpy(buffer, type, typeLen);
    //     buffer += typeLen;
    //     memcpy(buffer, &shapeSize, sizeof(size_t));
    //     // memcpy(buffer, (void*) variable.m_Shape.size(), sizeof(size_t));
    //     buffer += sizeof(size_t);
    //     memcpy(buffer, variable.m_Shape.data(), shapeLen);
    //     buffer += shapeLen;
    //     memcpy(buffer, &startSize, sizeof(size_t));
    //     buffer += sizeof(size_t);
    //     memcpy(buffer, variable.m_Start.data(), startLen);
    //     buffer += startLen;
    //     memcpy(buffer, &countSize, sizeof(size_t));
    //     buffer += sizeof(size_t);
    //     memcpy(buffer, variable.m_Count.data(), countLen);
    //     buffer += countLen;
    //     memcpy(buffer, &numberSteps, sizeof(size_t));
    //     buffer += sizeof(size_t);
    //     memcpy(buffer, blocks, blocksLen);
    //     buffer += blocksLen;

    // }
    // else
    // {
    //     // JuleaKVWriter::StepMetadata md;
    //     // // md = metaDataBuf;
    //     // md.shape = variable.m_Shape;
    //     // md.start = variable.m_Start;
    //     // md.count = variable.m_Count;
    //     // md.type = variable.m_Type;
    //     // md.isConstantDims = variable.IsConstantDims();
    //     // md.numberSteps = currStep + 1;
    //     // md.blocks = (size_t *)g_slice_alloc(md.numberSteps *
    //     sizeof(size_t));

    //     // // TODO: check whether this is called too often.
    //     // // it is called in KVWriter.tcc for every block but every block
    //     change
    //     // // needs to update variable. so it must be ok
    //     // std::cout << "md->numberSteps: " << md.numberSteps << std::endl;
    //     // std::cout << "type " << md.type.c_str() << std::endl;
    //     // std::cout << "isConstantDims" << md.isConstantDims << std::endl;
    //     // for (uint i = 0; i <= currStep; i++)
    //     // {
    //     //     std::cout << "--- DEBUG ---" << std::endl;
    //     //     // only one element in vector -> number of blocks for this
    //     step
    //     //     // blocks[i] =
    //     variable.m_AvailableStepBlockIndexOffsets[i].at(0);
    //     //     md.blocks[i] =
    //     variable.m_AvailableStepBlockIndexOffsets[i].at(0);
    //     //     // md->blocks = blocks;
    //     //     std::cout << "i: " << i << "  blocks: " << md.blocks[i]
    //     //               << std::endl;
    //     // }
    //     //     std::cout << "--- DEBUG 2 ---" << std::endl;
    //     // size_t len = sizeof(JuleaKVWriter::StepMetadata);
    //     //     std::cout << "--- DEBUG 3 ---" << std::endl;
    //     // metaDataBuf = g_memdup(&md, len);
    //     //     std::cout << "--- DEBUG 4 ---" << std::endl;

    //     // j_kv_put(kvVarMetadata, metaDataBuf, len, g_free, batch);
    //     //     std::cout << "--- DEBUG 5 ---" << std::endl;
    //     // j_batch_execute(batch);
    // }

    char *buffer = NULL;

    bool constantDims = variable.IsConstantDims();
    size_t typeLen = sizeof(variable.m_Type.c_str());
    const char *type = variable.m_Type.c_str();

    size_t shapeSize = variable.m_Shape.size();
    size_t shapeLen = shapeSize * sizeof(Dims[0]);

    size_t startSize = variable.m_Start.size();
    size_t startLen = startSize * sizeof(Dims[0]);

    size_t countSize = variable.m_Count.size();
    size_t countLen = countSize * sizeof(Dims[0]);

    size_t numberSteps = currStep + 1;
    size_t blocksLen = numberSteps * sizeof(size_t);
    // calculating buffer size
    size_t len = sizeof(bool) + typeLen + sizeof(size_t) + shapeLen +
                 sizeof(size_t) + // shapeSize + size for counter
                 startLen + sizeof(size_t) + countLen + sizeof(size_t) +
                 blocksLen + sizeof(size_t);
    std::cout << "len: " << len << std::endl;

    buffer = (char *)g_slice_alloc(len);

    // size_t *blocks = (size_t *)g_slice_alloc(numberSteps * sizeof(size_t));
    size_t blocks[numberSteps];
    for (uint i = 0; i < numberSteps; i++)
    {
        std::cout << "--- DEBUG ---" << std::endl;
        // blocks[i] = variable.m_AvailableStepBlockIndexOffsets[i].at(0);
        blocks[i] = variable.m_AvailableStepBlockIndexOffsets[i].size();
        // blocks[i] = blockID + 1;
        std::cout << "i: " << i << "  blocks: " << blocks[i] << std::endl;
    }

    memcpy(buffer, &constantDims, sizeof(bool));
    buffer += sizeof(bool);
    memcpy(buffer, &typeLen, sizeof(size_t));
    buffer += sizeof(size_t);
    memcpy(buffer, type, typeLen);
    buffer += typeLen;
    memcpy(buffer, &shapeSize, sizeof(size_t));
    // memcpy(buffer, (void*) variable.m_Shape.size(), sizeof(size_t));
    buffer += sizeof(size_t);
    memcpy(buffer, variable.m_Shape.data(), shapeLen);
    buffer += shapeLen;
    memcpy(buffer, &startSize, sizeof(size_t));
    buffer += sizeof(size_t);
    memcpy(buffer, variable.m_Start.data(), startLen);
    buffer += startLen;
    memcpy(buffer, &countSize, sizeof(size_t));
    buffer += sizeof(size_t);
    memcpy(buffer, variable.m_Count.data(), countLen);
    buffer += countLen;
    memcpy(buffer, &numberSteps, sizeof(size_t));
    buffer += sizeof(size_t);
    memcpy(buffer, blocks, blocksLen);
    buffer += blocksLen;

    std::cout << "--- DEBUG 2 ---" << std::endl;
    j_kv_put(kvVarMetadata, buffer, len, g_free, batch);
    std::cout << "--- DEBUG 3 ---" << std::endl;
    j_batch_execute(batch);
    std::cout << "--- DEBUG 4 ---" << std::endl;

    g_free(stringMetadataKV);
    j_kv_unref(kvVarMetadata);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void PutAttributeMetadataToJuleaSmall(Attribute<T> &attribute,
                                      bson_t *bsonMetaData,
                                      const std::string nameSpace)
{

    const char *kvNames = "attribute_names";
    const char *kvMD = "attributes";

    // TODO: more leaks than old version below ?!
    WriteNameToJuleaKV(attribute.m_Name, nameSpace.c_str(), kvNames);
    // WriteMetadataToJuleaKV(kvMD, attribute.m_Name, nameSpace.c_str(),
    // bsonMetaData);

    //  // TODO: check if update version is necessary!
    // bool IsAlreadyInKV = false;
    //  bson_t *bsonNames;

    // //  /* names_kv = kv holding all variable names */
    //  auto kvObjectNames = j_kv_new(kvNames, nameSpace.c_str());

    //  CheckIfAlreadyInKV(kvMD, attribute.m_Name, nameSpace.c_str(),
    //  &bsonNames,
    //                     kvObjectNames, &IsAlreadyInKV);

    //  if (!IsAlreadyInKV)
    //  {
    //      WriteNameToJuleaKVOld(kvMD, attribute.m_Name, nameSpace.c_str(),
    //      bsonNames,
    //                         kvObjectNames);
    //      std::cout << "Test IsAlreadyInKV " << IsAlreadyInKV << std::endl;
    //  }
    //  else
    //  {
    //      // UpdateMetadataInKV(kvMD, variable.m_Name, nameSpace.c_str(),
    //      //                    bsonNames, bsonMetaData, kvObjectNames);
    //      std::cout << "___ NEEDS UPDATE ___ " << std::endl;
    //  }
    // WriteMetadataToJuleaKV(kvMD, attribute.m_Name, nameSpace.c_str(),
    // bsonMetaData);

    // j_kv_unref(kvObjectNames);
    // bson_destroy(bsonNames);
}

// FIXME: params for writeblockmetadata!
// template <class T>
// void PutVariableMetadataToJulea(Variable<T> &variable,
//                                 JuleaKVWriter::Metadata<T> &md,
//                                 const std::string nameSpace, size_t currStep,
//                                 size_t blockID, bool isNameWritten)
template <class T>
void PutVariableMetadataToJulea(Variable<T> &variable, gpointer &md,
                                guint32 valueLen, const std::string nameSpace,
                                size_t currStep, size_t blockID,
                                bool isNameWritten)
{
    const char *kvNames = "variable_names";
    const char *kvMD = "variables";
    auto stepBlockID = g_strdup_printf("%d_%d", currStep, blockID);
    std::cout << "stepBlockID: " << stepBlockID << std::endl;

    if (!isNameWritten)
    {
        WriteNameToJuleaKV(variable.m_Name, nameSpace.c_str(), kvNames);
    }

    WriteVarMetadataToJuleaKV(variable, nameSpace.c_str(), currStep, blockID);
    // WriteBlockMetadataToJuleaKV(variable, nameSpace.c_str(), md, currStep,
    // blockID);

    WriteBlockMetadataToJuleaKV(nameSpace.c_str(), md, valueLen, stepBlockID);
}

/** ------------------------- DATA ------------------------------------------**/
void WriteDataToJuleaObjectStore(std::string objName, std::string paramName,
                                 std::string nameSpace, unsigned int dataSize,
                                 const void *data, size_t currStep)
{
    guint64 bytesWritten = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto name = strdup(paramName.c_str());

    /* Write data pointer to object store:
     * printf("%s_variables_%s", nameSpace, name)*/
    std::cout << "objName: " << objName << std::endl;
    auto stringDataObject =
        // g_strdup_printf("%s_%s_%s_%d", nameSpace.c_str(), objName.c_str(),
        // name, currStep); g_strdup_printf("%s_%s_%s", nameSpace.c_str(),
        // objName.c_str(), name);
        g_strdup_printf("%s_%s", nameSpace.c_str(), objName.c_str());
    std::cout << "stringDataObject " << stringDataObject << std::endl;
    auto dataObject = j_object_new(stringDataObject, name);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for:  " << name
                  << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << dataSize << " bytes! "
                  << std::endl;
    }
    free(name);
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

template <class T>
void PutVariableDataToJulea(Variable<T> &variable, const T *data,
                            const std::string nameSpace, size_t currStep,
                            size_t block)
{
    guint64 bytesWritten = 0;
    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    auto dataSize = variable.m_ElementSize * numberElements;

    std::string objName = "variableblocks";
    auto stepBlockID = g_strdup_printf("%d_%d", currStep, block);
    std::cout << "stepBlockID: " << stepBlockID << std::endl;
    auto stringDataObject =
        g_strdup_printf("%s_%s", nameSpace.c_str(), objName.c_str());
    std::cout << "stringDataObject " << stringDataObject << std::endl;

    std::cout << "data [0] = " << data[0] << std::endl;
    std::cout << "data [1] = " << data[1] << std::endl;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto dataObject = j_object_new(stringDataObject, stepBlockID);

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for:  "
                  << stepBlockID << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << dataSize << " bytes! "
                  << std::endl;
    }
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction: PutVariableDataToJulea" << std::endl;
}

// FIXME: not yet implemented correctly! need to differentiate between strings
// and other types
template <class T>
void PutAttributeDataToJuleaSmall(Attribute<T> &attribute, const T *data,
                                  const std::string nameSpace)
{
    // std::string objName = "attributes";
    // unsigned int dataSize = 0;

    // if (attribute.m_IsSingleValue)
    // {
    //     // TODO: check if this is correct
    //     dataSize = sizeof(attribute.m_DataSingleValue);
    // }
    // else
    // {
    //     dataSize = attribute.m_DataArray.size();
    // }

    // WriteDataToJuleaObjectStore(objName, attribute.m_Name, nameSpace.c_str(),
    //                             dataSize, data);
    std::cout << "++ Julea Interaction: PutAttributeDataToJuleaSmall"
              << std::endl;
}

/** -------------------------------------------------------------------------**/
/** -------------- TESTING GENERIC FUNCTIONS END ----------------------------**/
/** -------------------------------------------------------------------------**/

/** ------------ ATTRIBUTES -------------------------------------------------**/

template <class T>
void PutAttributeDataToJulea(Attribute<T> &attribute,
                             const std::string nameSpace)
{
    std::cout << "-- PutAttributeDataToJulea -------" << std::endl;
    void *dataBuf = NULL;
    guint64 bytesWritten = 0;
    unsigned int dataSize = 0;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    char *cString;

    auto attrName = strdup(attribute.m_Name.c_str());
    auto numberElements = attribute.m_Elements;

    /* Write data pointer to object store*/
    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // g_strdup_printf("%s_attributes_%s", nameSpace.c_str(),
    // attrName);
    auto dataObject = j_object_new(stringDataObject, attrName);

    j_object_create(dataObject, batch);

    if (attribute.m_IsSingleValue)
    {
        dataSize = sizeof(attribute.m_DataSingleValue);
        j_object_write(dataObject, &attribute.m_DataSingleValue, dataSize, 0,
                       &bytesWritten, batch);
    }
    else
    {
        dataSize = attribute.m_DataArray.size() * sizeof(T);
        j_object_write(dataObject, attribute.m_DataArray.data(), dataSize, 0,
                       &bytesWritten, batch);
    }

    j_batch_execute(batch);
    if (bytesWritten == dataSize)
    {
        std::cout << "++ Julea Interaction Writer: Data written for \
            attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << dataSize << " bytes!"
                  << std::endl;
    }

    free(attrName);
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

template <>
void PutAttributeDataToJulea<std::string>(Attribute<std::string> &attribute,
                                          const std::string nameSpace)
{
    std::cout << "-- PutAttributeDataToJulea -------" << std::endl;

    unsigned int dataSize = 0;
    guint64 bytesWritten = 0;
    void *dataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    auto attrName = strdup(attribute.m_Name.c_str());
    auto numberElements = attribute.m_Elements;

    auto stringDataObject = g_strdup_printf("%s_attributes", nameSpace.c_str());
    // g_strdup_printf("%s_attributes_%s", nameSpace.c_str(),
    // attrName);
    auto dataObject = j_object_new(stringDataObject, attrName);
    unsigned int offset = 0;

    j_object_create(dataObject, batch);

    if (attribute.m_IsSingleValue)
    {
        dataSize = attribute.m_DataSingleValue.length() + 1;
        char *dataElement = new char[dataSize];
        strcpy(dataElement, attribute.m_DataSingleValue.c_str());

        j_object_write(dataObject, dataElement, dataSize, 0, &bytesWritten,
                       batch);
        j_batch_execute(batch);
        delete[] dataElement;
    }
    else
    {
        for (size_t i = 0; i < attribute.m_DataArray.size(); ++i)
        {
            dataSize = attribute.m_DataArray.data()[i].length() + 1;
            j_object_write(dataObject, attribute.m_DataArray.data()[i].c_str(),
                           dataSize, offset, &bytesWritten, batch);
            offset += dataSize;
        }
        j_batch_execute(batch);
    }

    if (bytesWritten == offset)
    {
        std::cout << "++ Julea Interaction Writer: Data written for \
            attribute "
                  << attrName << std::endl;
    }
    else
    {
        std::cout << "WARNING: only " << bytesWritten
                  << " bytes written instead of " << offset << " bytes! "
                  << std::endl;
    }
    free(attrName);
    g_free(stringDataObject);
    j_object_unref(dataObject);
    j_batch_unref(batch);
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}

template <class T>
void PutAttributeMetadataToJulea(Attribute<T> &attribute, bson_t *bsonMetaData,
                                 const std::string nameSpace)
{
    std::cout << "-- PutAttributeMetadataToJulea ------ " << std::endl;
    guint32 valueLen = 0;

    bson_iter_t bIter;
    bson_t *bsonNames;

    void *namesBuf = NULL;
    void *metaDataBuf = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    auto attrName = strdup(attribute.m_Name.c_str());

    /* names_kv = kv holding all variable names */
    auto kvObjectNames = j_kv_new("attribute_names", nameSpace.c_str());
    j_kv_get(kvObjectNames, &namesBuf, &valueLen, batch);
    j_batch_execute(batch);

    if (valueLen == 0)
    {
        bsonNames = bson_new();
    }
    else
    {
        bsonNames = bson_new_from_data((const uint8_t *)namesBuf, valueLen);
    }

    /* Check if variable name is already in kv store */
    if (!bson_iter_init_find(&bIter, bsonNames, attrName))
    {
        std::cout << "Init b_iter successfull " << std::endl;
        // bson_append_int32(bsonNames, varName, -1,
        // bsonMetaData->var_type);
        // //FIXME: var_type?!
    }
    else
    {
        std::cout << "++ Julea Interaction Writer: Attribute " << attrName
                  << " already in kv store. " << std::endl;
        // TODO: update variable -> is there anything else necessary to
        // do?
    }

    /* Write metadata struct to kv store*/
    auto stringMetadataKV = g_strdup_printf("attributes_%s", nameSpace.c_str());
    auto kvObjectMetadata = j_kv_new(stringMetadataKV, attrName);

    metaDataBuf = g_memdup(bson_get_data(bsonMetaData), bsonMetaData->len);
    namesBuf = g_memdup(bson_get_data(bsonNames), bsonNames->len);

    j_kv_put(kvObjectMetadata, metaDataBuf, bsonMetaData->len, g_free, batch2);
    j_kv_put(kvObjectNames, namesBuf, bsonNames->len, g_free, batch2);

    j_batch_execute(batch2); // Writing metadata

    free(attrName);
    g_free(stringMetadataKV);
    j_kv_unref(kvObjectNames);
    j_kv_unref(kvObjectMetadata);
    j_batch_unref(batch);
    j_batch_unref(batch2);
    bson_destroy(bsonNames);
    j_semantics_unref(semantics);

    std::cout << "++ Julea Interaction Writer: Put Attribute " << std::endl;
}
// template void WriteVarMetadataToJuleaKV(Variable<T> &variable, const std::string nameSpace,size_t currStep);\

#define declare_template_instantiation(T)                                      \
    template void PutVariableDataToJulea(Variable<T> &variable, const T *data, \
                                         const std::string nameSpace,          \
                                         size_t currentStep, size_t blockID);  \
    template void PutVariableMetadataToJulea(                                  \
        Variable<T> &variable, gpointer &md, guint32 valueLen,                 \
        const std::string nameSpace, size_t currentStep, size_t blockID,       \
        bool isNameWritten);                                                   \
                                                                               \
    template void PutAttributeDataToJulea(Attribute<T> &attribute,             \
                                          const std::string nameSpace);        \
    template void PutAttributeDataToJuleaSmall(                                \
        Attribute<T> &attribute, const T *data, const std::string nameSpace);  \
    template void PutAttributeMetadataToJulea(Attribute<T> &attribute,         \
                                              bson_t *bsonMetaData,            \
                                              const std::string nameSpace);    \
    template void PutAttributeMetadataToJuleaSmall(                            \
        Attribute<T> &attribute, bson_t *bsonMetaData,                         \
        const std::string nameSpace);

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
