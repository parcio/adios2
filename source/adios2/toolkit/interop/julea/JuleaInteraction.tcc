/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.cpp
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAINTERACTION_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAINTERACTION_TCC_

#include "JuleaInteraction.h"
#include "adios2/helper/adiosFunctions.h" // IsRowMajor

#include <complex>
#include <ios>
#include <iostream>
#include <stdexcept>
#include <vector>

#include <cstring> // strlen

namespace adios2
{
namespace interop
{

/**
 * @brief Now uses
 *
 * @tparam T
 * @param variable
 * @param data
 * @param projectNamespace
 * @param fileName
 * @param ID either entryID or blockID for kv backend
 * @param isKV bool to determine used backend -> uniqueid
 */
template <class T>
void JuleaInteraction::PutVariableDataToJulea(
    core::Variable<T> &variable, const T *data,
    const std::string projectNamespace, const std::string fileName,
    const size_t step, uint32_t ID, bool isKV) const
{
    JObject *dataObject;
    // std::cout << "--- PutVariableDataToJulea ----- " << std::endl;
    // std::cout << "data: " << data[0] << std::endl;
    // std::cout << "data: " << data[1] << std::endl;
    guint64 bytesWritten = 0;
    std::string objName = "variableblocks";

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    j_semantics_set(semantics, J_SEMANTICS_SAFETY, J_SEMANTICS_SAFETY_STORAGE);
    auto batch = j_batch_new(semantics);

    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    auto dataSize = variable.m_ElementSize * numberElements;

    // auto stringDataObject = g_strdup_printf(
    //     "%s_%s_%s", fileName.c_str(), variable.m_Name.c_str(),
    //     objName.c_str());
    auto stringDataObject = g_strdup_printf(
        "%s_%s_%s_%s", projectNamespace.c_str(), fileName.c_str(),
        variable.m_Name.c_str(), objName.c_str());
    // const char id = (char) entryID;

    if (isKV)
    {
        auto stepBlockID = g_strdup_printf("%lu_%lu", step, ID);
        dataObject = j_object_new(stringDataObject, stepBlockID);
    }
    else
    {
        auto uniqueID = g_strdup_printf("%d", ID);
        dataObject = j_object_new(stringDataObject, uniqueID);
    }

    j_object_create(dataObject, batch);
    j_object_write(dataObject, data, dataSize, 0, &bytesWritten, batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesWritten == dataSize)
    {
        // std::cout << "++ Julea Interaction Writer: Data written for:  "
        // << stepBlockID << std::endl;
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

    // std::cout << "++ Julea Interaction: PutVariableDataToJulea" << std::endl;
}

template <class T>
void JuleaInteraction::GetVariableDataFromJulea(
    core::Variable<T> &variable, T *data, const std::string projectNamespace,
    const std::string fileName, size_t offset, size_t dataSize,
    const size_t step, uint32_t ID, bool isKV) const
{
    // std::cout << "-- GetVariableDataFromJulea ----- " << std::endl;
    JObject *dataObject;
    guint64 bytesRead = 0;
    const char *varName = variable.m_Name.c_str();

    std::string objName = "variableblocks";
    // auto stringDataObject =
    //     g_strdup_printf("%s_%s_%s", nameSpace.c_str(),
    //     variable.m_Name.c_str(),
    //                     objName.c_str());
    auto stringDataObject = g_strdup_printf(
        "%s_%s_%s_%s", projectNamespace.c_str(), fileName.c_str(),
        variable.m_Name.c_str(), objName.c_str());
    // std::cout << "stringDataObject: " << stringDataObject << "\n";
    // std::cout << "stringDataObject: " << stringDataObject << std::endl;
    // auto uniqueID = g_strdup_printf("%d", entryID);

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    if (isKV)
    {
        auto stepBlockID = g_strdup_printf("%lu_%lu", step, ID);
        std::cout << "stepBlockID: " << stepBlockID << "\n";
        dataObject = j_object_new(stringDataObject, stepBlockID);
    }
    else
    {
        auto uniqueID = g_strdup_printf("%d", ID);
        dataObject = j_object_new(stringDataObject, uniqueID);
    }

    j_object_read(dataObject, data, dataSize, offset, &bytesRead, batch);
    g_assert_true(j_batch_execute(batch) == true);

    if (bytesRead == dataSize)
    {
        // std::cout << "Data[0] = " << data[0] << std::endl;
        // std::cout << "Data[1] = " << data[1] << std::endl;
        // std::cout << "Data[2] = " << data[2] << std::endl;
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

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAINTERACTION_TCC_ */