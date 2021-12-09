/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.cpp
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

#include "JuleaInteraction.h"
// #include "JuleaSerializer.tcc"
// #include "adios2/common/ADIOSMPI.h"
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

// JuleaInteraction::JuleaInteraction(const bool debugMode) :
// m_DebugMode(debugMode)
JuleaInteraction::JuleaInteraction(helper::Comm const &comm)
{
    std::cout << "This is the constructor" << std::endl;
}
// JuleaInteraction::JuleaInteraction() {}


template <class T>
void JuleaInteraction::PutVariableDataToJulea(core::Variable<T> &variable, const T *data,
                              const std::string nameSpace, uint32_t entryID) const
{
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

    // auto stepBlockID = g_strdup_printf("%lu_%lu", currStep, block);
    auto uniqueID = g_strdup_printf("%d", entryID);
    auto stringDataObject =
        g_strdup_printf("%s_%s_%s", nameSpace.c_str(), variable.m_Name.c_str(),
                        objName.c_str());
    // const char id = (char) entryID;

    // auto dataObject = j_object_new(stringDataObject, stepBlockID);
    auto dataObject = j_object_new(stringDataObject, uniqueID);

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



void JuleaInteraction::SetMinMaxValueFields(std::string *minField, std::string *maxField,
                          std::string *valueField, std::string *meanField,
                          const adios2::DataType varType)
{
    switch (varType)
    {
    case adios2::DataType::None:
        // TODO: Do something?
        break;
    case adios2::DataType::Int8:
    case adios2::DataType::UInt8:
    case adios2::DataType::Int16:
    case adios2::DataType::UInt16:
    case adios2::DataType::Int32:
        *minField = "min_sint32";
        *maxField = "max_sint32";
        *valueField = "value_sint32";
        break;
    case adios2::DataType::UInt32:
        *minField = "min_uint32";
        *maxField = "max_uint32";
        *valueField = "value_uint32";
        break;
    case adios2::DataType::Int64:
        *minField = "min_sint64";
        *maxField = "max_sint64";
        *valueField = "value_sint64";
        break;
    case adios2::DataType::UInt64:
        *minField = "min_uint64";
        *maxField = "max_uint64";
        *valueField = "value_uint64";
        break;
    case adios2::DataType::Float:
        *minField = "min_float32";
        *maxField = "max_float32";
        *valueField = "value_float32";
        break;
    case adios2::DataType::Double:
        *minField = "min_float64";
        *maxField = "max_float64";
        *valueField = "value_float64";
        *meanField = "mean_float64";
        break;
    case adios2::DataType::LongDouble:
    case adios2::DataType::FloatComplex:
    case adios2::DataType::DoubleComplex:
        *minField = "min_blob";
        *maxField = "max_blob";
        *valueField = "value_blob";
        break;
    case adios2::DataType::String:
        *valueField = "value_sint32";
        break;
    case adios2::DataType::Compound:
        std::cout << "Compound variables not supported";
        break;
    }
}

#define declare_template_instantiation(T)                                      \
    template void JuleaInteraction::PutVariableDataToJulea(                                    \
        core::Variable<T> &variable, const T *data, const std::string nameSpace,     \
        uint32_t entryID) const;                                                     
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios
