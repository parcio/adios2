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
#include "JuleaInteraction.tcc"
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
    // std::cout << "This is the constructor" << std::endl;
}

void JuleaInteraction::PrintMiniPenguin()
{
    std::cout << "   (o_ \n   (/)_ \n" << std::endl;
}

void JuleaInteraction::PrintPenguinFamily()
{
    std::cout << "           (o_ \n   (o_     //\\ \n   (/)_    V_/_ "
              << std::endl;
}

void JuleaInteraction::PrintLargePenguin()
{
    std::cout
        << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
        << std::endl;
}

void JuleaInteraction::SetMinMaxValueFields(std::string *minField,
                                            std::string *maxField,
                                            std::string *valueField,
                                            std::string *meanField,
                                            std::string *sumField,
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
        *meanField = "mean_sint32";
        *sumField = "sum_sint32";
        break;
    case adios2::DataType::UInt32:
        *minField = "min_uint32";
        *maxField = "max_uint32";
        *valueField = "value_uint32";
        *meanField = "mean_uint32";
        *sumField = "sum_uint32";
        break;
    case adios2::DataType::Int64:
        *minField = "min_sint64";
        *maxField = "max_sint64";
        *valueField = "value_sint64";
        *meanField = "mean_sint64";
        *sumField = "sum_sint64";
        break;
    case adios2::DataType::UInt64:
        *minField = "min_uint64";
        *maxField = "max_uint64";
        *valueField = "value_uint64";
        *meanField = "mean_uint64";
        *sumField = "sum_uint64";
        break;
    case adios2::DataType::Float:
        *minField = "min_float32";
        *maxField = "max_float32";
        *valueField = "value_float32";
        *meanField = "mean_float32";
        *sumField = "sum_float32";
        break;
    case adios2::DataType::Double:
        *minField = "min_float64";
        *maxField = "max_float64";
        *valueField = "value_float64";
        *meanField = "mean_float64";
        *sumField = "sum_float64";
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
    template void JuleaInteraction::PutVariableDataToJulea(                    \
        core::Variable<T> &variable, const T *data,                            \
        const std::string projectNamespace, const std::string fileName,        \
        uint32_t entryID) const;                                               \
    template void JuleaInteraction::GetVariableDataFromJulea(                  \
        core::Variable<T> &variable, T *data,                                  \
        const std::string projectNamespace, const std::string fileName,        \
        size_t offset, long unsigned int dataSize, uint32_t entryID) const;
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios
