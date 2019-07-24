/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaDeserializer.cpp
 *
 *  Created on: July 24, 2019
 *      Author: Kira Duwe
 */

#include "JuleaDeserializer.h"
#include "JuleaDeserializer.tcc"

#include <complex>
#include <ios>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "adios2/common/ADIOSMPI.h"
#include "adios2/helper/adiosFunctions.h" // IsRowMajor
#include <cstring>                        // strlen

namespace adios2
{
namespace interop
{

JuleaDeserializer::JuleaDeserializer(const bool debugMode) : m_DebugMode(debugMode)
{
}

void JuleaDeserializer::PrintMiniPenguin()
{
    std::cout << "   (o_ \n   (/)_ \n" << std::endl;
}

void JuleaDeserializer::PrintPenguinFamily()
{
    std::cout << "           (o_ \n   (o_     //\\ \n   (/)_    V_/_ "
              << std::endl;
}

void JuleaDeserializer::PrintLargePenguin()
{
    std::cout
        << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
        << std::endl;
}

void JuleaDeserializer::ParseParameters(core::IO &io) {}

void JuleaDeserializer::Init()
{
    std::cout << "\n----------------------- JULEA SERIALIZER "
                 "-------------------------"
              << std::endl;

    PrintPenguinFamily();

    std::cout << "Init JuleaDeserializer " << std::endl;
}

// void JuleaDeserializer::WriteAdiosSteps() {}

// read from all time steps
void JuleaDeserializer::ReadAllVariables(core::IO &io) {}

// read variables from the input timestep
void JuleaDeserializer::ReadVariables(unsigned int ts, core::IO &io) {}

void JuleaDeserializer::Close() {}

#define declare_template_instantiation(T)                                      \
    template void JuleaDeserializer::Read(core::Variable<T> &, const T *);

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios
