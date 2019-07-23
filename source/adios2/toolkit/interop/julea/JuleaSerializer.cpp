/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.cpp
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

#include "JuleaSerializer.h"
#include "JuleaSerializer.tcc"

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


JuleaSerializer::JuleaSerializer(const bool debugMode) : m_DebugMode(debugMode)
{

}

void JuleaSerializer::ParseParameters(core::IO &io)
{

}

void JuleaSerializer::Init()
{
}

void JuleaSerializer::WriteAdiosSteps()
{

}


// read from all time steps
void JuleaSerializer::ReadAllVariables(core::IO &io)
{

}


// read variables from the input timestep
void JuleaSerializer::ReadVariables(unsigned int ts, core::IO &io)
{

}


void JuleaSerializer::Close()
{

}


#define declare_template_instantiation(T)                                      \
    template void JuleaSerializer::Write(core::Variable<T> &, const T *);

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios
