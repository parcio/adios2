/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaDAI.cpp
 *
 *  Created on: May 13, 2022
 *      Author: Kira Duwe
 */

// #include "JuleaInteraction.h"
// #include "JuleaInteraction.tcc"
// #include "adios2/helper/adiosFunctions.h" // IsRowMajor

// #include <complex>
// #include <ios>
// #include <iostream>
#include "JuleaDAI.h"

#include <stdexcept>
#include <vector>

#include <cstring> // strlen

namespace adios2
{
namespace interop
{

// JuleaDAI::JuleaDAI(helper::Comm const &comm)
JuleaDAI::JuleaDAI(helper::Comm const &comm) : m_Comm(comm)
// JuleaDAI::JuleaDAI()
{
    // m_WriterRank = m_Comm.Rank();
    // m_SizeMPI = m_Comm.Size();
    // std::cout << "This is the constructor" << std::endl;
    // m_TemperatureName =
    // m_PrecipitationName =
}

void JuleaDAI::Init(helper::Comm const &comm)
{
    //    m_Comm = comm;z
}
// #define declare_template_instantiation(T)                                      \

// ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
// #undef declare_template_instantiation

} // end namespace interop
} // end namespace adios
