/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.cpp
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

// #include "JuleaInteraction.h"
// #include "JuleaInteraction.tcc"
// #include "adios2/helper/adiosFunctions.h" // IsRowMajor

// #include <complex>
// #include <ios>
// #include <iostream>
#include "JuleaCDO.h"
#include <stdexcept>
#include <vector>

#include <cstring> // strlen

namespace adios2
{
namespace interop
{

JuleaCDO::JuleaCDO(helper::Comm const &comm)
{
    // std::cout << "This is the constructor" << std::endl;
}

void precomputeCFD(void)
{
    // TODO:
}
void computeDailyMinimum(const std::string nameSpace, std::string variableName,
                         uint32_t entryID)
{
    // TODO:
}

} // end namespace interop
} // end namespace adios
