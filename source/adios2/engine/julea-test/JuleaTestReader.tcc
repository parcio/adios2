/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEATESTREADER_TCC_
#define ADIOS2_ENGINE_JULEATESTREADER_TCC_

#include "JuleaTestReader.h"

#include <iostream>
// #include <fstream>
// #include <string>

namespace adios2
{
namespace core
{
namespace engine
{

template <>
void JuleaTestReader::GetSyncCommon(Variable<std::string> &variable,
                                std::string *data)
{
    std::cout << "Julea Test Reader " << m_ReaderRank
              << " Reached Get Sync Common (String, String) " << std::endl;
    std::cout << "Julea Test Reader " << m_ReaderRank << " Namespace of variable "
              << m_Name << std::endl;
    /* Get variables from JULEA storage*/

    variable.m_Data = data;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test sReader " << m_ReaderRank << "     GetSync("
                  << variable.m_Name << ")\n";
    }
}

// inline needed? is in skeleton-engine
template <class T>
void JuleaTestReader::GetSyncCommon(Variable<T> &variable, T *data)
{
    std::cout << "Julea Test Reader " << m_ReaderRank
              << " Reached Get Sync Common (T, T)" << std::endl;
    std::cout << "Julea Test Reader " << m_ReaderRank << " Namespace of variable "
              << m_Name << std::endl;

    /* all the additional metadata which is not used in InitVariables has to be
     * read again */

    /* get variable data from JULEA storage*/
}

template <class T>
void JuleaTestReader::GetDeferredCommon(Variable<T> &variable, T *data)
{
    // std::cout << "JULEA ENGINE: GetDeferredCommon" << std::endl;
    // returns immediately
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << "     GetDeferred("
                  << variable.m_Name << ")\n";
    }
    m_NeedPerformGets = true;
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif // ADIOS2_ENGINE_JULEATESTREADER_TCC_
