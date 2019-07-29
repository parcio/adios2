/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEATESTWRITER_TCC_
#define ADIOS2_ENGINE_JULEATESTWRITER_TCC_

#include "JuleaTestWriter.h"

#include <adios2_c.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

namespace adios2
{
namespace core
{
namespace engine
{

// TODO: necessary function?
template <class T>
void JuleaTestWriter::PutSyncCommon(Variable<T> &variable,
                                const typename Variable<T>::Info &blockInfo)
{
    /* ParseVariableType */
    /* GetMinMax */
    /* PutVariableToJulea*/

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaTestWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    // create and initialize metadata struct
    // parse variable to metadata struct
    // parse variable type
    // get min max
    // put metadata to julea
        // kv get variable_names kv
        //
    // put data to julea
    //
        // bson new
        // var metadata to bson
        // kv put
        // object write
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaTestWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    // Parse variable type
    // get min max
    // put metadata to julea
    // put data to julea
    // add deferred variable

    // m_DeferredVariables.insert(variable.m_Name)
    // m_NeedPerformPuts = true;

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << "     PutDeferred("
                  << variable.m_Name << ")\n";
    }
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEATESTWRITER_TCC_ */
