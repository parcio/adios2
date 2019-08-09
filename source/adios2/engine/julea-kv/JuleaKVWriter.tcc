/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAKVWRITER_TCC_
#define ADIOS2_ENGINE_JULEAKVWRITER_TCC_

#include "JuleaFormatWriter.h"
#include "JuleaInteractionWriter.h"
#include "JuleaKVWriter.h"

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
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable,
                                  const typename Variable<T>::Info &blockInfo)
{
    auto bsonMetadata = bson_new();
    Metadata *metadata = g_slice_new(Metadata);

    SetMinMax(variable, blockInfo.Data);

    ParseVariableToMetadataStruct(variable, blockInfo, metadata);
    ParseVarTypeToBSON(variable, blockInfo, metadata);

    // PutVariableMetadataToJulea(variable, bsonMetadata,
                               // m_Name);
    PutVariableMetadataToJuleaSmall(variable, bsonMetadata,
                               m_Name);
    // PutVariableDataToJulea(variable, blockInfo.Data, m_Name);
    PutVariableDataToJuleaSmall(variable, blockInfo.Data, m_Name);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }

    // ParseVariableType(variable, blockInfo, metadata); //TODO: what needs to
    // be done?

    // TODO: free memory
}

template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    auto bsonMetadata = bson_new();

    SetMinMax(variable, data);

    ParseVariableToBSON(variable, bsonMetadata);
    ParseVarTypeToBSON(variable, data, bsonMetadata);

    PutVariableMetadataToJulea(variable, bsonMetadata,
                                    m_Name);
    // PutVariableMetadataToJuleaSmall(variable, bsonMetadata,
                                    // m_Name); //FIXME
    // PutVariableDataToJulea(variable, data, m_Name);
    PutVariableDataToJuleaSmall(variable, data, m_Name);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaKVWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    // std::cout << "JULEA ENGINE: PutDeferredCommon" << std::endl;
    // std::cout << "You successfully reached the JULEA engine with the DEFERRED
    // mode "<< std::endl;
    // variable.SetBlockInfo(data, CurrentStep());

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutDeferred("
                  << variable.m_Name << ")\n";
    }
    m_DeferredVariables.insert(variable.m_Name);

    // FIXME: adapt name!
    // DESIGN: is it necessary/useful to do this?
    // m_DeferredVariablesDataSize += static_cast<size_t>(
    //     1.05 * helper::PayloadSize(variable.m_Data, variable.m_Count) +
    //     4 * GetBPIndexSizeInData(variable.m_Name, variable.m_Count));

    m_NeedPerformPuts = true;
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAKVWRITER_TCC_ */
