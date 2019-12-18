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
// template <class T>
// void JuleaKVWriter::PutSyncCommon(Variable<T> &variable,
//                                   const typename Variable<T>::Info
//                                   &blockInfo)
// {
//     // auto bsonMetadata = bson_new();
//     // Metadata *metadata = g_slice_new(Metadata);

//      std::cout << "\n______________PutSync BlockInfo_____________________" <<
//      std::endl;
//     // SetMinMax(variable, blockInfo.Data);

//     // ParseVariableToMetadataStruct(variable, blockInfo, metadata);
//     // ParseVarTypeToBSON(variable, blockInfo, metadata);

//     // // PutVariableMetadataToJulea(variable, bsonMetadata, m_Name);
//     // PutVariableMetadataToJuleaSmall(variable, bsonMetadata,
//     //                            m_Name);
//     // // PutVariableDataToJulea(variable, blockInfo.Data, m_Name);
//     // PutVariableDataToJuleaSmall(variable, blockInfo.Data, m_Name);
//     // bson_destroy(bsonMetadata);

//     if (m_Verbosity == 5)
//     {
//         std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
//                   << variable.m_Name << ")\n";
//     }

//     // ParseVariableType(variable, blockInfo, metadata); //TODO: what needs
//     to
//     // be done?

//     // TODO: free memory
// }

// TODO: necessary function?
template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable,
                                  const typename Variable<T>::Info &blockInfo)
{

    std::cout << "\n______________PutSync BlockInfo_____________________"
              << std::endl;
    auto bsonMetadata = bson_new();
    SetMinMax(variable, blockInfo.Data);

    ParseVariableToBSON(variable, bsonMetadata);
    ParseVarTypeToBSON(variable, blockInfo.Data, bsonMetadata);
    PutVariableMetadataToJuleaSmall(variable, bsonMetadata, m_Name);
    PutVariableDataToJuleaSmall(variable, blockInfo.Data, m_Name);
    // TODO: free memory
    bson_destroy(bsonMetadata);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    auto bsonMetadata = bson_new();

    std::cout << "\n______________PutSync T_____________________" << std::endl;
    std::cout << "Julea Writer " << m_WriterRank
              << " Reached Get Sync Common (T, T)" << std::endl;
    std::cout << "Julea Writer " << m_WriterRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank
              << " Variable name: " << variable.m_Name << std::endl;
    SetMinMax(variable, data);

    ParseVariableToBSON(variable, bsonMetadata);
    ParseVarTypeToBSON(variable, data, bsonMetadata);

    // PutVariableMetadataToJulea(variable, bsonMetadata,
    //                                 m_Name);
    PutVariableMetadataToJuleaSmall(variable, bsonMetadata, m_Name);
    // PutVariableDataToJulea(variable, data, m_Name);
    PutVariableDataToJuleaSmall(variable, data, m_Name);

    bson_destroy(bsonMetadata);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaKVWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    std::cout << "\n______________PutDeferred T_____________________"
              << std::endl;

    std::cout << "variable.m_Data: " << variable.m_Data << std::endl;
    std::cout << "data[0]: " << data[0] << std::endl;

    if (variable.m_SingleValue)
    {
        std::cout << "variable.m_SingleValue: " << variable.m_SingleValue
                  << std::endl;
        DoPutSync(variable, data);
        return;
    }

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());

    std::cout << "variable.m_BlocksInfo[0].Data: "
              << variable.m_BlocksInfo[0].Data << std::endl;
    std::cout << "blockInfo.Data: " << blockInfo.Data << std::endl;
    std::cout << "blockInfo.Data[0]: " << blockInfo.Data[0] << std::endl;
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

    // m_NeedPerformPuts = true;
}

template <class T>
void JuleaKVWriter::PerformPutCommon(Variable<T> &variable)
{
    std::cout << "\n______________PerformPutCommon T_____________________"
              << std::endl;
    for (size_t i = 0; i < variable.m_BlocksInfo.size(); ++i)
    {
        // PutSyncCommon(variable, variable.m_BlocksInfo[i]);
        PutSyncCommon(variable, variable.m_BlocksInfo[i].Data);

        // auto itSpanBlock = variable.m_BlocksSpan.find(b);
        // if (itSpanBlock == variable.m_BlocksSpan.end())
        // {
        //     PutSyncCommon(variable, variable.m_BlocksInfo[b]);
        // }
        // else
        // {
        //     m_BP3Serializer.PutSpanMetadata(variable, itSpanBlock->second);
        // }
        std::cout << "Mode: " << m_OpenMode << std::endl;
    }

    variable.m_BlocksInfo.clear();
    variable.m_BlocksSpan.clear();
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAKVWRITER_TCC_ */