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
    std::cout << "Julea Writer " << m_WriterRank
              << " Reached Get Sync Common (T, T)" << std::endl;
    std::cout << "Julea Writer " << m_WriterRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank
              << " Variable name: " << variable.m_Name << std::endl;
    // std::cout << "variable.m_BlocksInfo.Step: " <<
    // variable.m_BlocksInfo[0].Step << std::endl; std::cout <<
    // "variable.m_BlocksInfo.StepsStart: " <<
    // variable.m_BlocksInfo[0].StepsStart << std::endl; std::cout <<
    // "variable.m_BlocksInfo.StepsCount: " <<
    // variable.m_BlocksInfo[0].StepsCount << std::endl; std::cout <<
    // "variable.m_AvailableStepsStart: " << variable.m_AvailableStepsStart <<
    // std::endl; std::cout << "variable.m_AvailableStepsCount: " <<
    // variable.m_AvailableStepsCount << std::endl; std::cout <<
    // "variable.m_StepsStart: " << variable.m_StepsStart << std::endl;
    // std::cout << "variable.m_StepsCount: " << variable.m_StepsCount <<
    // std::endl;

    std::cout << "DEBUG: CurrentStep" << m_CurrentStep << std::endl;
    std::cout << "---------------------\n" << std::endl;

    auto bsonMetadata = bson_new();
    guint32 buffer_len = 0;
    SetMinMax(variable, blockInfo.Data);
    gpointer md_buffer = NULL;
    // ParseVariableToBSON(variable, bsonMetadata);
    // ParseVarTypeToBSON(variable, blockInfo.Data, bsonMetadata);

    // FIXME: implement
    md_buffer =
        GetMetadataBuffer(variable, buffer_len, m_CurrentStep, m_CurrentBlockID);

    // check whether variable name is already in variable_names kv
    auto itVariableWritten = m_WrittenVariableNames.find(variable.m_Name);
    if (itVariableWritten == m_WrittenVariableNames.end())
    {
        // PutVariableMetadataBSONToJulea(variable, bsonMetadata, m_Name,
        //                                m_CurrentStep, m_CurrentBlockID,
        //                                false);
        PutVariableMetadataToJulea(variable, md_buffer, buffer_len, m_Name,
                                   m_CurrentStep, m_CurrentBlockID, false);
        m_WrittenVariableNames.insert(variable.m_Name);
    }
    else
    {
        // PutVariableMetadataBSONToJulea(variable, bsonMetadata, m_Name,
        // m_CurrentStep, m_CurrentBlockID, true);
        PutVariableMetadataToJulea(variable, md_buffer, buffer_len, m_Name,
                                   m_CurrentStep, m_CurrentBlockID, true);
    }

    std::cout << "Variable names written to the names kv: " << std::endl;
    for (auto it = m_WrittenVariableNames.begin();
         it != m_WrittenVariableNames.end(); ++it)
    {
        std::cout << ' ' << *it << std::endl;
    }

    // every step PutVariableDataToJulea(variable, data, m_Name, m_CurrentStep);

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

    std::cout << "DEBUG: CurrentStep" << m_CurrentStep << std::endl;

    // std::cout << "variable.m_BlocksInfo.Step: " <<
    // variable.m_BlocksInfo[0].Step << std::endl; std::cout <<
    // "variable.m_BlocksInfo.StepsStart: " <<
    // variable.m_BlocksInfo[0].StepsStart << std::endl; std::cout <<
    // "variable.m_BlocksInfo.StepsCount: " <<
    // variable.m_BlocksInfo[0].StepsCount << std::endl; std::cout <<
    // "variable.m_AvailableStepsStart: " << variable.m_AvailableStepsStart <<
    // std::endl; std::cout << "variable.m_AvailableStepsCount: " <<
    // variable.m_AvailableStepsCount << std::endl; std::cout <<
    // "variable.m_StepsStart: " << variable.m_StepsStart << std::endl;
    // std::cout << "variable.m_StepsCount: " << variable.m_StepsCount <<
    // std::endl;

    std::cout << "---------------------\n" << std::endl;

    // ParseVariableToBSON(variable, bsonMetadata);
    // ParseVarTypeToBSON(variable, data, bsonMetadata);

    // FIXME: create bson storing max number of steps + steps bitmap

    // PutVariableMetadataToJulea(variable, bsonMetadata, m_Name); //FIXME: for
    // every step PutVariableDataToJulea(variable, data, m_Name, m_CurrentStep);

    // FIXME: store bson in variables kv

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
    std::cout << "data[1]: " << data[1] << std::endl;

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
              << variable.m_BlocksInfo[0].Data[0] << std::endl;
    std::cout << "variable.m_BlocksInfo[0].Data: "
              << variable.m_BlocksInfo[0].Data[1] << std::endl;
    // std::cout << "blockInfo.Data: " << blockInfo.Data << std::endl;
    // std::cout << "blockInfo.Data[0]: " << blockInfo.Data[0] << std::endl;
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
        std::cout
            << "------- TEST for different info block sizes ----------------"
            << std::endl;
        std::cout << "variable:" << variable.m_Name << std::endl;
        std::cout << "i = " << i << std::endl;
        // std::cout << "variable.m_BlocksInfo.Step: "
        //           << variable.m_BlocksInfo[i].Step << std::endl;
        // std::cout << "variable.m_BlocksInfo.StepsStart: "
        //           << variable.m_BlocksInfo[i].StepsStart << std::endl;
        // std::cout << "variable.m_BlocksInfo.StepsCount: "
        //           << variable.m_BlocksInfo[i].StepsCount << std::endl;
        // std::cout << "variable.m_AvailableStepsStart: "
        //           << variable.m_AvailableStepsStart << std::endl;
        // std::cout << "variable.m_AvailableStepsCount: "
        //           << variable.m_AvailableStepsCount << std::endl;
        // std::cout << "variable.m_StepsStart: " << variable.m_StepsStart
        //           << std::endl;
        std::cout << "variable.m_StepsCount: " << variable.m_StepsCount
                  << std::endl;
        // std::cout << "variable.m_StepsCount: " <<
        // variable.m_AvailableStepBlockIndexOffsets[0] << std::endl;

        std::cout << "Map size = "
                  << variable.m_AvailableStepBlockIndexOffsets.size()
                  << std::endl;

        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
            variable.m_BlocksInfo.size());

        // PutSyncCommon(variable, variable.m_BlocksInfo[i]);
        // PutSyncCommon(variable, variable.m_BlocksInfo[i].Data);

        /** if there are no SpanBlocks simply put every variable */
        auto itSpanBlock = variable.m_BlocksSpan.find(i);
        if (itSpanBlock == variable.m_BlocksSpan.end())
        {
            m_CurrentBlockID = i;
            PutSyncCommon(variable,
                          variable.m_BlocksInfo[i]); // FIXME: check BSON stuff
        }
        // else
        // {
        //     m_BP3Serializer.PutSpanMetadata(variable, itSpanBlock->second);
        // }

        std::cout << "Map size = "
                  << variable.m_AvailableStepBlockIndexOffsets.size()
                  << std::endl;
        // DEBUG:
        if (variable.m_AvailableStepBlockIndexOffsets.size() > 0)
        {
            auto it = variable.m_AvailableStepBlockIndexOffsets.begin();
        }
        // std::cout << "variable.m_StepsCount: " << it->second << std::endl;
        // auto &position = m_BPSerializer.m_Data.m_Position;
        // auto &absolutePosition = m_BPSerializer.m_Data.m_AbsolutePosition;

        // std::cout << "position: " << position << std::endl;
        // std::cout << "absolutePosition: " << absolutePosition << std::endl;
        // std::cout << "Mode: " << m_OpenMode << std::endl;
    }
    std::cout << "BlockInfo.size = " << variable.m_BlocksInfo.size();

    variable.m_BlocksInfo.clear();
    variable.m_BlocksSpan.clear();
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAKVWRITER_TCC_ */
