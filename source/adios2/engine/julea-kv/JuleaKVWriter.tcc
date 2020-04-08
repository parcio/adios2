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

template <class T>
void JuleaKVWriter::PutSyncToJulea(Variable<T> &variable, const T *data)
{
    auto bsonMetadata = bson_new();
    guint32 buffer_len = 0;
    SetMinMax(variable, data);
    gpointer md_buffer = NULL;

    md_buffer = GetMetadataBuffer(variable, buffer_len, m_CurrentStep,
                                  m_CurrentBlockID);

    // check whether variable name is already in variable_names kv
    auto itVariableWritten = m_WrittenVariableNames.find(variable.m_Name);
    if (itVariableWritten == m_WrittenVariableNames.end())
    {
        PutVariableMetadataToJulea(variable, md_buffer, buffer_len, m_Name,
                                   m_CurrentStep, m_CurrentBlockID, false);
        m_WrittenVariableNames.insert(variable.m_Name);
    }
    else
    {
        PutVariableMetadataToJulea(variable, md_buffer, buffer_len, m_Name,
                                   m_CurrentStep, m_CurrentBlockID, true);
    }
    // std::cout << "Variable names written to the names kv: " << std::endl;

    for (auto it = m_WrittenVariableNames.begin();
         it != m_WrittenVariableNames.end(); ++it)
    {
        std::cout << ' ' << *it << std::endl;
    }

    PutVariableDataToJulea(variable, data, m_Name, m_CurrentStep,
                           m_CurrentBlockID);

    bson_destroy(bsonMetadata);
}

template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable,
                                  const typename Variable<T>::Info &blockInfo)
{

    std::cout << "\n______________PutSyncCommon BlockInfo_____________________"
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank
              << " Variable name: " << variable.m_Name << std::endl;

    std::cout << "DEBUG: CurrentStep" << m_CurrentStep << std::endl;
    std::cout << "---------------------\n" << std::endl;

    PutSyncToJulea(variable, blockInfo.Data);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    std::cout << "\n______________PutSyncCommon T_____________________"
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank
              << " Variable name: " << variable.m_Name << std::endl;

    std::cout << "DEBUG: CurrentStep" << m_CurrentStep << std::endl;
    std::cout << "---------------------\n" << std::endl;
    PutSyncToJulea(variable, data);

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

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutDeferred("
                  << variable.m_Name << ")\n";
    }
    m_DeferredVariables.insert(variable.m_Name);

    // TODO: change m_DeferredVariablesDataSize?

    // m_NeedPerformPuts = true; //TODO: currently not used but number of
    // deferredVariables
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
        std::cout << "Map size = "
                  << variable.m_AvailableStepBlockIndexOffsets.size()
                  << std::endl;

        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
            variable.m_BlocksInfo.size());

        /** if there are no SpanBlocks simply put every variable */
        auto itSpanBlock = variable.m_BlocksSpan.find(i);
        if (itSpanBlock == variable.m_BlocksSpan.end())
        {
            std::cout << "m_CurrentBlockID = i: " << m_CurrentBlockID
                      << std::endl;
            PutSyncCommon(variable,
                          variable.m_BlocksInfo[i]); // FIXME: check BSON stuff
            m_CurrentBlockID = m_CurrentBlockID + i;
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
    }
    std::cout << "BlockInfo.size = " << variable.m_BlocksInfo.size();

    variable.m_BlocksInfo.clear();
    variable.m_BlocksSpan.clear();
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAKVWRITER_TCC_ */
