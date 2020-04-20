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
void JuleaKVWriter::PutSyncToJulea(Variable<T> &variable, const T *data,
                                   const typename Variable<T>::Info &blockInfo)
{
    std::cout << "------------------ PutSyncToJulea --------- BlockID: "
              << m_CurrentBlockID << std::endl;

    guint32 blockMD_len = 0;
    guint32 varMD_len = 0;
    gpointer md_buffer = NULL;
    auto bsonMetadata = bson_new();

    SetMinMax(variable, data);
    auto stepBlockID =
        g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
    std::cout << "    stepBlockID: " << stepBlockID << std::endl;

    gpointer varMD =
        SerializeVariableMetadata(variable, varMD_len, m_CurrentStep);
    gpointer blockMD = SerializeBlockMetadata(
        variable, blockMD_len, m_CurrentStep, m_CurrentBlockID, blockInfo);

    // check whether variable name is already in variable_names kv
    auto itVariableWritten = m_WrittenVariableNames.find(variable.m_Name);
    if (itVariableWritten == m_WrittenVariableNames.end())
    {
        std::cout << "--- Variable name not yet written " << std::endl;

        PutNameToJulea(variable.m_Name, m_Name, "variable_names");
        m_WrittenVariableNames.insert(variable.m_Name);
    }

    // std::cout << "Variable names written to the names kv: " << std::endl;
    for (auto it = m_WrittenVariableNames.begin();
         it != m_WrittenVariableNames.end(); ++it)
    {
        std::cout << "___ Written variables:" << ' ' << *it << std::endl;
    }

    PutVariableMetadataToJulea(m_Name, varMD, varMD_len, variable.m_Name);
    PutBlockMetadataToJulea(m_Name, variable.m_Name, blockMD, blockMD_len,
                            stepBlockID);
    PutVariableDataToJulea(variable, data, m_Name, m_CurrentStep,
                           m_CurrentBlockID);

    // m_PutBlockID ++;
    // std::cout << " --------------------- BlockID is now: " << m_PutBlockID <<
    // std::endl;

    bson_destroy(bsonMetadata);
}

template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable,
                                  const typename Variable<T>::Info &blockInfo)
{

    std::cout << "\n_________________________PutSyncCommon "
                 "BlockInfo_____________________________"
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank
              << " Variable name: " << variable.m_Name << std::endl;

    std::cout << "    CurrentStep: " << m_CurrentStep << std::endl;
    PutSyncToJulea(variable, blockInfo.Data, blockInfo);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    std::cout << "\n___________________________PutSyncCommon "
                 "T__________________________"
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Writer " << m_WriterRank
              << " Variable name: " << variable.m_Name << std::endl;

    std::cout << "    CurrentStep: " << m_CurrentStep << std::endl;

    // FIXME: needed for serializing but does it introduce new problems?
    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());
    PutSyncToJulea(variable, data, blockInfo);
    // PutSyncToJulea(variable, data);

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
    }
}

template <class T>
void JuleaKVWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    std::cout << "\n___________________________PutDeferred "
                 "T____________________________"
              << std::endl;
    std::cout << "data[0]: " << data[0] << std::endl;
    std::cout << "data[1]: " << data[1] << std::endl;

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());

    std::cout << "BlockInfo size: " << variable.m_BlocksInfo.size()
              << std::endl;
    if (variable.m_SingleValue)
    {
        std::cout << "variable.m_SingleValue: " << variable.m_SingleValue
                  << std::endl;
        DoPutSync(variable, data); // TODO: correct?!
        return;
    }

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
    std::cout << "BlockInfo.size = " << variable.m_BlocksInfo.size()
              << std::endl;
    for (size_t i = 0; i < variable.m_BlocksInfo.size(); ++i)
    {
        std::cout << "variable: " << variable.m_Name << "--- i: " << i << std::endl;
        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
            m_CurrentBlockID);

        // FIXME: needed for deserialize problem with m_BlocksInfo.size()?
        //  const typename Variable<T>::Info blockInfo =
        // variable.SetBlockInfo(data, CurrentStep());

        /** if there are no SpanBlocks simply put every variable */
        auto itSpanBlock = variable.m_BlocksSpan.find(i);
        if (itSpanBlock == variable.m_BlocksSpan.end())
        {
            // std::cout << "m_CurrentBlockID = i: " << m_CurrentBlockID
                      // << std::endl;
            PutSyncCommon(variable,
                          variable.m_BlocksInfo[i]);
            m_CurrentBlockID = m_CurrentBlockID + i + 1;
        }
        // else
        // {
        //     m_BP3Serializer.PutSpanMetadata(variable, itSpanBlock->second);
        // }
    }

    variable.m_BlocksInfo.clear();
    variable.m_BlocksSpan.clear();
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAKVWRITER_TCC_ */
