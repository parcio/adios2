/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEADBWRITER_TCC_
#define ADIOS2_ENGINE_JULEADBWRITER_TCC_

#include "JuleaDBInteractionWriter.h"
#include "JuleaDBWriter.h"

// #include <adios2_c.h>
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
void JuleaDBSetMinMax(Variable<T> &variable, const T *data, T &blockMin,
                      T &blockMax)
{
    T min;
    T max;

    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    adios2::helper::GetMinMax(data, number_elements, min, max);

    blockMin = min;
    blockMax = max;

    if (min < variable.m_Min)
    {
        // std::cout << "updated global min" << std::endl;
        variable.m_Min = min;
    }
    if (max > variable.m_Max)
    {
        // std::cout << "updated global max" << std::endl;
        variable.m_Max = max;
    }
    if (false)
    {
        std::cout << "min: " << min << std::endl;
        std::cout << "global min: " << variable.m_Min << std::endl;
        std::cout << "max: " << max << std::endl;
        std::cout << "global max: " << variable.m_Max << std::endl;
    }
}

template <>
void JuleaDBSetMinMax<std::complex<float>>(
    Variable<std::complex<float>> &variable, const std::complex<float> *data,
    std::complex<float> &blockMin, std::complex<float> &blockMax)
{
    // TODO implement?
}

template <>
void JuleaDBSetMinMax<std::complex<double>>(
    Variable<std::complex<double>> &variable, const std::complex<double> *data,
    std::complex<double> &blockMin, std::complex<double> &blockMax)
{
    // TODO implement?
}

template <class T>
void JuleaDBWriter::PutSyncToJulea(Variable<T> &variable, const T *data,
                                   const typename Variable<T>::Info &blockInfo)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank
                  << "     PutSyncToJulea(" << variable.m_Name
                  << " ---- BlockID: " << m_CurrentBlockID << std::endl;
    }
    T blockMin;
    T blockMax;

    JuleaDBSetMinMax(variable, data, blockMin, blockMax);

    auto stepBlockID =
        g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
    std::cout << "    stepBlockID: " << stepBlockID << std::endl;

    // check whether variable name is already in variable_names DB
    auto itVariableWritten = m_WrittenVariableNames.find(variable.m_Name);
    if (itVariableWritten == m_WrittenVariableNames.end())
    {
        if (m_Verbosity == 5)
        {
            std::cout << "--- Variable name not yet written with this writer "
                      << std::endl;
        }

        m_WrittenVariableNames.insert(variable.m_Name);
    }

    // std::cout << "Variable names written to the names DB: " << std::endl;
    if (m_Verbosity == 5)
    {
        for (auto it = m_WrittenVariableNames.begin();
             it != m_WrittenVariableNames.end(); ++it)
        {
            // std::cout << "___ Written variables:" << ' ' << *it << std::endl;
        }
    }

    /** updates the variable metadata as there is a new block now */
    DBPutVariableMetadataToJulea(variable, m_Name, variable.m_Name,
                                 m_CurrentStep, m_CurrentBlockID);

    /** put block metadata to DB */
    DBPutBlockMetadataToJulea(variable, m_Name, variable.m_Name, m_CurrentStep,
                              m_CurrentBlockID, blockInfo, blockMin, blockMax);
    /** put data to object store */
    DBPutVariableDataToJulea(variable, data, m_Name, m_CurrentStep,
                             m_CurrentBlockID);
}

template <class T>
void JuleaDBWriter::PutSyncCommon(Variable<T> &variable,
                                  const typename Variable<T>::Info &blockInfo)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
        std::cout << "\n_________________________PutSyncCommon "
                     "BlockInfo_____________________________"
                  << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Namespace: " << m_Name << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Variable name: " << variable.m_Name << std::endl;

        std::cout << "    CurrentStep: " << m_CurrentStep << std::endl;
    }

    PutSyncToJulea(variable, blockInfo.Data, blockInfo);
}

template <class T>
void JuleaDBWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
        std::cout << "\n___________________________PutSyncCommon "
                     "T__________________________"
                  << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Namespace: " << m_Name << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Variable name: " << variable.m_Name << std::endl;

        std::cout << "    CurrentStep: " << m_CurrentStep << std::endl;
    }

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());
    PutSyncToJulea(variable, data, blockInfo);
}

template <class T>
void JuleaDBWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank << "     PutDeferred("
                  << variable.m_Name << ")\n";
        std::cout << "\n___________________________PutDeferred "
                     "T____________________________"
                  << std::endl;
        std::cout << "data[0]: " << data[0] << std::endl;
        // std::cout << "data[1]: " << data[1] << std::endl;
    }

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());

    // if (variable.m_SingleValue)
    // {
    // std::cout << "variable.m_SingleValue: " << variable.m_SingleValue
    // << std::endl;
    // DoPutSync(variable, data); // causes issues with blockID when no
    // steps and no bpls return;
    // }

    m_DeferredVariables.insert(variable.m_Name);
}

template <class T>
void JuleaDBWriter::PerformPutCommon(Variable<T> &variable)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________PerformPutCommon T_____________________"
                  << std::endl;
        std::cout << "BlockInfo.size = " << variable.m_BlocksInfo.size()
                  << std::endl;
    }
    for (size_t i = 0; i < variable.m_BlocksInfo.size(); ++i)
    {
        // std::cout << "variable: " << variable.m_Name << "--- i: " << i
        // << std::endl;
        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
            m_CurrentBlockID);

        /** if there are no SpanBlocks simply put every variable */
        auto itSpanBlock = variable.m_BlocksSpan.find(i);
        if (itSpanBlock == variable.m_BlocksSpan.end())
        {
            PutSyncCommon(variable, variable.m_BlocksInfo[i]);
            // m_CurrentBlockID = m_CurrentBlockID + i + 1;
            m_CurrentBlockID = m_CurrentBlockID + 1;
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

#endif /* ADIOS2_ENGINE_JULEADBWRITER_TCC_ */
