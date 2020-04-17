/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAREADER_TCC_
#define ADIOS2_ENGINE_JULEAREADER_TCC_

#include "JuleaFormatReader.h"
#include "JuleaInteractionReader.h"
#include "JuleaKVReader.h"

#include <iostream>

namespace adios2
{
namespace core
{
namespace engine
{

template <>
void JuleaKVReader::GetSyncCommon(Variable<std::string> &variable,
                                  std::string *data)
{
    g_autoptr(JBatch) batch = NULL;
    g_autoptr(JSemantics) semantics;

    gboolean use_batch = TRUE;
    semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    batch = j_batch_new(semantics);

    // FIXME: still using metadata struct
    // gchar *name_space = strdup(m_Name.c_str());
    // Metadata *metadata = g_slice_new(Metadata);
    // metadata->name = g_strdup(variable.m_Name.c_str());

    // std::cout << "\n______________GetSync String_____________________" <<
    // std::endl; std::cout << "Julea Reader " << m_ReaderRank
    //           << " Reached Get Sync Common (String, String) " << std::endl;
    // std::cout << "Julea Reader " << m_ReaderRank << " Namespace of variable "
    //           << m_Name << std::endl;
    // GetVarDataFromJulea(name_space, metadata->name, metadata->data_size,
    //                     (void *)(data), batch);

    // FIXME: additional metadata infos as "IsReadAsJoined" need to be stored in
    // ADIOS

    // std::cout << "JULEA ENGINE: GetSyncCommon (string data)" << std::endl;
    variable.m_Data = data;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << "     GetSync("
                  << variable.m_Name << ")\n";
    }
    j_batch_unref(batch);
    j_semantics_unref(semantics);
}

// inline needed? is in skeleton-engine
template <class T>
void JuleaKVReader::GetSyncCommon(Variable<T> &variable, T *data)
{

    std::cout << "\n______________GetSync T_____________________" << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank
              << " Reached Get Sync Common (T, T)" << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank
              << " Variable name: " << variable.m_Name << std::endl;
    /* all the additional metadata which is not used in InitVariables has to be
     * read again */
    // FIXME:

    guint32 buffer_len;
    gpointer md_buffer = nullptr;

    auto nameSpace = m_Name;
    long unsigned int dataSize = 0;
    auto stepBlockID =
        g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
    std::cout << "stepBlockID: " << stepBlockID << std::endl;

    // TODO: check if variable.m_StepsStart set correctly!
    variable.SetBlockInfo(data, variable.m_StepsStart, variable.m_StepsCount);

    GetBlockMetadataFromJulea(nameSpace, variable.m_Name, &md_buffer,
                              &buffer_len, stepBlockID);
    std::cout << "buffer_len = " << buffer_len << std::endl;

    DeserializeBlockMetadata(variable, md_buffer, m_CurrentBlockID);
    size_t numberElements =
        helper::GetTotalSize(variable.m_BlocksInfo[m_CurrentBlockID].Count);
    // dataSize = numberElements * variable.m_ElementSize;
    GetVariableDataFromJulea(variable, data, nameSpace, numberElements, m_CurrentStep,
                             m_CurrentBlockID);
    // std::cout <<"data: " <<variable.m_Data[0] << std::endl;
    // data = variable.m_BlocksInfo[m_CurrentBlockID].Data;

    // std::cout << "data: " << data[0] << std::endl;
    // std::cout << "data: " << data[1] << std::endl;
    data = variable.m_Data;
    std::cout << "data: " << data[0] << std::endl;
    std::cout << "data: " << data[1] << std::endl;
}

template <class T>
void JuleaKVReader::GetDeferredCommon(Variable<T> &variable, T *data)
{
    // std::cout << "JULEA ENGINE: GetDeferredCommon" << std::endl;
    // returns immediately
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << "     GetDeferred("
                  << variable.m_Name << ")\n";
    }
    variable.SetBlockInfo(data, variable.m_StepsStart, variable.m_StepsCount);
    // m_NeedPerformGets = true;

    // returns immediately without populating data
    // m_BP4Deserializer.InitVariableBlockInfo(variable, data);  //TODO: needed?
    m_DeferredVariables.insert(variable.m_Name);
}

template <class T>
std::map<size_t, std::vector<typename core::Variable<T>::Info>>
JuleaKVReader::DoAllStepsBlocksInfo(const core::Variable<T> &variable) const
{
    std::cout << "\n______________ DoAllStepsBlocksInfo _____________________"
              << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank
              << " Reached DoAllStepsBlocksInfo" << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " << m_Name
              << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank
              << " Variable name: " << variable.m_Name << std::endl;
    std::map<size_t, std::vector<typename core::Variable<T>::Info>>
        allStepsBlocksInfo;

    for (const auto &pair : variable.m_AvailableStepBlockIndexOffsets)
    {
        const size_t step = pair.first;
        const std::vector<size_t> &blockPositions = pair.second;
        std::cout << "step: " << step << std::endl;
        std::cout << "blockPositions" << &blockPositions << std::endl;
        // bp4 index starts at 1
        // allStepsBlocksInfo[step - 1] =
        //     BlocksInfoCommon(variable, blockPositions);
    }
    return allStepsBlocksInfo;
}

template <class T>
std::vector<std::vector<typename core::Variable<T>::Info>>
JuleaKVReader::DoAllRelativeStepsBlocksInfo(
    const core::Variable<T> &variable) const
{
    std::vector<std::vector<typename core::Variable<T>::Info>>
        allRelativeStepsBlocksInfo(
            variable.m_AvailableStepBlockIndexOffsets.size());

    size_t relativeStep = 0;
    for (const auto &pair : variable.m_AvailableStepBlockIndexOffsets)
    {
        const std::vector<size_t> &blockPositions = pair.second;
        // allRelativeStepsBlocksInfo[relativeStep] =
        // BlocksInfoCommon(variable, blockPositions);
        ++relativeStep;
    }
    return allRelativeStepsBlocksInfo;
}

template <class T>
std::vector<typename core::Variable<T>::Info>
JuleaKVReader::DoBlocksInfo(const core::Variable<T> &variable,
                            const size_t step) const
{
    // bp4 format starts at 1
    auto itStep = variable.m_AvailableStepBlockIndexOffsets.find(step + 1);
    if (itStep == variable.m_AvailableStepBlockIndexOffsets.end())
    {
        return std::vector<typename core::Variable<T>::Info>();
    }
    // return BlocksInfoCommon(variable, itStep->second);
    return NULL;
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif // ADIOS2_ENGINE_JULEAREADER_TCC_
