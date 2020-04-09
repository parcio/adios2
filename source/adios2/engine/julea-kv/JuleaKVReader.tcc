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

    auto nameSpace = m_Name;
    long unsigned int dataSize = 0;

    /* all the additional metadata which is not used in InitVariables has to be
     * read again */
    GetVariableMetadataFromJulea(variable, nameSpace, &dataSize);
    GetVariableDataFromJulea(variable, data, nameSpace, dataSize);
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
    m_NeedPerformGets = true;
}

template <class T>
std::map<size_t, std::vector<typename core::Variable<T>::Info>>
JuleaKVReader::DoAllStepsBlocksInfo(const core::Variable<T> &variable) const
{
    std::map<size_t, std::vector<typename core::Variable<T>::Info>>
        allStepsBlocksInfo;

    for (const auto &pair : variable.m_AvailableStepBlockIndexOffsets)
    {
        const size_t step = pair.first;
        const std::vector<size_t> &blockPositions = pair.second;
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
