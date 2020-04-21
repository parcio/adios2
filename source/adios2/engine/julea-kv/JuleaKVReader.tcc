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
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________GetSync T_____________________"
                  << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank
                  << " Reached Get Sync Common (T, T)" << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " << m_Name
                  << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank
                  << " Variable name: " << variable.m_Name << std::endl;
    }

    if (variable.m_SingleValue)
    {
        // m_BP3Deserializer.GetValueFromMetadata(variable, data);
        // return;
    }

    // guint32 buffer_len;
    // gpointer md_buffer = nullptr;

    // auto nameSpace = m_Name;
    // long unsigned int dataSize = 0;
    // auto stepBlockID =
    //     g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
    // std::cout << "stepBlockID: " << stepBlockID << std::endl;

    // // TODO: check if variable.m_StepsStart set correctly!
    // variable.SetBlockInfo(data, variable.m_StepsStart,
    // variable.m_StepsCount);

    // GetBlockMetadataFromJulea(nameSpace, variable.m_Name, &md_buffer,
    //                           &buffer_len, stepBlockID);
    // std::cout << "buffer_len = " << buffer_len << std::endl;

    // DeserializeBlockMetadata(variable, md_buffer, m_CurrentBlockID);
    // size_t numberElements =
    //     helper::GetTotalSize(variable.m_BlocksInfo[m_CurrentBlockID].Count);
    // dataSize = numberElements * variable.m_ElementSize;
    // GetVariableDataFromJulea(variable, data, nameSpace, dataSize,
    // m_CurrentStep,
    //                          m_CurrentBlockID);

    // std::cout << "data: " << data[0] << std::endl;
    // std::cout << "data: " << data[1] << std::endl;

    // TODO: InitVariableBlockInfo -> store data pointer in BlockInfo
    // typename Variable<T>::Info &blockInfo =
    // InitVariableBlockInfo(variable, data);
    // SetVariableBlockInfo(variable,blockinfo)

    InitVariableBlockInfo(variable, data);
    ReadVariableBlocks(variable);
    variable.m_BlocksInfo.pop_back();
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
    // variable.SetBlockInfo(data, variable.m_StepsStart,
    // variable.m_StepsCount); m_NeedPerformGets = true;

    // returns immediately without populating data
    InitVariableBlockInfo(variable, data); // TODO: needed?
    m_DeferredVariables.insert(variable.m_Name);
}

template <class T>
void JuleaKVReader::ReadVariableBlocks(Variable<T> &variable)
{

    // std::cout << "\n__________ReadVariableBlocks_____________" << std::endl;

    // std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " << m_Name
    // << std::endl;
    // std::cout << "Julea Reader " << m_ReaderRank
    // << " Variable name: " << variable.m_Name << std::endl;

    for (typename Variable<T>::Info &blockInfo : variable.m_BlocksInfo)
    {
        guint32 buffer_len;
        gpointer md_buffer = nullptr;

        auto nameSpace = m_Name;
        long unsigned int dataSize = 0;
    std::cout << "m_CurrentBlockID: " << m_CurrentBlockID << std::endl;

        /** when called from bpls there is no endStep or anything */
        if(variable.m_BlockID == 0)
        {
            // m_CurrentBlockID = 0;
            // m_CurrentStep ++;
            // m_CurrentStep = variable.m_StepsStart;
        }
    std::cout << "variable.m_BlockID: " << variable.m_BlockID << std::endl;
    std::cout << "variable.m_StepsStart: " << variable.m_StepsStart << std::endl;
    std::cout << "variable.m_StepsCount " << variable.m_StepsCount << std::endl;
    std::cout << "variable.m_AvailableStepsStart " << variable.m_AvailableStepsStart << std::endl;
    std::cout << "variable.m_AvailableStepsCount " << variable.m_AvailableStepsCount << std::endl;
    std::cout << "m_CurrentBlockID: " << m_CurrentBlockID << std::endl;
    std::cout << "m_CurrentStep: " << m_CurrentStep << std::endl;

        auto stepBlockID =
            g_strdup_printf("%lu_%lu", variable.m_StepsStart, variable.m_BlockID);

        std::cout << "variable.m... stepBlockID: " << stepBlockID << std::endl;
        stepBlockID = g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
        std::cout << "m_Current... stepBlockID: " << stepBlockID << std::endl;
        // std::cout << "blocksInfos.size: " << variable.m_BlocksInfo.size()
                  // << std::endl;

        // // TODO: check if variable.m_StepsStart set correctly!
        // variable.SetBlockInfo(data, variable.m_StepsStart,
        // variable.m_StepsCount);

        GetBlockMetadataFromJulea(nameSpace, variable.m_Name, &md_buffer,
                                  &buffer_len, stepBlockID);
        // std::cout << "buffer_len = " << buffer_len << std::endl;

        // DeserializeBlockMetadata(variable, md_buffer, variable.m_BlockID, blockInfo);
        DeserializeBlockMetadata(variable, md_buffer, variable.m_BlockID, blockInfo);

        if (variable.m_SingleValue)
        {
            std::cout << "Single value" << std::endl;
            return;
        }

        // std::cout << " isConstantDims: " << variable.IsConstantDims()
                  // << std::endl;

        // size_t count = blockInfo.Count;
        size_t numberElements =
            // helper::GetTotalSize(variable.m_BlocksInfo[m_CurrentBlockID].Count);
            helper::GetTotalSize(blockInfo.Count);
        dataSize = numberElements * variable.m_ElementSize;
        // std::cout << "numberElements: " << numberElements << std::endl;

        T *data = blockInfo.Data;
        // T *data = variable.m_BlocksInfo[m_CurrentBlockID].Data;
        GetVariableDataFromJulea(variable, data, nameSpace, dataSize,
                                 // variable.m_StepsStart, variable.m_BlockID);
                                 m_CurrentStep, m_CurrentBlockID);
        std::cout << "data: " << data[0] << std::endl;
        std::cout << "data: " << data[1] << std::endl;
        m_CurrentBlockID++;
    }
    // m_CurrentBlockID = 0;
}

template <class T>
std::map<size_t, std::vector<typename core::Variable<T>::Info>>
JuleaKVReader::AllStepsBlocksInfo(const core::Variable<T> &variable) const
{
    std::map<size_t, std::vector<typename core::Variable<T>::Info>>
        allStepsBlocksInfo;

    for (const auto &pair : variable.m_AvailableStepBlockIndexOffsets)
    {
        const size_t step = pair.first;
        const std::vector<size_t> &blockPositions = pair.second;
        // std::cout << "--- step: " << step
        // << " blockPositions: " << blockPositions.data()[0] << std::endl;
        // std::cout << "--- step: " << step
        // << " blockPositions: " << blockPositions.size() << std::endl;

        for (int i = 0; i < blockPositions.size(); i++)
        {
            // allStepsBlocksInfo[step -1 ] = variable.m_BlocksInfo[i];
            // allStepsBlocksInfo[step -1 ].push_back(variable.m_BlocksInfo[i]);
            // std::cout << "i: " << i << std::endl;
        }
        // bp3 index starts at 1
        allStepsBlocksInfo[step - 1] =
            BlocksInfoCommon(variable, blockPositions, step - 1);
    }
    // std::cout << "--- finished allStepsBlocksInfo --- " << std::endl;
    return allStepsBlocksInfo;
}

template <class T>
std::vector<typename core::Variable<T>::Info>
JuleaKVReader::BlocksInfoCommon(const core::Variable<T> &variable,
                                const std::vector<size_t> &blocksIndexOffsets,
                                size_t step) const
{
    // std::cout << "____ BlocksInfoCommon _____ step: " << step << std::endl;
    std::vector<typename core::Variable<T>::Info> blocksInfo;
    blocksInfo.reserve(blocksIndexOffsets.size());
    typename core::Variable<T>::Info blockInfo;

    // for (const size_t blockIndexOffset : blocksIndexOffsets)
    // {
    //     size_t position = blockIndexOffset;
    //     std::cout << "position: " << position << std::endl;
    //             blocksInfo.push_back(variable.m_BlocksInfo[0]);

    // }
    for (size_t i = 0; i < blocksIndexOffsets.size(); i++)
    {
        guint32 buffer_len;
        gpointer md_buffer = nullptr;

        auto nameSpace = m_Name;
        long unsigned int dataSize = 0;
        auto stepBlockID = g_strdup_printf("%lu_%lu", step, i);
        // std::cout << "---- blocksIndexOffsets.size(): " <<
        // blocksIndexOffsets.size()
        // << std::endl;
        // std::cout << "---- stepBlockID: " << stepBlockID << std::endl;

        // // TODO: check if variable.m_StepsStart set correctly!
        // variable.SetBlockInfo(data, variable.m_StepsStart,
        // variable.m_StepsCount);
        size_t test = i;
        GetBlockMetadataFromJulea(nameSpace, variable.m_Name, &md_buffer,
                                  &buffer_len, stepBlockID);
        // std::cout << "buffer_len = " << buffer_len << std::endl;
        typename core::Variable<T>::Info info =
            *GetDeserializedMetadata(variable, md_buffer);
        info.IsReverseDims = false;
        info.Step = step;
        // std::cout << "--- DEBUG --- " << std::endl;

        blocksInfo.push_back(info);
        // std::cout << "BlocksInfoCommon - blocksInfo.size(): " << blocksInfo.size() << std::endl;
    }
    // return variable.m_BlocksInfo[0];
    return blocksInfo;
}

template <class T>
std::vector<std::vector<typename core::Variable<T>::Info>>
JuleaKVReader::DoAllRelativeStepsBlocksInfo(
    const core::Variable<T> &variable) const
{
    // std::cout << "--- DoAllRelativeStepsBlocksInfo --- " << std::endl;
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
JuleaKVReader::BlocksInfo(const core::Variable<T> &variable,
                          const size_t step) const
{

    // std::cout << "--- BlocksInfo --- " << std::endl;

    // std::cout << "variable.m_AvailableStepBlockIndexOffsets.size() " << variable.m_AvailableStepBlockIndexOffsets.size() <<std::endl;
    // std::cout << "variable.m_AvailableStepBlockIndexOffsets.find(step + 1) " << variable.m_AvailableStepBlockIndexOffsets.find(step + 1) <<std::endl;
    // std::cout << "BlocksInfo: step = " << step <<std::endl;
    // bp4 format starts at 1
    // auto itStep = variable.m_AvailableStepBlockIndexOffsets.find(step + 1);
    auto itStep = variable.m_AvailableStepBlockIndexOffsets.find(step+1);
    if (itStep == variable.m_AvailableStepBlockIndexOffsets.end())
    {
        return std::vector<typename core::Variable<T>::Info>();
        std::cout << " --- step not found in m_AvailableStepBlockIndexOffsets " << std::endl;
    }
    return BlocksInfoCommon(variable, itStep->second, step);
    // return NULL;
}

template <class T>
typename core::Variable<T>::Info &
JuleaKVReader::InitVariableBlockInfo(core::Variable<T> &variable, T *data)
{
    const size_t stepsStart = variable.m_StepsStart;
    const size_t stepsCount = variable.m_StepsCount;
    // std::cout << "--- InitVariableBlockInfo --- selectionType: "
              // << variable.m_SelectionType << std::endl;

    // if (m_DebugMode)
    if (m_Verbosity == 5)
    {
        const auto &indices = variable.m_AvailableStepBlockIndexOffsets;
        const size_t maxStep = indices.rbegin()->first;
        if (stepsStart + 1 > maxStep)
        {
            throw std::invalid_argument(
                "ERROR: steps start " + std::to_string(stepsStart) +
                " from SetStepsSelection or BeginStep is larger than "
                "the maximum available step " +
                std::to_string(maxStep - 1) + " for variable " +
                variable.m_Name + ", in call to Get\n");
        }

        auto itStep = std::next(indices.begin(), stepsStart);

        for (size_t i = 0; i < stepsCount; ++i)
        {
            if (itStep == indices.end())
            {
                throw std::invalid_argument(
                    "ERROR: offset " + std::to_string(i) +
                    " from steps start " + std::to_string(stepsStart) +
                    " in variable " + variable.m_Name +
                    " is beyond the largest available step = " +
                    std::to_string(maxStep - 1) +
                    ", check Variable SetStepSelection argument stepsCount "
                    "(random access), or "
                    "number of BeginStep calls (streaming), in call to Get");
            }
            ++itStep;
        }
    }

    if (variable.m_SelectionType == SelectionType::WriteBlock)
    {
        // std::cout << "--- DEBUG: Selection Type " << std::endl;
        const std::vector<typename core::Variable<T>::Info> blocksInfo =
            JuleaKVReader::BlocksInfo(variable, stepsStart);

        if (variable.m_BlockID >= blocksInfo.size())
        {
            throw std::invalid_argument(
                "ERROR: invalid blockID " + std::to_string(variable.m_BlockID) +
                " from steps start " + std::to_string(stepsStart) +
                " in variable " + variable.m_Name +
                ", check argument to Variable<T>::SetBlockID, in call "
                "to Get\n");
        }

        // switch to bounding box for global array
        if (variable.m_ShapeID == ShapeID::GlobalArray)
        {
            // std::cout
                // << "----------- DEBUG: switch to bounding box for global array "
                // << std::endl;
            const Dims &start = blocksInfo[variable.m_BlockID].Start;
            const Dims &count = blocksInfo[variable.m_BlockID].Count;
            // std::cout << "variable.m_BlockID " << variable.m_BlockID
                      // << std::endl;

            variable.SetSelection({start, count});
        }
        else if (variable.m_ShapeID == ShapeID::LocalArray)
        {
            // std::cout
                // << "----------- DEBUG: switch to bounding box for local array "
                // << std::endl;
            // std::cout << "variable.m_BlockID " << variable.m_BlockID
                      // << std::endl;

            // TODO keep Count for block updated
            variable.m_Count = blocksInfo[variable.m_BlockID].Count;
        }
    }
    // std::cout << "stepsstart: " << stepsStart << std::endl;
    // std::cout << "stepsCount: " << stepsCount << std::endl;
    // create block info
    // FIXME: only create once for every block!
    return variable.SetBlockInfo(data, stepsStart, stepsCount);
}

// template <class T>
// std::map<size_t, std::vector<typename core::Variable<T>::Info>>
// JuleaKVReader::DoAllStepsBlocksInfo(const core::Variable<T> &variable) const
// {
//     std::cout << "\n______________ DoAllStepsBlocksInfo
//     _____________________"
//               << std::endl;
//     std::cout << "Julea Reader " << m_ReaderRank
//               << " Reached DoAllStepsBlocksInfo" << std::endl;
//     std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " << m_Name
//               << std::endl;
//     std::cout << "Julea Reader " << m_ReaderRank
//               << " Variable name: " << variable.m_Name << std::endl;
//     std::map<size_t, std::vector<typename core::Variable<T>::Info>>
//         allStepsBlocksInfo;

//     for (const auto &pair : variable.m_AvailableStepBlockIndexOffsets)
//     {
//         const size_t step = pair.first;
//         const std::vector<size_t> &blockPositions = pair.second;
//         std::cout << "step: " << step << std::endl;
//         std::cout << "blockPositions" << &blockPositions << std::endl;
//         // bp4 index starts at 1
//         allStepsBlocksInfo[step - 1] =
//             BlocksInfoCommon(variable, blockPositions);
//     }
//     return allStepsBlocksInfo;
// }

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif // ADIOS2_ENGINE_JULEAREADER_TCC_
