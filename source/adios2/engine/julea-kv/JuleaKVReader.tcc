/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JuleaKVReader_TCC_
#define ADIOS2_ENGINE_JuleaKVReader_TCC_

#include "JuleaKVReader.h"

#include <iostream>
// #include <fstream>
// #include <string>

namespace adios2
{
namespace core
{
namespace engine
{

// TODO: check whether this is sufficient for strings!
// is separate data stored which needs to be read or just metadata?
template <>
void JuleaKVReader::GetSyncCommon(Variable<std::string> &variable,
                                  std::string *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________GetSync String_____________________"
                  << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank << " Namespace: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }

    for (typename Variable<std::string>::Info &blockInfo :
         variable.m_BlocksInfo)
    {
        long unsigned int dataSize = 0;
        guint32 buffer_len = 0;
        std::string fileName = m_Name;
        std::string stepBlockID;
        gpointer md_buffer = nullptr;

        if (m_UseKeysForBPLS)
        {
            stepBlockID = g_strdup_printf("%lu_%lu", variable.m_StepsStart,
                                          variable.m_BlockID);
            // std::cout << "variable.m... stepBlockID: " << stepBlockID <<
            // std::endl;
        }
        else
        {
            stepBlockID =
                g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
            // std::cout << "m_Current... stepBlockID: " << stepBlockID <<
            // std::endl;
        }
        // std::cout << "blocksInfos.size: " << variable.m_BlocksInfo.size()
        // << std::endl;

        // GetBlockMetadataFromJulea(fileName, variable.m_Name, &md_buffer,
        // &buffer_len, stepBlockID);
        // DeserializeBlockMetadata(variable, md_buffer, variable.m_BlockID,
        // blockInfo);

        if (variable.m_SingleValue)
        {
            std::cout << "Single value" << std::endl;
            return;
        }
        m_CurrentBlockID++;
    }
}

template <class T>
void JuleaKVReader::GetSyncCommon(Variable<T> &variable, T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________GetSync T_____________________"
                  << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank
                  << " Reached Get Sync Common (T, T)" << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " << m_Name
                  << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank
                  << " Variable name: " << variable.m_Name << std::endl;
    }

    if (variable.m_SingleValue)
    {
        std::cout << "FIXME: read single value" << std::endl;
        // FIXME: just read metadata from DB. value is stored in there.
        // m_BP3Deserializer.GetValueFromMetadata(variable, data);
        // return;
    }

    typename Variable<T>::Info &blockInfo =
        InitVariableBlockInfo(variable, data);

    SetVariableBlockInfo(variable, blockInfo);

    if (variable.m_ShapeID == ShapeID::LocalArray)
    {
        ReadBlock(variable, data, variable.m_BlockID);
    }
    else if (variable.m_ShapeID == ShapeID::GlobalArray)
    {
        ReadVariableBlocks(variable);
    }

    variable.m_BlocksInfo.pop_back();
}

template <class T>
void JuleaKVReader::GetDeferredCommon(Variable<T> &variable, T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank << "     GetDeferred("
                  << variable.m_Name << ")\n";
    }

    // store data pointer in related blockinfo to be called in perform gets
    // InitVariableBlockInfo(variable, data);

    typename Variable<T>::Info &blockInfo =
        InitVariableBlockInfo(variable, data);

    SetVariableBlockInfo(variable, blockInfo);

    size_t size = variable.m_BlocksInfo.size();

    // FIXME:
    if (variable.m_ShapeID == ShapeID::LocalArray)
    {
        variable.m_BlocksInfo[size - 1].BlockID = variable.m_BlockID;
    }
    else if (variable.m_ShapeID == ShapeID::GlobalArray)
    {
        // variable.m_BlocksInfo[size-1].BlockID =
        // blockInfo.StepBlockSubStreamsInfo[m_CurrentStep].SubStreamID;
        // variable.m_BlocksInfo[size - 1].BlockID =
        //     blockInfo.StepBlockSubStreamsInfo[m_CurrentStep][m_ReaderRank]
        //         .SubStreamID;
    }

    m_DeferredVariables.insert(variable.m_Name);
}

template <class T>
void JuleaKVReader::ReadBlock(Variable<T> &variable, T *data, size_t blockID)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n ReadBlock" << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }
    size_t step = 0;
    std::string stepBlockID;
    Dims count;
    std::string fileName = m_Name;

    long unsigned int dataSize = 0;
    guint32 buffer_len = 0;
    gpointer md_buffer = nullptr;

    // std::cout << "unique entry ID: " <<
    // variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep] << std::endl;
    if (m_UseKeysForBPLS)
    {
        stepBlockID =
            g_strdup_printf("%lu_%lu", variable.m_StepsStart, blockID);
        step = variable.m_StepsStart;
        // std::cout << "variable.m... stepBlockID: " << stepBlockID <<
        // std::endl;
    }
    else
    {
        stepBlockID = g_strdup_printf("%lu_%lu", m_CurrentStep, blockID);
        step = m_CurrentStep;
        // std::cout << "variable.m... stepBlockID: " << stepBlockID <<
        // std::endl;
    }

    /** only retrieve Count. Everything is only needed for bp3 and bp4 to
     * determine block position in buffer and for AllStepsBlockInfo for bpls */
    auto entryID = variable.m_AvailableStepBlockIndexOffsets[step + 1][blockID];
    // auto entryID = variable.m_AvailableStepBlockIndexOffsets[step][blockID];
    m_JuleaKVInteractionReader.GetCountFromBlockMetadata(
        m_ProjectNamespace, fileName, variable.m_Name, step, blockID, &count,
        entryID, variable.m_SingleValue, data);
    if (variable.m_SingleValue)
    {
        // FIXME: get Value from DB
        std::cout << "Single value" << std::endl;
        return;
    }

    size_t numberElements = helper::GetTotalSize(count);
    dataSize = numberElements * variable.m_ElementSize;
    size_t offset = 0;

    // FIXME:
    // DBGetVariableDataFromJulea(variable, data, fileName, offset, dataSize,
    // stepBlockID);
}

template <class T>
void JuleaKVReader::ReadBlockMD(typename core::Variable<T>::Info &blockInfo,
                                size_t blockID)
{
    // if (m_Verbosity == 5)
    // {
    //     std::cout << "\n ReadBlock" << std::endl;
    //     std::cout << "Julea Reader " << m_ReaderRank << " Namespace: " <<
    //     m_Name
    //               << " Variable name: " << variable.m_Name << std::endl;
    // }
    // size_t step = 0;
    // std::string stepBlockID;
    // Dims count;
    // std::string nameSpace = m_Name;

    // long unsigned int dataSize = 0;
    // guint32 buffer_len = 0;
    // gpointer md_buffer = nullptr;

    // //TODO: needed here?
    // if (m_UseKeysForBPLS)
    // {
    //     // stepBlockID =
    //     //     g_strdup_printf("%lu_%lu", variable.m_StepsStart, blockID);
    //     // step = variable.m_StepsStart;
    //     // std::cout << "variable.m... stepBlockID: " << stepBlockID <<
    //     // std::endl;
    // }
    // else
    // {
    //     // stepBlockID = g_strdup_printf("%lu_%lu", m_CurrentStep, blockID);
    //     // step = m_CurrentStep;
    //     // std::cout << "variable.m... stepBlockID: " << stepBlockID <<
    //     // std::endl;
    // }

    // /** only retrieve Count. Everything is only needed for bp3 and bp4 to
    //  * determine block position in buffer and for AllStepsBlockInfo for bpls
    //  */
    // auto entryID = variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep +
    // 1][blockID]; GetCountFromBlockMetadata(nameSpace, variable.m_Name,
    // m_CurrentStep, blockID, &count,
    //                           entryID, variable.m_SingleValue, data);
}

template <class T>
void JuleaKVReader::ReadVariableBlocks(Variable<T> &variable)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________ReadVariableBlocks_____________" << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank << " File: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }

    for (typename Variable<T>::Info &blockInfo : variable.m_BlocksInfo)
    {
        // std::cout << "blocksInfos.size: " << variable.m_BlocksInfo.size()
        // << std::endl;
        // long unsigned int dataSize = 0;
        // guint32 buffer_len = 0;
        // std::string nameSpace = m_Name;
        // std::string stepBlockID;
        // gpointer md_buffer = nullptr;
        // size_t block = 0;
        // size_t step = 0;

        // if (m_UseKeysForBPLS)
        // {
        //     stepBlockID = g_strdup_printf("%lu_%lu", variable.m_StepsStart,
        //                                   variable.m_BlockID);
        //     step = variable.m_StepsStart;
        //     block = variable.m_BlockID;
        //     std::cout << "variable.m... stepBlockID: " << stepBlockID <<
        //     std::endl;
        // }
        // else
        // {
        //     stepBlockID =
        //         g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
        //         step = m_CurrentStep;
        //         block = m_CurrentBlockID;
        //     std::cout << "m_Current... stepBlockID: " << stepBlockID <<
        //     std::endl;
        // }

        // // GetBlockMetadataFromJulea(nameSpace, variable.m_Name, &md_buffer,
        // // &buffer_len, stepBlockID);
        // // DeserializeBlockMetadata(variable, md_buffer, variable.m_BlockID,
        // // blockInfo);
        // DBGetBlockMetadata(variable,nameSpace,step, block, blockInfo);
        // // variable.m_BlocksInfo.push_back(blockInfo);
        // variable.m_BlocksInfo[block] = blockInfo;
        // // variable.m_BlocksInfo[0] = blockInfo;

        // if (variable.m_SingleValue)
        // {
        //     std::cout << "Single value" << std::endl;
        //     return;
        // }

        // size_t numberElements = helper::GetTotalSize(blockInfo.Count);
        // dataSize = numberElements * variable.m_ElementSize;

        // T *data = blockInfo.Data;
        // if (m_UseKeysForBPLS)
        // {
        //     DBGetVariableDataFromJulea(variable, data, nameSpace, dataSize,
        //     variable.m_StepsStart, variable.m_BlockID);
        // }
        // else
        // {
        //     DBGetVariableDataFromJulea(variable, data, nameSpace, dataSize,
        //     m_CurrentStep, m_CurrentBlockID);
        // }
        // m_CurrentBlockID++;
        for (typename Variable<T>::Info &blockInfo : variable.m_BlocksInfo)
        {
            T *originalBlockData = blockInfo.Data;
            // std::cout << "blockInfo.StepBlockSubStreamsInfo.Size() = " << blockInfo.StepBlockSubStreamsInfo.size() << "\n";
            for (const auto &stepPair : blockInfo.StepBlockSubStreamsInfo)
            {
            // std::cout << "stepPair.second.Size() = " << stepPair.second.size() << "\n";
                for (const helper::SubStreamBoxInfo &subStreamBoxInfo :
                     stepPair.second)
                {
                    if (subStreamBoxInfo.ZeroBlock)
                    {
                        continue;
                    }

                    // char *buffer = nullptr;
                    std::string stepBlockID;
                    std::string fileName = m_Name;
                    long unsigned int dataSize = 0;
                    // TODO: -1 because BP stuff starts at 1

                    size_t offset = subStreamBoxInfo.Seeks.first;
                    // size_t step = stepPair.first - 1;
                    size_t step = stepPair.first;

                    // std::cout << "stepPair.first: " << stepPair.first
                    // << std::endl;

                    // std::cout
                    // << "unique entry ID: " << subStreamBoxInfo.SubStreamID
                    // << std::endl;

                    // std::string entryID = subStreamBoxInfo.SubStreamID;

                    // FIXME: implement
                    // stepBlockID = g_strdup_printf("%lu_%lu", step,
                    // subStreamBoxInfo.SubStreamID);
                    // std::cout << "stepBlockID: " << stepBlockID << std::endl;

                    // variable.m_AvailableStepBlockIndexOffsets

                    // FIXME: 18.10.2021
                    // is there still a difference whether data is read by bpls
                    // or a "normal" read call?
                    // there was a difference in how some ID was increased. No
                    // idea whether this is still the case
                    if (m_UseKeysForBPLS)
                    {
                        // std::cout << "--- m_UseKeysForBPLS ---" << std::endl;
                        // DBGetVariableDataFromJulea(
                        //     variable, data, fileName, dataSize,
                        //     variable.m_StepsStart, variable.m_BlockID);

                        // TODO: the following is just the else copied
                        dataSize = subStreamBoxInfo.Seeks.second -
                                   subStreamBoxInfo.Seeks.first;
                        // std::cout << "dataSize: " << dataSize << std::endl;

                        // T data[dataSize];
                        std::vector<T> data = std::vector<T>(dataSize);
                        m_JuleaKVInteractionReader.GetVariableDataFromJulea(
                            variable, data.data(), m_ProjectNamespace, fileName,
                            offset, dataSize, step, m_CurrentBlockID, true);

                        // offset, dataSize, subStreamBoxInfo.SubStreamID);

                        const Dims blockInfoStart =
                            (variable.m_ShapeID == ShapeID::LocalArray &&
                             blockInfo.Start.empty())
                                ? Dims(blockInfo.Count.size(), 0)
                                : blockInfo.Start;

                        helper::ClipContiguousMemory(
                            blockInfo.Data, blockInfoStart, blockInfo.Count,
                            (char *)data.data(), subStreamBoxInfo.BlockBox,
                            subStreamBoxInfo.IntersectionBox, true, false,
                            false);
                    }
                    else
                    {
                        // size_t numberElements = helper::GetTotalSize(count);
                        // dataSize = numberElements * variable.m_ElementSize;
                        dataSize = subStreamBoxInfo.Seeks.second -
                                   subStreamBoxInfo.Seeks.first;
                        // std::cout << "dataSize: " << dataSize << std::endl;

                        // T data[dataSize];
                        // true = is key value backend
                        std::vector<T> data = std::vector<T>(dataSize);
                        m_JuleaKVInteractionReader.GetVariableDataFromJulea(
                            variable, data.data(), m_ProjectNamespace, fileName,
                            offset, dataSize, step, subStreamBoxInfo.SubStreamID, true);
                            // offset, dataSize, step, m_CurrentBlockID, true);
                        // offset, dataSize, subStreamBoxInfo.SubStreamID);

                        const Dims blockInfoStart =
                            (variable.m_ShapeID == ShapeID::LocalArray &&
                             blockInfo.Start.empty())
                                ? Dims(blockInfo.Count.size(), 0)
                                : blockInfo.Start;

                        helper::ClipContiguousMemory(
                            blockInfo.Data, blockInfoStart, blockInfo.Count,
                            (char *)data.data(), subStreamBoxInfo.BlockBox,
                            subStreamBoxInfo.IntersectionBox, true, false,
                            false);

                        // helper::ClipVector(m_ThreadBuffers[threadID][0],
                        //                    subStreamBoxInfo.Seeks.first,
                        //                    subStreamBoxInfo.Seeks.second);
                    }
                    m_CurrentBlockID++;

                } // substreams loop
                // advance pointer to next step
                blockInfo.Data += helper::GetTotalSize(blockInfo.Count);
            } // steps loop
            blockInfo.Data = originalBlockData;
        } // deferred blocks loop
    }
}

template <class T>
std::map<size_t, std::vector<typename core::Variable<T>::Info>>
JuleaKVReader::AllStepsBlocksInfo(const core::Variable<T> &variable) const
{
    std::map<size_t, std::vector<typename core::Variable<T>::Info>>
        allStepsBlocksInfo;

    // Explanation for this ugly assumption is in the header file. This assumes
    // that only bpls calls AllStepsBlocksInfo. for now that should be ok now
    // = 21.04.2020
    m_UseKeysForBPLS = true;
    // SetUseKeysForBPLS(true);
    for (const auto &pair : variable.m_AvailableStepBlockIndexOffsets)
    {
        const size_t step = pair.first;
        const std::vector<size_t> &blockPositions = pair.second;

        if (m_Verbosity == 5)
        {
            std::cout << "\n__________AllStepsBlocksInfo_____________"
                      << std::endl;
            std::cout << "JKV Reader " << m_ReaderRank << " File: " << m_Name
                      << " Variable name: " << variable.m_Name << std::endl;
            std::cout << "--- step: " << step
                      << " blockPositions: " << blockPositions.data()[0]
                      << std::endl;
        }

        // bp3 index starts at 1
        // allStepsBlocksInfo[step - 1] =
        // BlocksInfoCommon(variable, blockPositions, step - 1);
        allStepsBlocksInfo[step] =
            BlocksInfoCommon(variable, blockPositions, step);
    }
    return allStepsBlocksInfo;
}

template <class T>
std::vector<typename core::Variable<T>::Info>
JuleaKVReader::BlocksInfoCommon(const core::Variable<T> &variable,
                                const std::vector<size_t> &blocksIndexOffsets,
                                size_t step) const
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________BlocksInfoCommon_____________" << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank << " File: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
        std::cout << "--- step: " << step << std::endl;
        std::cout << "blocksIndexOffsets.size(): " << blocksIndexOffsets.size()
                  << std::endl;
    }
    std::vector<typename core::Variable<T>::Info> blocksInfo;
    blocksInfo.reserve(blocksIndexOffsets.size());
    typename core::Variable<T>::Info blockInfo;

    for (size_t i = 0; i < blocksIndexOffsets.size(); i++)
    {
        guint32 buffer_len = 0;
        gpointer md_buffer = nullptr;

        auto fileName = m_Name;
        long unsigned int dataSize = 0;
        auto stepBlockID = g_strdup_printf("%lu_%lu", step, i);
        auto entryID = blocksIndexOffsets[i];

        typename core::Variable<T>::Info info =
            // *DBGetBlockMetadata(variable, fileName, step, i, entryID);
            *m_JuleaKVInteractionReader.GetBlockMetadata(
                variable, m_ProjectNamespace, m_Name, step, entryID);
        info.IsReverseDims = false;
        info.Step = step;

        blocksInfo.push_back(info);
    }
    return blocksInfo;
}

// FIXME: not yet tested!
template <class T>
std::vector<std::vector<typename core::Variable<T>::Info>>
JuleaKVReader::AllRelativeStepsBlocksInfo(
    const core::Variable<T> &variable) const
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________AllRelativeStepsBlocksInfo_____________"
                  << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank << " File: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }
    std::vector<std::vector<typename core::Variable<T>::Info>>
        allRelativeStepsBlocksInfo(
            variable.m_AvailableStepBlockIndexOffsets.size());

    size_t relativeStep = 0;
    for (const auto &pair : variable.m_AvailableStepBlockIndexOffsets)
    {
        const std::vector<size_t> &blockPositions = pair.second;
        allRelativeStepsBlocksInfo[relativeStep] =
            BlocksInfoCommon(variable, blockPositions, relativeStep);
        ++relativeStep;
    }
    return allRelativeStepsBlocksInfo;
}

template <class T>
std::vector<typename core::Variable<T>::Info>
JuleaKVReader::BlocksInfo(const core::Variable<T> &variable,
                          const size_t step) const
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________BlocksInfo_____________" << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank << " File: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }

    // bp4 format starts at 1
    auto itStep = variable.m_AvailableStepBlockIndexOffsets.find(step + 1);
    // auto itStep = variable.m_AvailableStepBlockIndexOffsets.find(step);
    if (itStep == variable.m_AvailableStepBlockIndexOffsets.end())
    {
        return std::vector<typename core::Variable<T>::Info>();
        std::cout << " --- step not found in m_AvailableStepBlockIndexOffsets "
                  << std::endl;
    }
    return BlocksInfoCommon(variable, itStep->second, step);
}

template <class T>
typename core::Variable<T>::Info &
JuleaKVReader::InitVariableBlockInfo(core::Variable<T> &variable, T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________InitVariableBlockInfo_____________"
                  << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank << " File: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }
    const size_t stepsStart = variable.m_StepsStart;
    const size_t stepsCount = variable.m_StepsCount;

    // std::cout << "stepsStart: " << stepsStart << std::endl;
    // std::cout << "stepsCount: " << stepsCount << std::endl;

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
            const Dims &start = blocksInfo[variable.m_BlockID].Start;
            const Dims &count = blocksInfo[variable.m_BlockID].Count;

            variable.SetSelection({start, count});
        }
        else if (variable.m_ShapeID == ShapeID::LocalArray)
        {
            // TODO from Adios people! "keep Count for block updated"
            variable.m_Count = blocksInfo[variable.m_BlockID].Count;
        }
    }

    return variable.SetBlockInfo(data, stepsStart, stepsCount);
}

template <class T>
void JuleaKVReader::SetVariableBlockInfo(
    core::Variable<T> &variable,
    typename core::Variable<T>::Info &blockInfo) const
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________SetVariableBlockInfo_____________________"
                  << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank
                  << " Reached SetVariableBlockInfo" << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank << " File: " << m_Name
                  << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank
                  << " Variable name: " << variable.m_Name << std::endl;
    }
    // auto lf_SetSubStreamInfoLocalArray =
    //     [&](const std::string &variableName, const Box<Dims> &selectionBox,
    //         typename core::Variable<T>::Info &blockInfo, const size_t step,
    //         const size_t blockIndexOffset, const BufferSTL &bufferSTL,
    //         const bool isRowMajor)

    // {
    //     const std::vector<char> &buffer = bufferSTL.m_Buffer;

    //     size_t position = blockIndexOffset;

    //     const Characteristics<T> blockCharacteristics =
    //         ReadElementIndexCharacteristics<T>(buffer, position,
    //                                            TypeTraits<T>::type_enum,
    //                                            false,
    //                                            m_Minifooter.IsLittleEndian);
    //     // check if they intersect
    //     helper::SubStreamBoxInfo subStreamInfo;

    //     if (helper::GetTotalSize(blockCharacteristics.Count) == 0)
    //     {
    //         subStreamInfo.ZeroBlock = true;
    //     }

    //     // if selection box start is not empty = local selection
    //     subStreamInfo.BlockBox =
    //         helper::StartEndBox(Dims(blockCharacteristics.Count.size(), 0),
    //                             blockCharacteristics.Count);

    //     if (!selectionBox.first.empty()) // selection start count was defined
    //     {
    //         subStreamInfo.IntersectionBox =
    //             helper::IntersectionBox(selectionBox,
    //             subStreamInfo.BlockBox);
    //     }
    //     else // read the entire block
    //     {
    //         subStreamInfo.IntersectionBox = subStreamInfo.BlockBox;
    //     }

    //     if (subStreamInfo.IntersectionBox.first.empty() ||
    //         subStreamInfo.IntersectionBox.second.empty())
    //     {
    //         return;
    //     }

    //     const size_t dimensions = blockCharacteristics.Count.size();
    //     if (dimensions != blockInfo.Count.size())
    //     {
    //         throw std::invalid_argument(
    //             "ERROR: block Count (available) and "
    //             "selection Count (requested) number of dimensions, do not "
    //             "match "
    //             "when reading local array variable " +
    //             variableName + ", in call to Get");
    //     }

    //     const Dims readInCount = m_ReverseDimensions
    //                                  ?
    //                                  Dims(blockCharacteristics.Count.rbegin(),
    //                                         blockCharacteristics.Count.rend())
    //                                  : blockCharacteristics.Count;

    //     const Dims blockInfoStart = blockInfo.Start.empty()
    //                                     ? Dims(blockInfo.Count.size(), 0)
    //                                     : blockInfo.Start;

    //     for (size_t i = 0; i < dimensions; ++i)
    //     {
    //         if (blockInfoStart[i] + blockInfo.Count[i] > readInCount[i])
    //         {
    //             throw std::invalid_argument(
    //                 "ERROR: selection Start " +
    //                 helper::DimsToString(blockInfoStart) + " and Count " +
    //                 helper::DimsToString(blockInfo.Count) +
    //                 " (requested) is out of bounds of (available) local"
    //                 " Count " +
    //                 helper::DimsToString(readInCount) +
    //                 " , when reading local array variable " + variableName +
    //                 ", in call to Get");
    //         }
    //     }

    //     subStreamInfo.Seeks.first =
    //         sizeof(T) * helper::LinearIndex(subStreamInfo.BlockBox,
    //                                         subStreamInfo.IntersectionBox.first,
    //                                         isRowMajor);

    //     subStreamInfo.Seeks.second =
    //         sizeof(T) * (helper::LinearIndex(
    //                          subStreamInfo.BlockBox,
    //                          subStreamInfo.IntersectionBox.second,
    //                          isRowMajor) +
    //                      1);

    //     const size_t payloadOffset =
    //         blockCharacteristics.Statistics.PayloadOffset;

    //     const BPOpInfo &bpOp = blockCharacteristics.Statistics.Op;
    //     // if they intersect get info Seeks (first: start, second:
    //     // count) depending on operation info
    //     if (bpOp.IsActive)
    //     {
    //         lf_SetSubStreamInfoOperations(bpOp, payloadOffset, subStreamInfo,
    //                                       m_IsRowMajor);
    //     }
    //     else
    //     {
    //         // make it absolute if no operations
    //         subStreamInfo.Seeks.first += payloadOffset;
    //         subStreamInfo.Seeks.second += payloadOffset;
    //     }
    //     subStreamInfo.SubStreamID =
    //         static_cast<size_t>(blockCharacteristics.Statistics.FileIndex);

    //     blockInfo.StepBlockSubStreamsInfo[step].push_back(
    //         std::move(subStreamInfo));
    // };

    auto lf_SetSubStreamInfoGlobalArray =
        [&](const std::string &variableName, const Box<Dims> &selectionBox,
            typename core::Variable<T>::Info &blockInfo, const size_t step,
            const size_t blockIndexOffset, const bool isRowMajor)

    {
        //     const std::vector<char> &buffer = bufferSTL.m_Buffer;
        // size_t position = blockIndexOffset;

        // info = blockCharacteristics in BP3
        /* here blockIndexOffset is the blockID because there is no entryID as
         * with the db backend*/
        // TODO: this may not be correct...
        typename core::Variable<T>::Info info =
            *m_JuleaKVInteractionReader.GetBlockMetadata(
                variable, m_ProjectNamespace, m_Name, step, blockIndexOffset);

        // check if they intersect
        helper::SubStreamBoxInfo subStreamInfo;

        if (helper::GetTotalSize(info.Count) == 0)
        {
            subStreamInfo.ZeroBlock = true;
        }

        subStreamInfo.BlockBox = helper::StartEndBox(info.Start, info.Count);
        // std::cout << "BlockBox: (["
        //           << helper::VectorToCSV(subStreamInfo.BlockBox.first) << "],
        //           ["
        //           << helper::VectorToCSV(subStreamInfo.BlockBox.second) <<
        //           "])"
        //           << std::endl;
        // std::cout << "selectionBox: (["
        //           << helper::VectorToCSV(selectionBox.first) << "], ["
        //           << helper::VectorToCSV(selectionBox.second) << "])"
        //           << std::endl;
        subStreamInfo.IntersectionBox =
            helper::IntersectionBox(selectionBox, subStreamInfo.BlockBox);

        if (subStreamInfo.IntersectionBox.first.empty() ||
            subStreamInfo.IntersectionBox.second.empty())
        {
            return;
        }

        const size_t dimensions = info.Shape.size();
        if (dimensions != blockInfo.Shape.size())
        {
            throw std::invalid_argument(
                "ERROR: block Shape (available) and "
                "selection Shape (requested) number of dimensions, do not "
                "match "
                "when reading global array variable " +
                variableName + ", in call to Get");
        }

        Dims readInShape = info.Shape;
        // if (m_ReverseDimensions)
        // {
        //     std::reverse(readInShape.begin(), readInShape.end());
        // }

        for (size_t i = 0; i < dimensions; ++i)
        {
            if (blockInfo.Start[i] + blockInfo.Count[i] > readInShape[i])
            {
                throw std::invalid_argument(
                    "ERROR: selection Start " +
                    helper::DimsToString(blockInfo.Start) + " and Count " +
                    helper::DimsToString(blockInfo.Count) +
                    " (requested) is out of bounds of (available) "
                    "Shape " +
                    helper::DimsToString(readInShape) +
                    " , when reading global array variable " + variableName +
                    ", in call to Get");
            }
        }

        // relative position
        subStreamInfo.Seeks.first =
            sizeof(T) * helper::LinearIndex(subStreamInfo.BlockBox,
                                            subStreamInfo.IntersectionBox.first,
                                            isRowMajor);
        // std::cout << "Seeks.first: " << subStreamInfo.Seeks.first <<
        // std::endl;

        subStreamInfo.Seeks.second =
            sizeof(T) * (helper::LinearIndex(
                             subStreamInfo.BlockBox,
                             subStreamInfo.IntersectionBox.second, isRowMajor) +
                         1);
        // std::cout << "Seeks.second: " << subStreamInfo.Seeks.second
        // << std::endl;

        //     const size_t payloadOffset =
        //         blockCharacteristics.Statistics.PayloadOffset;
        //     const auto &bp3Op = blockCharacteristics.Statistics.Op;
        //     // if they intersect get info Seeks (first: start, second:
        //     // count) depending on operation info
        //     if (bp3Op.IsActive)
        //     {
        //         lf_SetSubStreamInfoOperations(bp3Op, payloadOffset,
        //         subStreamInfo,
        //                                       m_IsRowMajor);
        //     }
        //     else
        //     {
        //         // make it absolute if no operations
        //         subStreamInfo.Seeks.first += payloadOffset;
        //         subStreamInfo.Seeks.second += payloadOffset;
        //     }
        //     subStreamInfo.SubStreamID =
        //         static_cast<size_t>(blockCharacteristics.Statistics.FileIndex);

        //     blockInfo.StepBlockSubStreamsInfo[step].push_back(
        //         std::move(subStreamInfo));

        // make it absolute if no operations
        // subStreamInfo.Seeks.first += 42;
        // subStreamInfo.Seeks.second += 42;

        // subStreamInfo.SubStreamID = static_cast<size_t>(blockIndexOffset);
        // TODO: changed for kv reader to blockID
        subStreamInfo.SubStreamID = static_cast<size_t>(info.BlockID);
        // static_cast<size_t>(blockCharacteristics.Statistics.FileIndex);

        blockInfo.StepBlockSubStreamsInfo[step].push_back(
            std::move(subStreamInfo));
    };

    // BODY OF FUNCTIONS STARTS HERE
    const std::map<size_t, std::vector<size_t>> &indices =
        variable.m_AvailableStepBlockIndexOffsets;

    // std::cout << "blockInfo.Start0: " << blockInfo.Start[0]
    //   << " blockInfo.Count0: " << blockInfo.Count[0] << "\n";
    // last param is m_reverseDims
    const Box<Dims> selectionBox =
        helper::StartEndBox(blockInfo.Start, blockInfo.Count, false);

    auto itStep = std::next(indices.begin(), blockInfo.StepsStart);

    for (size_t i = 0; i < blockInfo.StepsCount; ++i)
    {
        // const size_t step = itStep->first;
        // TODO: step was one too high... hacky temp solution
        const size_t step = (itStep->first) - 1;
        const std::vector<size_t> &blockOffsets = itStep->second;

        if (variable.m_ShapeID == ShapeID::GlobalArray)
        {
            for (const size_t blockOffset : blockOffsets)
            {
                // std::cout << "step: " << step << "\n";
                // std::cout << "blockOffset: " << blockOffset << "\n";
                lf_SetSubStreamInfoGlobalArray(variable.m_Name, selectionBox,
                                               blockInfo, step, blockOffset,
                                               true);
            }
        }
        else if (variable.m_ShapeID == ShapeID::LocalArray)
        {
            // lf_SetSubStreamInfoLocalArray(
            // variable.m_Name, selectionBox, blockInfo, step,
            // blockOffsets[blockInfo.BlockID], m_Metadata, m_IsRowMajor);
        }

        ++itStep;
    }
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif // ADIOS2_ENGINE_JuleaKVReader_TCC_
