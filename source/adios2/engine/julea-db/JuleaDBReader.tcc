/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEADBREADER_TCC_
#define ADIOS2_ENGINE_JULEADBREADER_TCC_

#include "JuleaDBReader.h"

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
void JuleaDBReader::GetSyncCommon(Variable<std::string> &variable,
                                  std::string *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________GetSync String_____________________"
                  << std::endl;
        std::cout << "Julea DB Reader " << m_ReaderRank
                  << " Namespace: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }

    for (typename Variable<std::string>::Info &blockInfo :
         variable.m_BlocksInfo)
    {
        long unsigned int dataSize = 0;
        guint32 buffer_len = 0;
        std::string nameSpace = m_Name;
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

        // GetBlockMetadataFromJulea(nameSpace, variable.m_Name, &md_buffer,
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
void JuleaDBReader::GetSyncCommon(Variable<T> &variable, T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________GetSync T_____________________"
                  << std::endl;
        std::cout << "Julea DB Reader " << m_ReaderRank
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
void JuleaDBReader::GetDeferredCommon(Variable<T> &variable, T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Reader " << m_ReaderRank << "     GetDeferred("
                  << variable.m_Name << ")\n";
    }

    // store data pointer in related blockinfo to be called in perform gets
    InitVariableBlockInfo(variable, data);

    size_t size = variable.m_BlocksInfo.size();
    variable.m_BlocksInfo[size - 1].BlockID = variable.m_BlockID;

    m_DeferredVariables.insert(variable.m_Name);
}

template <class T>
void JuleaDBReader::ReadBlock(Variable<T> &variable, T *data, size_t blockID)
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
    std::string nameSpace = m_Name;

    long unsigned int dataSize = 0;
    guint32 buffer_len = 0;
    gpointer md_buffer = nullptr;

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
    GetCountFromBlockMetadata(nameSpace, variable.m_Name, step, blockID, &count,
                              entryID, variable.m_SingleValue, data);
    if (variable.m_SingleValue)
    {
        // FIXME: get Value from DB
        std::cout << "Single value" << std::endl;
        return;
    }

    size_t numberElements = helper::GetTotalSize(count);
    dataSize = numberElements * variable.m_ElementSize;
    DBGetVariableDataFromJulea(variable, data, nameSpace, dataSize,
                               stepBlockID);
}

template <class T>
void JuleaDBReader::ReadBlockMD(typename core::Variable<T>::Info &blockInfo,
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
void JuleaDBReader::ReadVariableBlocks(Variable<T> &variable)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________ReadVariableBlocks_____________" << std::endl;
        std::cout << "Julea DB Reader " << m_ReaderRank
                  << " Namespace: " << m_Name
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
    }
}

template <class T>
std::map<size_t, std::vector<typename core::Variable<T>::Info>>
JuleaDBReader::AllStepsBlocksInfo(const core::Variable<T> &variable) const
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
            std::cout << "Julea DB Reader " << m_ReaderRank
                      << " Namespace: " << m_Name
                      << " Variable name: " << variable.m_Name << std::endl;
            std::cout << "--- step: " << step
                      << " blockPositions: " << blockPositions.data()[0]
                      << std::endl;
        }

        // bp3 index starts at 1
        allStepsBlocksInfo[step - 1] =
            BlocksInfoCommon(variable, blockPositions, step - 1);
    }
    return allStepsBlocksInfo;
}

template <class T>
std::vector<typename core::Variable<T>::Info>
JuleaDBReader::BlocksInfoCommon(const core::Variable<T> &variable,
                                const std::vector<size_t> &blocksIndexOffsets,
                                size_t step) const
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________BlocksInfoCommon_____________" << std::endl;
        std::cout << "Julea DB Reader " << m_ReaderRank
                  << " Namespace: " << m_Name
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

        auto nameSpace = m_Name;
        long unsigned int dataSize = 0;
        auto stepBlockID = g_strdup_printf("%lu_%lu", step, i);
        auto entryID = blocksIndexOffsets[i];

        typename core::Variable<T>::Info info =
            *DBGetBlockMetadata(variable, nameSpace, step, i, entryID);
        info.IsReverseDims = false;
        info.Step = step;

        blocksInfo.push_back(info);
    }
    return blocksInfo;
}

// FIXME: not yet tested!
template <class T>
std::vector<std::vector<typename core::Variable<T>::Info>>
JuleaDBReader::AllRelativeStepsBlocksInfo(
    const core::Variable<T> &variable) const
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________AllRelativeStepsBlocksInfo_____________"
                  << std::endl;
        std::cout << "Julea DB Reader " << m_ReaderRank
                  << " Namespace: " << m_Name
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
JuleaDBReader::BlocksInfo(const core::Variable<T> &variable,
                          const size_t step) const
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________BlocksInfo_____________" << std::endl;
        std::cout << "Julea DB Reader " << m_ReaderRank
                  << " Namespace: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }

    // bp4 format starts at 1
    auto itStep = variable.m_AvailableStepBlockIndexOffsets.find(step + 1);
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
JuleaDBReader::InitVariableBlockInfo(core::Variable<T> &variable, T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n__________InitVariableBlockInfo_____________"
                  << std::endl;
        std::cout << "Julea DB Reader " << m_ReaderRank
                  << " Namespace: " << m_Name
                  << " Variable name: " << variable.m_Name << std::endl;
    }
    const size_t stepsStart = variable.m_StepsStart;
    const size_t stepsCount = variable.m_StepsCount;

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
            JuleaDBReader::BlocksInfo(variable, stepsStart);

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
void JuleaDBReader::SetVariableBlockInfo(
    core::Variable<T> &variable,
    typename core::Variable<T>::Info &blockInfo) const
{
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

        size_t position = blockIndexOffset;

        auto nameSpace = m_Name;
        // auto stepBlockID = g_strdup_printf("%lu_%lu", step, i);
        // auto entryID = blocksIndexOffsets[i];
        typename core::Variable<T>::Info info =
            *DBGetBlockMetadata(variable, nameSpace, step, i, blockIndexOffset);

        // ReadBlockMD(variable, blockInfo.BlockID);

        //     const Characteristics<T> blockCharacteristics =
        //         ReadElementIndexCharacteristics<T>(buffer, position,
        //                                            TypeTraits<T>::type_enum,
        //                                            false,
        //                                            m_Minifooter.IsLittleEndian);

        // TODO
        // here the blockmetadata needs to be read

        // check if they intersect
        helper::SubStreamBoxInfo subStreamInfo;

        // if (helper::GetTotalSize(blockCharacteristics.Count) == 0)
        {
            subStreamInfo.ZeroBlock = true;
        }

        subStreamInfo.BlockBox =
            helper::StartEndBox(blockInfo.Start, blockInfo.Count);
        subStreamInfo.IntersectionBox =
            helper::IntersectionBox(selectionBox, subStreamInfo.BlockBox);

        if (subStreamInfo.IntersectionBox.first.empty() ||
            subStreamInfo.IntersectionBox.second.empty())
        {
            return;
        }

        // FIXME: some kind of check is needed for this engine.
        // However, not sure what blockCharacteristics actually stores

        // const size_t dimensions = blockCharacteristics.Shape.size();
        // if (dimensions != blockInfo.Shape.size())
        // {
        //     throw std::invalid_argument(
        //         "ERROR: block Shape (available) and "
        //         "selection Shape (requested) number of dimensions, do not "
        //         "match "
        //         "when reading global array variable " +
        //         variableName + ", in call to Get");
        // }

        //     Dims readInShape = blockCharacteristics.Shape;
        //     if (m_ReverseDimensions)
        //     {
        //         std::reverse(readInShape.begin(), readInShape.end());
        //     }

        //     for (size_t i = 0; i < dimensions; ++i)
        //     {
        //         if (blockInfo.Start[i] + blockInfo.Count[i] > readInShape[i])
        //         {
        //             throw std::invalid_argument(
        //                 "ERROR: selection Start " +
        //                 helper::DimsToString(blockInfo.Start) + " and Count "
        //                 + helper::DimsToString(blockInfo.Count) + "
        //                 (requested) is out of bounds of (available) " "Shape
        //                 " + helper::DimsToString(readInShape) + " , when
        //                 reading global array variable " + variableName +
        //                 ", in call to Get");
        //         }
        //     }

        // relative position
        subStreamInfo.Seeks.first =
            sizeof(T) * helper::LinearIndex(subStreamInfo.BlockBox,
                                            subStreamInfo.IntersectionBox.first,
                                            isRowMajor);
        std::cout << "Seeks.first: " << subStreamInfo.Seeks.first << std::endl;

        subStreamInfo.Seeks.second =
            sizeof(T) * (helper::LinearIndex(
                             subStreamInfo.BlockBox,
                             subStreamInfo.IntersectionBox.second, isRowMajor) +
                         1);
        std::cout << "Seeks.second: " << subStreamInfo.Seeks.second
                  << std::endl;

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
    };

    // BODY OF FUNCTIONS STARTS HERE
    const std::map<size_t, std::vector<size_t>> &indices =
        variable.m_AvailableStepBlockIndexOffsets;

    // last param is m_reverseDims
    const Box<Dims> selectionBox =
        helper::StartEndBox(blockInfo.Start, blockInfo.Count, false);

    auto itStep = std::next(indices.begin(), blockInfo.StepsStart);

    for (size_t i = 0; i < blockInfo.StepsCount; ++i)
    {
        const size_t step = itStep->first;
        const std::vector<size_t> &blockOffsets = itStep->second;

        if (variable.m_ShapeID == ShapeID::GlobalArray)
        {
            for (const size_t blockOffset : blockOffsets)
            {
                lf_SetSubStreamInfoGlobalArray(variable.m_Name, selectionBox,
                                               blockInfo, step, blockOffset,
                                               false);
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

#endif // ADIOS2_ENGINE_JULEADBREADER_TCC_
