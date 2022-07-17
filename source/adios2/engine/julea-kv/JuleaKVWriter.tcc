/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JuleaKVWriter_TCC_
#define ADIOS2_ENGINE_JuleaKVWriter_TCC_

// #include "adios2/helper/adiosCommMPI.h"

// #include "JuleaDBDAIInteractionWriter.h"
#include "JuleaKVWriter.h"
#include "adios2/toolkit/interop/julea/JuleaCDO.h"

// #include <adios2_c.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <stdexcept> // std::out_of_range
#include <string>
#include <utility>

#include <mpi.h>

namespace adios2
{
namespace core
{
namespace engine
{

// void JuleaKVWriter::JuleaDBDAICheckPrecomputes(std::string fileName,
// std::string varName, )
// {

// }

// template <class T>
// void JuleaKVWriter::ManageBlockStepMetadataOriginal(Variable<T> &variable,
//                                                        const T *data,
//                                                        T &blockMin, T
//                                                        &blockMax)
// {
//     // T blockMin;
//     // T blockMax;
//     bool isOriginal = true;
//     // will be ignored when writing
//     T blockMean;
//     T blockSum;
//     T blockVar;

//     m_JuleaCDO.SetMinMax(variable, data, blockMin, blockMax, m_CurrentStep,
//                          m_CurrentBlockID);

//     m_JuleaCDO.BufferCDOStats(variable, blockMin, blockMax, blockMean,
//     blockSum, blockVar, isOriginal);
// }

// template <>
// void JuleaKVWriter::ManageBlockStepMetadataOriginal<std::string>(
//     Variable<std::string> &variable, const std::string *data,
//     std::string &blockMin, std::string &blockMax)
// {
//     // TODO implement?
// }

// template <>
// void JuleaKVWriter::ManageBlockStepMetadataOriginal<std::complex<float>>(
//     Variable<std::complex<float>> &variable, const std::complex<float> *data,
//     std::complex<float> &blockMin, std::complex<float> &blockMax)
// {
//     // TODO implement?
// }

// template <>
// void JuleaKVWriter::ManageBlockStepMetadataOriginal<std::complex<double>>(
//     Variable<std::complex<double>> &variable, const std::complex<double>
//     *data, std::complex<double> &blockMin, std::complex<double> &blockMax)
// {
//     // TODO implement?
// }

// had data parameter but currently no way to indicate that tags should be set
// when actual data meets query. only works on min/max/mean/sum/var; not on data
// buffer
template <class T>
void JuleaKVWriter::TaggingDataIfRequired(
    std::string fileName, std::string varName, size_t currentStep,
    size_t blockID, T blockMin, T blockMax, T blockMean, T blockSum, T blockVar)
{
    // std::cout << "Debug: TaggingDataIfRequired \n";
    JDAIStatistic statistic;
    JDAIOperator op;
    JDAIGranularity granularity;
    std::string tagName;

    double threshold = 0;

    // auto it = m_JuleaCDO.m_Tags.find(
    auto it = m_JuleaDBInteractionWriter.m_Tags.find(
        std::pair<std::string, std::string>(fileName, varName));
    if (it == m_JuleaDBInteractionWriter.m_Tags.end())
    {
        // TODO: should not happen; is checked in init whether there is
        // anything in tag table
    }
    else
    {
        // // Hard-coded way for eval
        // if (blockMax < m_JuleaCDO.m_TagThreshold)
        // {
        //     m_JuleaDBInteractionWriter.AddEntriesForTagTable(
        //         m_Name, tagName, fileName, varName, currentStep, blockID,
        //         blockMax);
        // }

        for (std::list<adios2::interop::JuleaCDO::Tag>::iterator it2 =
                 it->second.begin();
             it2 != it->second.end(); ++it2)
        {
            adios2::interop::JuleaCDO::Tag currentTag = *it2;

            tagName = currentTag.m_TagName;
            statistic = currentTag.m_Statistic;
            op = currentTag.m_Operator;
            granularity = currentTag.m_Granularity;
            threshold = currentTag.m_Threshold_d;

            // ----------------- Granularity -------------------------
            switch (granularity)
            {
            case J_DAI_GRAN_BLOCK:
                // std::cout << "Debug: J_DAI_GRAN_BLOCK reached \n";

                // ----------------- Statistic -------------------------
                switch (statistic)
                {
                case J_DAI_STAT_MIN:
                    // std::cout << "Debug: J_DAI_STAT_MIN reached \n";
                    // ----------------- Operator
                    switch (op)
                    {
                    case J_DAI_OP_GT:
                        // std::cout << "Debug: J_DAI_OP_GT reached \n";
                        if (blockMin > threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockMin);
                        }
                        break;
                    case J_DAI_OP_LT:
                        if (blockMin < threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockMin);
                        }
                        break;
                    } // end op
                    break;
                case J_DAI_STAT_MAX:
                    // std::cout << "Debug: J_DAI_STAT_MAX reached \n";
                    // ----------------- Operator
                    switch (op)
                    {
                    case J_DAI_OP_GT:
                        // std::cout << "Debug: J_DAI_OP_GT reached \n";
                        if (blockMax > threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockMax);
                        }
                        break;
                    case J_DAI_OP_LT:
                        // std::cout << "Debug: J_DAI_OP_LT reached \n";
                        if (blockMax < threshold)
                        {
                            std::cout << "this case is called\n";
                            std::cout << "blockMax: " << blockMax << "\n";
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockMax);
                        }
                        break;
                    } // end op
                    break;
                case J_DAI_STAT_MEAN:
                    // ----------------- Operator
                    switch (op)
                    {
                    case J_DAI_OP_GT:
                        if (blockMean > threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockMean);
                        }
                        break;
                    case J_DAI_OP_LT:
                        if (blockMean < threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockMean);
                        }
                        break;
                    } // end op
                    break;
                case J_DAI_STAT_SUM:
                    // ----------------- Operator
                    switch (op)
                    {
                    case J_DAI_OP_GT:
                        if (blockSum > threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockSum);
                        }
                        break;
                    case J_DAI_OP_LT:
                        if (blockSum < threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockSum);
                        }
                        break;
                    } // end op
                    break;
                case J_DAI_STAT_VAR:
                    // ----------------- Operator
                    switch (op)
                    {
                    case J_DAI_OP_GT:
                        if (blockVar > threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockVar);
                        }
                        break;
                    case J_DAI_OP_LT:
                        if (blockVar < threshold)
                        {
                            m_JuleaDBInteractionWriter.AddEntriesForTagTable(
                                m_ProjectNamespace, tagName, fileName, varName,
                                currentStep, blockID, blockVar);
                        }
                        break;
                    } // end op
                }     // end statistic
                break;
            case J_DAI_GRAN_STEP:
                // TODO: compute something
                break;
            case J_DAI_GRAN_VARIABLE:
                // TODO: compute something
                break;
            } // end granularity
        }
    }
}

template <>
void JuleaKVWriter::TaggingDataIfRequired<std::string>(
    std::string fileName, std::string varName, size_t currentStep,
    size_t blockID, std::string blockMin, std::string blockMax,
    std::string blockMean, std::string blockSum, std::string blockVar)
{
    // TODO implement?
}

template <>
void JuleaKVWriter::TaggingDataIfRequired<std::complex<float>>(
    std::string fileName, std::string varName, size_t currentStep,
    size_t blockID, std::complex<float> blockMin, std::complex<float> blockMax,
    std::complex<float> blockMean, std::complex<float> blockSum,
    std::complex<float> blockVar)
{
    // TODO implement?
}

template <>
void JuleaKVWriter::TaggingDataIfRequired<std::complex<double>>(
    std::string fileName, std::string varName, size_t currentStep,
    size_t blockID, std::complex<double> blockMin,
    std::complex<double> blockMax, std::complex<double> blockMean,
    std::complex<double> blockSum, std::complex<double> blockVar)
{
    // TODO implement?
}

// based on isOriginalFormat different MD is computed
template <class T>
void JuleaKVWriter::ManageBlockStepMetadata(Variable<T> &variable,
                                               const T *data, T &blockMin,
                                               T &blockMax, T &blockMean,
                                               T &blockSum, T &blockVar)
{
    T blockSumSquares;

    T stepMin;
    T stepMax;
    T stepMean;
    T stepSum;
    T stepSumSquares;
    T stepVar;

    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);

    // T blockStd;
    uint32_t entryID = 0;
    std::vector<double> testBuffer;
    std::vector<T> testBuffer2;
    double testDouble = 42.0;

    // if(m_isOriginalFormat)
    // {
    //     m_JuleaCDO.SetMinMax(variable, data, blockMin, blockMax,
    //     m_CurrentStep,
    //                      m_CurrentBlockID);
    //     m_JuleaCDO.BufferCDOStats(variable, blockMin, blockMax, blockMean,
    //     blockSum, blockVar, m_isOriginalFormat);
    // }

    m_JuleaCDO.ComputeAllBlockStats(variable, data, blockMin, blockMax,
                                    blockMean, blockSum, blockSumSquares,
                                    blockVar, m_IsOriginalFormat, m_Name);

    /**  to initialize the global min/max, they are set to the
        first min/max for the first block of the first step */
    if ((m_CurrentStep == 0) && (m_CurrentBlockID == 0))
    {
        variable.m_Min = blockMin;
        variable.m_Max = blockMax;
        stepMin = blockMin;
        stepMax = blockMax;
    }

    /** reduce only necessary if more than one process*/
    if (m_Comm.Size() > 1)
    // if (m_WriterRank > 0)
    {
        /** const T *sendbuf, T *recvbuf, size_t count, Op op, int root, const
        std::string &hint = std::string()) */
        m_Comm.Reduce(&blockMin, &stepMin, 1, helper::Comm::Op::Min, 0);
        m_Comm.Reduce(&blockMax, &stepMax, 1, helper::Comm::Op::Max, 0);
        if (!m_IsOriginalFormat)
        {
            m_Comm.Reduce(&blockSum, &stepSum, 1, helper::Comm::Op::Sum, 0);
            m_Comm.Reduce(&blockSumSquares, &stepSumSquares, 1,
                          helper::Comm::Op::Sum, 0);
        }

        /** not required since blockSum is also computed now */
        // m_Comm.Reduce(&blockMean, &stepMean, 1, helper::Comm::Op::Sum, 0);

        // if (variable.m_Name == m_PrecipitationName)
        // {
        //     m_Comm.Reduce(&blockMean, &stepSum, 1, helper::Comm::Op::Sum, 0);
        //     m_HPrecSum.push_back(stepSum);
        // }
        if (!m_IsOriginalFormat)
        {

            stepMean = stepSum / (number_elements * m_Comm.Size());
            stepVar = stepSumSquares / (number_elements * m_Comm.Size());
        }
    }
    else if (m_Comm.Size() == 1)
    {
        stepMin = blockMin;
        stepMax = blockMax;
        stepMean = blockMean;
        stepSum = blockSum;
        stepSumSquares = blockSumSquares;
        stepVar = blockVar;
    }

    if (stepMin < variable.m_Min)
    {
        // std::cout << "updated global min from " << variable.m_Min << " to "
        //   << stepMin << std::endl;
        variable.m_Min = stepMin;
    }

    if (stepMax > variable.m_Max)
    {
        // std::cout << "updated global max from "  << variable.m_Max << " to "
        // << stepMax << std::endl;
        variable.m_Max = stepMax;
    }

    m_JuleaCDO.BufferCDOStats(variable, blockMin, blockMax, blockMean, blockSum,
                              blockVar, m_IsOriginalFormat);

    if (m_CurrentStep % m_JuleaCDO.m_StepsPerDay == 0)
    {
        int day;
        int month;
        int year;
        m_JuleaCDO.ComputeDailyStats(variable.m_Name);
        m_JuleaCDO.ComputeDateFromStep(m_CurrentStep, year, month, day);

        std::cout << "Year: " << year << " Month: " << month << " Day: " << day
                  << "\n";
        if (m_WriterRank == 0)
        {
            m_JuleaDBInteractionWriter.AddEntriesForDailyGlobalStatsTable(
                m_ProjectNamespace, m_Name, variable.m_Name, m_CurrentStep,
                m_JuleaCDO, year, month, day);
        }
    }

    if (m_CurrentStep % m_JuleaCDO.m_StepsPerMonth == 0)
    {
        m_JuleaCDO.ComputeMonthlyStats(variable.m_Name);
    }

    if (m_CurrentStep % m_JuleaCDO.m_StepsPerYear == 0)
    {
        m_JuleaCDO.ComputeYearlyStats(variable.m_Name);
        if (m_WriterRank == 0)
        {
            m_JuleaDBInteractionWriter.AddEntriesForClimateIndexTable(
                m_ProjectNamespace, m_Name, variable.m_Name, m_CurrentStep,
                m_JuleaCDO);
        }
    }
}

template <>
void JuleaKVWriter::ManageBlockStepMetadata<std::string>(
    Variable<std::string> &variable, const std::string *data,
    std::string &blockMin, std::string &blockMax, std::string &blockMean,
    std::string &blockSum, std::string &blockVar)
{
    // TODO implement?
}

template <>
void JuleaKVWriter::ManageBlockStepMetadata<std::complex<float>>(
    Variable<std::complex<float>> &variable, const std::complex<float> *data,
    std::complex<float> &blockMin, std::complex<float> &blockMax,
    std::complex<float> &blockMean, std::complex<float> &blockSum,
    std::complex<float> &blockVar)
{
    // TODO implement?
}

template <>
void JuleaKVWriter::ManageBlockStepMetadata<std::complex<double>>(
    Variable<std::complex<double>> &variable, const std::complex<double> *data,
    std::complex<double> &blockMin, std::complex<double> &blockMax,
    std::complex<double> &blockMean, std::complex<double> &blockSum,
    std::complex<double> &blockVar)
{
    // TODO implement?
}

template <class T>
void JuleaKVWriter::SetBlockID(Variable<T> &variable)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : SetBlockID()\n";
    }

    if (variable.m_ShapeID == ShapeID::GlobalValue ||
        variable.m_ShapeID == ShapeID::GlobalArray)
    {
        m_CurrentBlockID = m_WriterRank;
        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].resize(
            m_Comm.Size());
        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].at(
            m_WriterRank) = m_CurrentBlockID;
    }
    else if (variable.m_ShapeID == ShapeID::JoinedArray)
    {
        std::cout << "JoinedArray: Currently not implemented yet." << std::endl;
    }
    else if (variable.m_ShapeID == ShapeID::LocalArray ||
             variable.m_ShapeID == ShapeID::LocalValue)
    {
        if (m_Comm.Size() == 1)
        {
            std::cout << "LocalValue/: Nothing to do?! Only increment "
                         "after put."
                      << std::endl;
            variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
                m_CurrentBlockID);
        }
        else
        {
            std::cout << "LocalArray: Have fun with synchronized counter "
                         "across processes."
                      << std::endl;
            variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
                m_CurrentBlockID);
        }
    }
    else
    {
        std::cout << "Shape Type not known." << std::endl;
    }
}

/**
 * Only works for 2D variables!
 */
template <class T>
void JuleaKVWriter::ComputeGlobalDimensions(Variable<T> &variable)
{
    int globalX = 0;
    int globalY = 0;

    int localX = 0;
    int localY = 0;
    int testNumberProcesses = 0;

    globalX = variable.m_Shape[0];
    globalY = variable.m_Shape[1];

    localX = variable.m_Count[0];
    localY = variable.m_Count[1];

    m_JuleaCDO.m_numberBlocksX = globalX / localX;
    m_JuleaCDO.m_numberBlocksY = globalY / localY;

    testNumberProcesses =
        m_JuleaCDO.m_numberBlocksX * m_JuleaCDO.m_numberBlocksY;

    if (m_Comm.Size() == testNumberProcesses)
    {
        std::cout << "wuhu: gleich groß \n";
    }

    std::cout << "X = " << m_JuleaCDO.m_numberBlocksX << "\n";
    std::cout << "Y = " << m_JuleaCDO.m_numberBlocksY << "\n";
}

template <class T>
void JuleaKVWriter::PutSyncToJulea(
    Variable<T> &variable, const T *data,
    const typename Variable<T>::Info &blockInfo)
{
    // bool original = false;
    T blockMin;
    T blockMax;
    T blockMean;
    T blockSum;
    T blockSumSquares;
    T blockVar;
    uint32_t entryID = 0;

    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : PutSyncToJulea("
                  << variable.m_Name << ") --- BlockID = " << m_CurrentBlockID
                  << " \n";
    }
    const DataType type = m_IO.InquireVariableType(variable.m_Name);

    /**
     * Attention: Here the assumption is made, that the dimensions are the same
     * for all processes and all steps!
     */
    if ((m_CurrentStep == 0) && (m_WriterRank == 0) &&
        (variable.m_Shape.size() == 2))
    {
        ComputeGlobalDimensions(variable);
    }

    if (m_ComputeStatsCombined)
    {
        // m_IsOriginalFormat determines which MD to compute
        ManageBlockStepMetadata(variable, data, blockMin, blockMax, blockMean,
                                blockSum, blockVar);
        // std::cout << "m_CurrentBlockID: " << m_CurrentBlockID << " blockMax:
        // " << blockMax <<   "\n";
        TaggingDataIfRequired(m_Name, variable.m_Name, m_CurrentStep,
                              m_CurrentBlockID, blockMin, blockMax, blockMean,
                              blockSum, blockVar);
    }
    else
    {

        auto it = m_JuleaCDO.m_Precomputes.find(
            std::pair<std::string, std::string>(m_Name, variable.m_Name));
        if (it == m_JuleaCDO.m_Precomputes.end())
        {
            // TODO: should not happen; is checked in init whether there is
            // anything in tag table
        }
        else
        {

            T blockResult;
            std::vector<T> blockResults;
            for (std::list<std::pair<JDAIStatistic, JDAIGranularity>>::iterator
                     it2 = it->second.begin();
                 it2 != it->second.end(); ++it2)
            {
                std::pair<JDAIStatistic, JDAIGranularity> pair = *it2;
                switch (pair.second)
                {
                case J_DAI_GRAN_BLOCK:
                    m_JuleaCDO.ComputeBlockStat(variable, data, blockResult,
                                                pair.first);
                    break;
                case J_DAI_GRAN_STEP:
                    // TODO: compute something
                    break;
                case J_DAI_GRAN_VARIABLE:
                    // TODO: compute something
                    break;
                    blockResults.push_back(blockResult);
                }
                // FIXME: write these results to JULEA
            }
        }
    }

    auto it2 = m_JuleaCDO.m_Tags.find(
        std::pair<std::string, std::string>(m_Name, variable.m_Name));
    if (it2 == m_JuleaCDO.m_Tags.end())
    {
        // TODO: should not happen; is checked in init whether there is
        // anything in tag table
    }
    else
    {

        // TODO: check whether tag query is met
    }

    // FIXME: compute step values
    //  JuleaDBDAIStepValues(variable, blockMin, blockMean, blockMax);
    //  m_JuleaCDO.ComputeStepStats(variable, blockMin, blockMean,
    //  blockMax,
    //                                   m_CurrentStep, m_CurrentBlockID);

    // auto stepBlockID =
    //     g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
    // std::cout << "    stepBlockID: " << stepBlockID << std::endl;

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
            // std::cout << "___ Written variables:" << ' ' << *it <<
            // std::endl;
        }
    }

    // FIXME: implement PutCDOStatsToJulea(blockStats)

    // TODO: check if there really is no case for global variables to have
    // different features across different blocks
    /* note (23.10.21): the global min/max do not work this way!
        fixed by setting them in 'JuleaDBDAISetMinMax' */
    if (m_WriterRank == 0)
    {
        // TODO: add mean value to DB
        /** updates the variable metadata as there is a new block now */
        m_JuleaDBInteractionWriter.PutVariableMetadataToJulea(
            variable, m_ProjectNamespace, m_Name, variable.m_Name,
            m_CurrentStep, m_CurrentBlockID, m_IsOriginalFormat);
    }

    /** put block metadata to DB */
    m_JuleaDBInteractionWriter.PutBlockMetadataToJulea(
        variable, m_ProjectNamespace, m_Name, variable.m_Name, m_CurrentStep,
        m_CurrentBlockID, blockInfo, blockMin, blockMax, blockMean, blockSum,
        blockVar, entryID, m_IsOriginalFormat);

    /** put data to object store */
    m_JuleaDBInteractionWriter.PutVariableDataToJulea(
        variable, data, m_ProjectNamespace, m_Name, entryID);
}

template <class T>
void JuleaKVWriter::PutSyncCommon(
    Variable<T> &variable, const typename Variable<T>::Info &blockInfo)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : PutSyncCommon("
                  << variable.m_Name << ") --- File = " << m_Name
                  << " --- CurrentStep = " << m_CurrentStep << "\n";
    }

    PutSyncToJulea(variable, blockInfo.Data, blockInfo);
}

template <class T>
void JuleaKVWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : PutSyncCommon("
                  << variable.m_Name << ") --- File = " << m_Name
                  << " --- CurrentStep = " << m_CurrentStep << "\n";
    }

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());
    PutSyncToJulea(variable, data, blockInfo);
}

template <class T>
void JuleaKVWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : PutDeferredCommon("
                  << variable.m_Name << ") "
                  << "\n";
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
void JuleaKVWriter::PerformPutCommon(Variable<T> &variable)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank
                  << ") : PerformPutCommon()\n";
    }
    for (size_t i = 0; i < variable.m_BlocksInfo.size(); ++i)
    {
        // std::cout << "variable: " << variable.m_Name << "--- i: " << i
        // << std::endl;
        // variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
        // m_CurrentBlockID);

        SetBlockID(variable);

        //  if (variable.m_ShapeID == ShapeID::GlobalValue ||
        //     variable.m_ShapeID == ShapeID::GlobalArray)
        // {
        //     std::cout
        //         << "GlobalValue/GlobalArray: m_CurrentBlockID =
        //         m_WriterRank"
        //         << std::endl;
        //     m_CurrentBlockID = m_WriterRank;
        //     variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].resize(
        //         m_Comm.Size());
        //     variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].at(
        //         m_WriterRank) = m_CurrentBlockID;
        // }
        // else if (variable.m_ShapeID == ShapeID::JoinedArray)
        // {
        //     std::cout << "JoinedArray: Currently not implemented yet."
        //               << std::endl;
        // }
        // else if (variable.m_ShapeID == ShapeID::LocalArray ||
        //          variable.m_ShapeID == ShapeID::LocalValue)
        // {
        //     if (m_Comm.Size() == 1)
        //     {
        //         std::cout << "LocalValue/: Nothing to do?! Only increment
        //         "
        //                      "after put."
        //                   << std::endl;
        //         variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep]
        //             .push_back(m_CurrentBlockID);
        //     }
        //     else
        //     {
        //         std::cout << "LocalArray: Have fun with synchronized
        //         counter
        //         "
        //                      "across processes."
        //                   << std::endl;
        //         variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep]
        //             .push_back(m_CurrentBlockID);
        //     }
        // }
        // else
        // {
        //     std::cout << "Shape Type not known." << std::endl;
        // }

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
        //     m_BP3Serializer.PutSpanMetadata(variable,
        //     itSpanBlock->second);
        // }
    }

    variable.m_BlocksInfo.clear();
    variable.m_BlocksSpan.clear();
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JuleaKVWriter_TCC_ */
