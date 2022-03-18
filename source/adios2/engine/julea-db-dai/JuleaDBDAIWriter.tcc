/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JuleaDBDAIWRITER_TCC_
#define ADIOS2_ENGINE_JuleaDBDAIWRITER_TCC_

// #include "adios2/helper/adiosCommMPI.h"

// #include "JuleaDBDAIInteractionWriter.h"
#include "JuleaDBDAIWriter.h"

// #include <adios2_c.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include <mpi.h>

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
void JuleaDBDAIWriter::JuleaDBDAISetMinMax(Variable<T> &variable, const T *data,
                                           T &blockMin, T &blockMax,
                                           T &blockMean)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank
                  << ") : JuleaDBDAISetMinMax()\n";
    }

    T min = 0;
    T max = 0;
    T sum = 0;
    T mean = 0;

    T stepMin = 0;
    T stepMax = 0;
    T stepMean = 0;
    T stepSum = 0;

    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    adios2::helper::GetMinMax(data, number_elements, min, max);

    for (size_t i = 0; i < number_elements; ++i)
    {
        sum += data[i];
    }

    // TODO: cast to T ?
    mean = sum / (double)number_elements;

    blockMin = min;
    blockMax = max;
    blockMean = mean;
    T blockSum = sum;

    // TODO: check whether this is incorrect
    // there may be some cases where this is not working
    /*  to initialize the global min/max, they are set to the
        first min/max for the first block of the first step */
    if ((m_CurrentStep == 0) && (m_CurrentBlockID == 0))
    {
        variable.m_Min = min;
        variable.m_Max = max;
        stepMin = min;
        stepMax = max;
    }

    /* reduce only necessary if more than one process*/
    if (m_WriterRank > 0)
    {
        // const T *sendbuf, T *recvbuf, size_t count, Op op, int root, const
        // std::string &hint = std::string())
        m_Comm.Reduce(&blockMin, &stepMin, 1, helper::Comm::Op::Min, 0);
        m_Comm.Reduce(&blockMax, &stepMax, 1, helper::Comm::Op::Max, 0);
        m_Comm.Reduce(&blockMean, &stepMean, 1, helper::Comm::Op::Sum, 0);

        if (variable.m_Name == m_JuleaCDO.m_PrecipitationName)
        {
            m_Comm.Reduce(&blockMean, &stepSum, 1, helper::Comm::Op::Sum, 0);
            m_JuleaCDO.m_HPrecSum.push_back(stepSum);
        }
    }

    /** The mean of means is ONLY the same as the mean of all, when the
     * cardinality is the same for every sub-mean. E.g. mean(1+2+3+4+5+6) = 21/6
     * = 3.5 mean(1+2+3) = 6/3 = 2; mean(4+5+6) = 15/3 = 5; mean(2+5) = 7/2
     * = 3.5
     *
     * Also: dividing each sub-sum by the total number of elements allows to
     * simply sum these "non-means" to a total mean. E.g. (1+2+3)/6 = 6/6 = 1;
     * (4+5+6)/6 = 15/6 = 2.5; 1 + 2.5 = 3.5
     * However, this is unintuitive. So, first version is used.
     */
    blockMean = stepMean / m_Comm.Size();

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

    if (false)
    {
        std::cout << "min: " << min << std::endl;
        std::cout << "global min: " << variable.m_Min << std::endl;
        std::cout << "max: " << max << std::endl;
        std::cout << "global max: " << variable.m_Max << std::endl;
    }
}

template <>
void JuleaDBDAIWriter::JuleaDBDAISetMinMax<std::string>(
    Variable<std::string> &variable, const std::string *data,
    std::string &blockMin, std::string &blockMax, std::string &blockMean)
{
    // TODO implement?
}

template <>
void JuleaDBDAIWriter::JuleaDBDAISetMinMax<std::complex<float>>(
    Variable<std::complex<float>> &variable, const std::complex<float> *data,
    std::complex<float> &blockMin, std::complex<float> &blockMax,
    std::complex<float> &blockMean)
{
    // TODO implement?
}

template <>
void JuleaDBDAIWriter::JuleaDBDAISetMinMax<std::complex<double>>(
    Variable<std::complex<double>> &variable, const std::complex<double> *data,
    std::complex<double> &blockMin, std::complex<double> &blockMax,
    std::complex<double> &blockMean)
{
    // TODO implement?
}

template <class T>
void JuleaDBDAIWriter::JuleaDBDAIStepValues(Variable<T> &variable, T blockMin,
                                            T blockMean, T blockMax)
{
}

/** Add means per step to buffer to make computation of "daily" means easier,
 * i.e. no reading from database required*/
template <>
void JuleaDBDAIWriter::JuleaDBDAIStepValues<double>(Variable<double> &variable,
                                                    double blockMin,
                                                    double blockMean,
                                                    double blockMax)
{
    if (variable.m_Name == m_JuleaCDO.m_TemperatureName)
    {
        m_JuleaCDO.m_HTempMin.push_back(blockMin);
        m_JuleaCDO.m_HTempMean.push_back(blockMean);
        m_JuleaCDO.m_HTempMax.push_back(blockMax);
    }

    if (variable.m_Name == m_JuleaCDO.m_PrecipitationName)
    {
        m_JuleaCDO.m_HPrecMin.push_back(blockMin);
        m_JuleaCDO.m_HPrecMean.push_back(blockMean);
        m_JuleaCDO.m_HPrecMax.push_back(blockMax);
    }
}

template <class T>
void JuleaDBDAIWriter::SetBlockID(Variable<T> &variable)
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

template <class T>
void JuleaDBDAIWriter::PutSyncToJulea(
    Variable<T> &variable, const T *data,
    const typename Variable<T>::Info &blockInfo)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : PutSyncToJulea("
                  << variable.m_Name << ") --- BlockID = " << m_CurrentBlockID
                  << " \n";
    }
    const DataType type = m_IO.InquireVariableType(variable.m_Name);
    // const DataType type = helper::GetDataType<T>();
    T blockMin;
    T blockMax;
    T blockMean;
    uint32_t entryID = 0;
    std::vector<double> testBuffer;
    std::vector<T> testBuffer2;
    double testDouble = 42.0;

    JuleaDBDAISetMinMax(variable, data, blockMin, blockMax, blockMean);

    JuleaDBDAIStepValues(variable, blockMin, blockMean, blockMax);

    auto stepBlockID =
        g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
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

    // TODO: check if there really is no case for global variables to have
    // different features across different blocks
    /* note (23.10.21): the global min/max do not work this way!
        fixed by setting them in 'JuleaDBDAISetMinMax' */
    if (m_WriterRank == 0)
    {
        // TODO: add mean value to DB
        /** updates the variable metadata as there is a new block now */
        m_JuleaDBInteractionWriter.PutVariableMetadataToJulea(
            variable, m_Name, variable.m_Name, m_CurrentStep, m_CurrentBlockID);
    }

    /** put block metadata to DB */
    m_JuleaDBInteractionWriter.PutBlockMetadataToJulea(
        variable, m_Name, variable.m_Name, m_CurrentStep, m_CurrentBlockID,
        blockInfo, blockMin, blockMax, blockMean, entryID);

    // std::cout << "entryID: " << entryID << std::endl;
    /** put data to object store */
    // DBPutVariableDataToJulea(variable, data, m_Name, m_CurrentStep,
    // m_CurrentBlockID);

    // DAIDBPutVariableDataToJulea(variable, data, m_Name, entryID);
    m_JuleaDBInteractionWriter.PutVariableDataToJulea(variable, data, m_Name,
                                                      entryID);
}

template <class T>
void JuleaDBDAIWriter::PutSyncCommon(
    Variable<T> &variable, const typename Variable<T>::Info &blockInfo)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : PutSyncCommon("
                  << variable.m_Name << ") --- Namespace = " << m_Name
                  << " --- CurrentStep = " << m_CurrentStep << "\n";
    }

    PutSyncToJulea(variable, blockInfo.Data, blockInfo);
}

template <class T>
void JuleaDBDAIWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : PutSyncCommon("
                  << variable.m_Name << ") --- Namespace = " << m_Name
                  << " --- CurrentStep = " << m_CurrentStep << "\n";
    }

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());
    PutSyncToJulea(variable, data, blockInfo);
}

template <class T>
void JuleaDBDAIWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
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
void JuleaDBDAIWriter::PerformPutCommon(Variable<T> &variable)
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

#endif /* ADIOS2_ENGINE_JuleaDBDAIWRITER_TCC_ */
