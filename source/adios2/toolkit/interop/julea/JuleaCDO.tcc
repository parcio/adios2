/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaCDO.tcc
 *
 *  Created on: March 23, 2022
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEACDO_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEACDO_TCC_

// #include "adios2/helper/adiosFunctions.h" // IsRowMajor

// #include <complex>
// #include <ios>
// #include <iostream>
#include "JuleaCDO.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include <mpi.h>
#include <stdexcept>
#include <vector>

// #include <cstring> // strlen

namespace adios2
{
namespace interop
{

template <class T>
void JuleaCDO::SetMinMax(core::Variable<T> &variable, const T *data,
                         T &blockMin, T &blockMax, size_t currentStep,
                         size_t blockID)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JuleaCDO (" << m_WriterRank << ") : SetMinMax()\n";
    }

    T min = 0;
    T max = 0;
    // T sum = 0;
    // T mean = 0;

    T stepMin = 0;
    T stepMax = 0;
    // T stepMean = 0;

    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    adios2::helper::GetMinMax(data, number_elements, min, max);

    // for (size_t i = 0; i < number_elements; ++i)
    // {
    //     sum += data[i];
    // }

    // TODO: cast to T ?
    // mean = sum / (double)number_elements;

    blockMin = min;
    blockMax = max;
    // blockMean = mean;

    // TODO: check whether this is incorrect
    // there may be some cases where this is not working
    /*  to initialize the global min/max, they are set to the
        first min/max for the first block of the first step */
    if ((currentStep == 0) && (blockID == 0))
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
        // m_Comm.Reduce(&blockMean, &stepMean, 1, helper::Comm::Op::Sum, 0);
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
    // blockMean = stepMean / m_Comm.Size();

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

    m_ExtremeTemperatureRange = variable.m_Max - variable.m_Min;

    if (false)
    {
        std::cout << "min: " << min << std::endl;
        std::cout << "global min: " << variable.m_Min << std::endl;
        std::cout << "max: " << max << std::endl;
        std::cout << "global max: " << variable.m_Max << std::endl;
    }
}

template <>
void JuleaCDO::SetMinMax<std::string>(core::Variable<std::string> &variable,
                                      const std::string *data,
                                      std::string &blockMin,
                                      std::string &blockMax, size_t currentStep,
                                      size_t blockID)
{
    // TODO implement?
}

template <>
void JuleaCDO::SetMinMax<std::complex<float>>(
    core::Variable<std::complex<float>> &variable,
    const std::complex<float> *data, std::complex<float> &blockMin,
    std::complex<float> &blockMax, size_t currentStep, size_t blockID)
{
    // TODO implement?
}

template <>
void JuleaCDO::SetMinMax<std::complex<double>>(
    core::Variable<std::complex<double>> &variable,
    const std::complex<double> *data, std::complex<double> &blockMin,
    std::complex<double> &blockMax, size_t currentStep, size_t blockID)
{
    // TODO implement?
}

/**
 * Function to compute additional statistics on their own. Useful for evaluation
 * for detailled overhead assessment. Computing time, memory consumption, db
 * size of single statistic vs. all of them
 */
template <class T>
void JuleaCDO::ComputeBlockStat(core::Variable<T> &variable, const T *data,
                                T &blockResult, JDAIStatistic statistic)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JuleaCDO (" << m_WriterRank
                  << ") : ComputeBlockStats()\n";
    }
    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);

    switch (statistic)
    {
    // TODO: not really necessary since already in ADIOS format, right?!
    //  case J_DAI_STAT_MIN:
    //      //DoStuff();
    //      break;
    //  case J_DAI_STAT_MAX:
    //      //DoStuff();
    //      break;
    case J_DAI_STAT_MEAN:
        for (size_t i = 0; i < number_elements; ++i)
        {
            blockResult += data[i];
        }
        blockResult = blockResult / (double)number_elements;
        break;

    case J_DAI_STAT_SUM:
        for (size_t i = 0; i < number_elements; ++i)
        {
            blockResult += data[i];
        }
        break;

    case J_DAI_STAT_VAR:
        T blockMean;
        T blockSumSquares;

        // Compute mean then sum squares
        for (size_t i = 0; i < number_elements; ++i)
        {
            blockResult += data[i];
        }
        blockMean = blockResult / (double)number_elements;

        for (size_t i = 0; i < number_elements; ++i)
        {
            blockSumSquares += std::pow(data[i] - blockMean, 2);
        }
        blockResult = blockSumSquares / number_elements;
        break;
    }
}

template <>
void JuleaCDO::ComputeBlockStat<std::string>(
    core::Variable<std::string> &variable, const std::string *data,
    std::string &blockResult, JDAIStatistic statistic)
{
}

template <>
void JuleaCDO::ComputeBlockStat<std::complex<float>>(
    core::Variable<std::complex<float>> &variable,
    const std::complex<float> *data, std::complex<float> &blockResult,
    JDAIStatistic statistic)
{
}

template <>
void JuleaCDO::ComputeBlockStat<std::complex<double>>(
    core::Variable<std::complex<double>> &variable,
    const std::complex<double> *data, std::complex<double> &blockResult,
    JDAIStatistic statistic)
{
}

// blockSumSquares = sum of the "fehlerquadrate"
// blockVar = block variance
template <class T>
void JuleaCDO::ComputeAllBlockStats(core::Variable<T> &variable, const T *data,
                                    T &blockMin, T &blockMax, T &blockMean,
                                    T &blockSum, T &blockSumSquares,
                                    T &blockVar, bool isOriginalFormat,
                                    std::string fileName)
{
    if (m_Verbosity == 5)
    {
        // std::cout << "JuleaCDO (" << m_WriterRank
                //   << ") : ComputeAllBlockStats()\n";
    }

    auto numberElements = adios2::helper::GetTotalSize(variable.m_Count);
    adios2::helper::GetMinMax(data, numberElements, blockMin, blockMax);
    // adios2::helper::GetMinMax(data, numberElements, tmpBlockMin, tmpBlockMax);
    /** accumulate does not work with type T data, so need to do it by hand */
    // blockSum = std::accumulate(data.begin(), data.end(), 0)

    if (!isOriginalFormat)
    {
        for (size_t i = 0; i < numberElements; ++i)
        {
            blockSum += data[i];
            // TaggingDataIfRequired(data[i],fileName, variable.m_Name);
        }

        blockMean = blockSum / (double)numberElements;

        for (size_t i = 0; i < numberElements; ++i)
        {
            blockSumSquares += std::pow(data[i] - blockMean, 2);
        }

        blockVar = blockSumSquares / numberElements;
    }
}

template <>
void JuleaCDO::ComputeAllBlockStats<std::string>(
    core::Variable<std::string> &variable, const std::string *data,
    std::string &blockMin, std::string &blockMax, std::string &blockMean,
    std::string &blockSum, std::string &blockSumSquares, std::string &blockVar,
    bool isOriginalFormat, std::string fileName)
{
}

template <>
void JuleaCDO::ComputeAllBlockStats<std::complex<float>>(
    core::Variable<std::complex<float>> &variable,
    const std::complex<float> *data, std::complex<float> &blockMin,
    std::complex<float> &blockMax, std::complex<float> &blockMean,
    std::complex<float> &blockSum, std::complex<float> &blockSumSquares,
    std::complex<float> &blockVar, bool isOriginalFormat, std::string fileName)
{
}

template <>
void JuleaCDO::ComputeAllBlockStats<std::complex<double>>(
    core::Variable<std::complex<double>> &variable,
    const std::complex<double> *data, std::complex<double> &blockMin,
    std::complex<double> &blockMax, std::complex<double> &blockMean,
    std::complex<double> &blockSum, std::complex<double> &blockSumSquares,
    std::complex<double> &blockVar, bool isOriginalFormat, std::string fileName)
{
}

template <class T>
void JuleaCDO::BufferCDOStats(core::Variable<T> &variable, T blockMin,
                              T blockMax, T blockMean, T blockSum, T blockVar,
                              bool isOriginalFormat)
{
}

/** Add means per step to buffer to make computation of "daily" means easier,
 * i.e. no reading from database required
 * Mean value will be ignored when original format is written
 */
template <>
void JuleaCDO::BufferCDOStats<double>(core::Variable<double> &variable,
                                      double blockMin, double blockMax,
                                      double blockMean, double blockSum,
                                      double blockVar, bool original)
{
    if (variable.m_Name == m_TemperatureName)
    {
        m_HTempMin.push_back(blockMin);
        m_HTempMax.push_back(blockMax);
        if (!original)
        {
            m_HTempMean.push_back(blockMean);
            m_HTempSum.push_back(blockSum);
            m_HTempVar.push_back(blockVar);
        }
    }

    if (variable.m_Name == m_PrecipitationName)
    {
        m_HPrecMin.push_back(blockMin);
        m_HPrecMax.push_back(blockMax);
        if (!original)
        {
            m_HPrecMean.push_back(blockMean);
            m_HPrecSum.push_back(blockSum);
            m_HPrecVar.push_back(blockVar);
        }
    }
}



} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEACDO_TCC_ */
