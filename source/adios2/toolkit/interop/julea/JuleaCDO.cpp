/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.cpp
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

// #include "JuleaInteraction.h"
// #include "JuleaInteraction.tcc"
// #include "adios2/helper/adiosFunctions.h" // IsRowMajor

// #include <complex>
// #include <ios>
// #include <iostream>
#include "JuleaCDO.h"
#include <stdexcept>
#include <vector>

#include <cstring> // strlen

namespace adios2
{
namespace interop
{

JuleaCDO::JuleaCDO(helper::Comm const &comm)
{
    // std::cout << "This is the constructor" << std::endl;
}

// daily minimum temperature < 0°cC
void JuleaCDO::computeFrostDays(double dailyTempMin)
{
    if (dailyTempMin < 0)
    {
        m_FrostDays++;
    }
}

void JuleaCDO::computeTropicalNights(double dailyTempMin)
{
    if (dailyTempMin > m_SummerDaysThreshold)
    {
        m_TropicalNights++;
    }
}

void JuleaCDO::computeSummerDays(double dailyTempMax)
{
    if (dailyTempMax > m_SummerDaysThreshold)
    {
        m_SummerDays++;
    }
}

void JuleaCDO::computeIcingDays(double dailyTempMax)
{

    if (dailyTempMax < 0)
    {
        m_IcingDays++;
    }
}

void JuleaCDO::computePrecipDays(double dailyPrecipSum)
{
    if (dailyPrecipSum >= 1)
    {
        m_PrecipDays1mm++;
    }
    if (dailyPrecipSum >= 10)
    {
        m_PrecipDays10mm++;
    }
    if (dailyPrecipSum >= 20)
    {
        m_PrecipDays20mm++;
    }
}


// void JuleaCDO::computeDailyMinimum(const std::string nameSpace, std::string variableName,
//                          uint32_t entryID)
// {
//     // TODO:
// }

/** Gets daily minimum, mean and maximum over all blocks for m_StepsPerDay */
void JuleaCDO::computeDailyStatistics(std::string variableName)
{
    double tmp = 0;
    double dailyMin = 0;
    double dailyMean = 0;
    double dailyMax = 0;

    if (variableName == "T")
    {
        // get daily minimum
        adios2::helper::GetMinMax(m_DTempMin.data(), m_DTempMin.size(),
                                  dailyMin, tmp);
        // get daily maximum
        adios2::helper::GetMinMax(m_DTempMax.data(), m_DTempMax.size(), tmp,
                                  dailyMax);
        dailyMean =
            std::accumulate(m_DTempMean.begin(), m_DTempMean.end(), 0) /
            m_StepsPerDay;
        m_MTempMean.push_back(dailyMean);

        m_MTempMin.push_back(dailyMin);
        computeFrostDays(dailyMin);
        computeTropicalNights(dailyMin);

        m_MTempMax.push_back(dailyMax);
        computeIcingDays(dailyMax);
        computeSummerDays(dailyMax);
    }

    if (variableName == "P")
    {
        adios2::helper::GetMinMax(m_DPrecMin.data(), m_DPrecMin.size(),
                                  dailyMin, tmp);
        adios2::helper::GetMinMax(m_DPrecMax.data(), m_DPrecMax.size(), tmp,
                                  dailyMax);
        dailyMean =
            std::accumulate(m_DPrecMean.begin(), m_DPrecMean.end(), 0) /
            m_StepsPerDay;
        m_MPrecMin.push_back(dailyMin);
        m_MPrecMean.push_back(dailyMean);
        m_MPrecMax.push_back(dailyMax);
        //TODO: do something with the min/mean/avg?

        computePrecipDays(m_DPrecSum);
    }
}

void JuleaCDO::computeMonthlyStatistics(std::string variableName) 
{
    
}

void JuleaCDO::computeYearlyStatistics(std::string variableName) {}

} // end namespace interop
} // end namespace adios
