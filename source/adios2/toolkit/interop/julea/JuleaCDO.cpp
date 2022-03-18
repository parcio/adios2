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

void precomputeCFD(void)
{
    // TODO:
}
void computeDailyMinimum(const std::string nameSpace, std::string variableName,
                         uint32_t entryID)
{
    // TODO:
}

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
        adios2::helper::GetMinMax(m_DBTempMin.data(), m_DBTempMin.size(),
                                  dailyMin, tmp);
        // get daily maximum
        adios2::helper::GetMinMax(m_DBTempMax.data(), m_DBTempMax.size(), tmp,
                                  dailyMax);
        dailyMean =
            std::accumulate(m_DBTempMean.begin(), m_DBTempMean.end(), 0) /
            m_StepsPerDay;

        m_MBTempMin.push_back(dailyMin);
        m_MBTempMean.push_back(dailyMean);
        m_MBTempMax.push_back(dailyMax);
    }

    if (variableName == "P")
    {
        adios2::helper::GetMinMax(m_DBPrecMin.data(), m_DBPrecMin.size(),
                                  dailyMin, tmp);
        adios2::helper::GetMinMax(m_DBPrecMax.data(), m_DBPrecMax.size(), tmp,
                                  dailyMax);
        dailyMean =
            std::accumulate(m_DBPrecMean.begin(), m_DBPrecMean.end(), 0) /
            m_StepsPerDay;
        m_MBPrecMin.push_back(dailyMin);
        m_MBPrecMean.push_back(dailyMean);
        m_MBPrecMax.push_back(dailyMax);
    }
}

void JuleaCDO::computeMonthlyStatistics(std::string variableName) {}

} // end namespace interop
} // end namespace adios
