/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_

#include "adios2/toolkit/interop/julea/JuleaInteraction.h"
// #include "JuleaMetadata.h"
// #include "adios2/common/ADIOSMacros.h"
// #include "adios2/common/ADIOSTypes.h"

#include "adios2/core/IO.h" // for CreateVar
// #include "adios2/core/Variable.h"

#include <julea.h>

#include <string>

#include <stdexcept> // for Intel Compiler

namespace adios2
{
namespace interop
{

class JuleaCDO
{
public:
    JuleaCDO(helper::Comm const &comm);
    // JuleaSerializer(const bool debugMode);
    ~JuleaCDO() = default;

    void precomputeCFD(void);

    // pc = precompute
    void pc_FD(void);

    // compute frost days: daily min temperature < 0°C
    void computeFrostDays(double dailyTempMin);

    // compute tropical nights: daily min temperature > 25°C
    void computeTropicalNights(double dailyTempMin);

    // compute summer days: daily max temperature > 25°C
    void computeSummerDays(double dailyTempMax);

    // compute icing days: daily max temperature < 0°C
    void computeIcingDays(double dailyTempMax);

    // precipitation R > 1mm (RR1)
    void computePrecipDays(double dailyPrecipSum);
    // void computePrecipDays1mm(double dailyPrecipMin);
    // void computePrecipDays10mm(double dailyPrecipMin);
    // void computePrecipDays20mm(double dailyPrecipMin);

    // precipitation R > 1 (RR1)
    // void computeWetDays(double dailyPrecipMin);


    // compute dailyMinTemperature; dailyMinPrecipitation

    // is computed every 24 steps
    // void computeDailyMinimum(const std::string nameSpace,
    //                          std::string variableName, uint32_t entryID);

    void computeDailyStatistics(std::string variableName);
    void computeMonthlyStatistics(std::string variableName);
    void computeYearlyStatistics(std::string variableName);

    std::string m_PrecipitationName = "P";
    std::string m_TemperatureName = "T";

    // set duration of day; currently 24 steps = 24 h
    size_t m_StepsPerDay = 24;

    size_t m_DaysPerMonth = 30;
    size_t m_StepsPerMonth = 720;

    size_t m_MonthsPerYear = 12;
    size_t m_DaysPerYear = 360;
    size_t m_StepsPerYear = 8640;

    bool m_computeCFD = false;
    bool m_computeFD = false;

    // #define declare_type(T)                                                        \
//     T *currentDailyMinTemperature;                                  \
//     T *currentDailyMaxTemperature;                                  \
//     void PutSomething(Variable<T> &, T *) final;
    //     ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
    // #undef declare_type

    size_t m_FrostDays = 0;
    size_t m_SummerDays = 0;
    size_t m_SummerDaysThreshold = 25;
    size_t m_IcingDays = 0;
    size_t m_TropicalNights = 0;

    size_t m_PrecipDays1mm = 0;
    size_t m_PrecipDays10mm = 0;
    size_t m_PrecipDays20mm = 0;
    
    // daily temperature min/mean/max
    std::vector<double> m_DTempMin;  // 24 hours
    std::vector<double> m_DTempMean; // 24 hours
    std::vector<double> m_DTempMax;  // 24 hours

    // monthly temperature min/mean/max
    std::vector<double> m_MTempMin;  // 30 days
    std::vector<double> m_MTempMean; // 30 days
    std::vector<double> m_MTempMax;  // 30 days

    // yearly temperature min/mean/max
    std::vector<double> m_YTempMin;  // 12 months
    std::vector<double> m_YTempMean; // 12 months
    std::vector<double> m_YTempMax;  // 12 months

    // daily precipitation min/mean/max/sum
    std::vector<double> m_DPrecMin;  // 24 hour
    std::vector<double> m_DPrecMean; // 24 hour
    std::vector<double> m_DPrecMax;  // 24 hour
    std::vector<double> m_DPrecSum;  // 24 hour

    // monthly precipitation min/mean/max/sum
    std::vector<double> m_MPrecMin;  // 30 days
    std::vector<double> m_MPrecMean; // 30 days
    std::vector<double> m_MPrecMax;  // 30 days
    std::vector<double> m_MPrecSum;  // 30 days

    // yearly precipitation min/mean/max/sum
    std::vector<double> m_YPrecMin;  // 12 months
    std::vector<double> m_YPrecMean; // 12 months
    std::vector<double> m_YPrecMax;  // 12 months
    std::vector<double> m_YPrecSum;  // 12 months
    
private:
};

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_ */
