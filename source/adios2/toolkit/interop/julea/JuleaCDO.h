/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaCDO.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_

// #include "adios2/toolkit/interop/julea/JuleaInteraction.h"
// #include "JuleaMetadata.h"
// #include "adios2/common/ADIOSMacros.h"
// #include "adios2/common/ADIOSTypes.h"

#include "adios2/core/IO.h" // for CreateVar
// #include "adios2/core/Variable.h"
#include "JuleaDAI.h"

#include <julea-dai.h>
#include <julea.h>

#include <list>
#include <string>

#include <stdexcept> // for Intel Compiler

namespace adios2
{
namespace interop
{

class JuleaCDO
// class JuleaCDO : public JuleaDAI
{
public:
    JuleaCDO(helper::Comm const &comm);
    // JuleaCDO();
    // JuleaCDO();
    ~JuleaCDO() = default;

    struct Tag
    {
        // std::string m_projectNamespace; //TODO: check whether sensible to
        // store here...
        std::string m_TagName; // determines the table name
        std::string m_FileName;
        std::string m_VariableName;
        // feature that should be tagged -> could be a stat, could be new
        // feature -> more flexible
        std::string m_FeatureName; // default "mean"

        // only one of them is used but since JULEA has not many types these two
        // are sufficient for now
        size_t m_Threshold_i;
        float m_Threshold_f;

        // JDAIStatistic m_Statistic;
        JDAIOperator m_Operator;       // default ">"
        JDAIGranularity m_Granularity; // default "block level"
    };

    // this is the struct holding the information which functions should be
    // precomputed
    struct Precompute
    {
        // std::string m_projectNamespace; //TODO: check whether sensible to
        // store here... probably better in engine itself
        std::string m_FileName;
        std::string m_VariableName;

        JDAIStatistic m_Statistic;
        JDAIGranularity m_Granularity; // default block level
    };

    std::vector<Tag> m_Tags;
    // std::vector<Precompute> m_Precomputes;

    // stores all functions that should be precomputed.
    // map
    // key: pair of filename and variable name
    // value: list of pairs (statistic and granularity)
    std::map<std::pair<std::string, std::string>,
             std::list<std::pair<JDAIStatistic, JDAIGranularity>>>
        m_Precomputes;

    template <class T>
    void SetMinMax(core::Variable<T> &variable, const T *data, T &blockMin,
                   T &blockMax, size_t currentStep, size_t blockID);

    template <class T>
    void ComputeBlockStat(core::Variable<T> &variable, const T *data,
                          T &blockResult, JDAIStatistic statistic);

    template <class T>
    void ComputeAllBlockStats(core::Variable<T> &variable, const T *data,
                              T &blockMin, T &blockMax, T &blockMean,
                              T &blockSum, T &blockSumSquares, T &blockVar,
                              bool isOriginalFormat);

    template <class T>
    void BufferCDOStats(core::Variable<T> &variable, T blockMin, T blockMax,
                        T blockMean, T blockSum, T blockVar,
                        bool isOriginalFormat);

    // template <class T>
    // void BufferTemperatureOriginal(core::Variable<T> &variable, T blockMin,
    //                                T blockMax);

    // template <class T>
    // void PutCDOStatsToBuffers(core::Variable<T> &variable, T blockMin,
    //                           T blockMean, T blockMax, size_t currentStep,
    //                           size_t blockID);
    // template <class T>
    // void PutCDOStatsToBuffersOriginal(core::Variable<T> &variable, T
    // blockMin,
    //                                   T blockMax, size_t currentStep,
    //                                   size_t blockID);

    void ComputeCoordinatesFromRank(int rank, int &x, int &y);
    void ComputeDailyStats(std::string variableName);
    void ComputeMonthlyStats(std::string variableName);
    void ComputeYearlyStats(std::string variableName);
    void ComputeYearlyLocalStats(std::string variableName);

    int m_numberBlocksX = 0;
    int m_numberBlocksY = 0;

    int m_StoreMonthlyValue = -1;
    int m_StoreYearlyValue = -2;
    double m_NotComputedValue = -424242;

    /** Variables*/
    std::string m_PrecipitationName = "P";
    std::string m_TemperatureName = "T";

    /** Simplified durations */
    size_t m_StepsPerDay = 24;
    size_t m_DaysPerMonth = 30;
    size_t m_StepsPerMonth = 720;
    size_t m_MonthsPerYear = 12;
    size_t m_DaysPerYear = 360;
    size_t m_StepsPerYear = 8640;

    /** Climate indices*/
    size_t m_FrostDays = 0;
    size_t m_SummerDays = 0;
    size_t m_IcingDays = 0;
    size_t m_TropicalNights = 0;
    size_t m_ExtremeTemperatureRange = 0;

    size_t m_PrecipDays1mm = 0;
    size_t m_PrecipDays10mm = 0;
    size_t m_PrecipDays20mm = 0;

    size_t m_SummerDaysTemperatureThreshold = 25;

    /** Climate indices for all years*/
    std::vector<size_t> m_FrostDaysPerYear;
    std::vector<size_t> m_SummerDaysPerYear;
    std::vector<size_t> m_IcingDaysPerYear;
    std::vector<size_t> m_TropicalNightsPerYear;

    std::vector<size_t> m_PrecipDays1mmPerYear;
    std::vector<size_t> m_PrecipDays10mmPerYear;
    std::vector<size_t> m_PrecipDays20mmPerYear;

    // TODO: Variance currently not computed for anything other than blocks
    //  daily temperature min/mean/max
    std::vector<double> m_DTempMin;  // 24 hours
    std::vector<double> m_DTempMean; // 24 hours
    std::vector<double> m_DTempMax;  // 24 hours
    std::vector<double> m_DTempSum;  // 24 hours
    std::vector<double> m_DTempVar;  // 24 hours

    // monthly temperature min/mean/max
    std::vector<double> m_MTempMin;  // 30 days
    std::vector<double> m_MTempMean; // 30 days
    std::vector<double> m_MTempMax;  // 30 days
    std::vector<double> m_MTempSum;  // 30 days
    std::vector<double> m_MTempVar;  // 30 days

    // yearly temperature min/mean/max
    std::vector<double> m_YTempMin;  // 12 months
    std::vector<double> m_YTempMean; // 12 months
    std::vector<double> m_YTempMax;  // 12 months
    std::vector<double> m_YTempSum;  // 12 months
    std::vector<double> m_YTempVar;  // 12 months

    // daily precipitation min/mean/max/sum
    std::vector<double> m_DPrecMin;  // 24 hour
    std::vector<double> m_DPrecMean; // 24 hour
    std::vector<double> m_DPrecMax;  // 24 hour
    std::vector<double> m_DPrecSum;  // 24 hour
    std::vector<double> m_DPrecVar;  // 24 hour

    // monthly precipitation min/mean/max/sum
    std::vector<double> m_MPrecMin;  // 30 days
    std::vector<double> m_MPrecMean; // 30 days
    std::vector<double> m_MPrecMax;  // 30 days
    std::vector<double> m_MPrecSum;  // 30 days
    std::vector<double> m_MPrecVar;  // 30 days

    // yearly precipitation min/mean/max/sum
    std::vector<double> m_YPrecMin;  // 12 months
    std::vector<double> m_YPrecMean; // 12 months
    std::vector<double> m_YPrecMax;  // 12 months
    std::vector<double> m_YPrecSum;  // 12 months
    std::vector<double> m_YPrecVar;  // 12 months

private:
    helper::Comm const &m_Comm; ///< multi-process communicator from Engine
    int m_Verbosity = 5;
    int m_WriterRank;
    int m_SizeMPI;

    // hourly temperature min/mean/max
    std::vector<double> m_HTempMin;  // 1 step
    std::vector<double> m_HTempMean; // 1 step
    std::vector<double> m_HTempMax;  // 1 step
    std::vector<double> m_HTempSum;  // 1 step
    std::vector<double> m_HTempVar;  // 1 step

    // hourly precipitation min/mean/max/sum
    std::vector<double> m_HPrecMin;  // 1 step
    std::vector<double> m_HPrecMean; // 1 step
    std::vector<double> m_HPrecMax;  // 1 step
    std::vector<double> m_HPrecSum;  // 1 step
    std::vector<double> m_HPrecVar;  // 1 step

    // compute frost days: daily min temperature < 0°C
    void ComputeFrostDays(double dailyTempMin);

    // compute tropical nights: daily min temperature > 25°C
    void ComputeTropicalNights(double dailyTempMin);

    // compute summer days: daily max temperature > 25°C
    void ComputeSummerDays(double dailyTempMax);

    // compute icing days: daily max temperature < 0°C
    void ComputeIcingDays(double dailyTempMax);

    // precipitation R > 1mm,10mm,20mm (RR1,RR10,RR20)
    void ComputePrecipDays(double dailyPrecipSum);
};

// #define declare_template_instantiation(T)                                      \
//     extern template void JuleaCDO::SetMinMax(                                  \
//         core::Variable<T> &variable, const T *data, T &blockMin, T &blockMax,  \
//         size_t currentStep, size_t blockID);                     \
//     extern template void JuleaCDO::ComputeBlockStatistics(                     \
//         core::Variable<T> &variable, const T *data, T &blockMin, T &blockMax,  \
//         T &blockMean, T &blockSum, T &blockVar, T &blockStd,                   \
//         size_t currentStep, size_t blockID);                                   \
//     extern template void JuleaCDO::ComputeStepStatistics(                      \
//         core::Variable<T> &variable, T blockMin, T blockMean, T blockMax,      \
//         size_t currentStep, size_t blockID);
// ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
// #undef declare_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_ */
