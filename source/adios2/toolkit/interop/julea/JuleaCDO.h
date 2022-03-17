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
    // set duration of day; currently 24 steps = 24 h
    size_t m_StepsPerDay = 24;

    size_t m_DaysPerMonth = 30;
    size_t m_StepsPerMonth = 720;

    size_t m_MonthsPerYear = 12;
    size_t m_DaysPerYear = 365;
    size_t m_StepsPerYear = 8640;

    bool m_computeCFD = false;
    bool m_computeFD = false;

    // #define declare_type(T)                                                        \
//     T *currentDailyMinTemperature;                                  \
//     T *currentDailyMaxTemperature;                                  \
//     void PutSomething(Variable<T> &, T *) final;
    //     ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
    // #undef declare_type

    void precomputeCFD(void);

    //pc = precompute
    void pc_FD(void);

    //compute frost days: daily min temperature < 0°C
    void computeFrostDays(void);

    //compute summer days: daily max temperature > 25°C
    void computeSummerDays(void);

    //compute icing days: daily max temperature < 0°C
    void computeIcingDays(void);

    //compute tropical nights: daily min temperature > 25°C
    void computeTropicalNights(void);

    // compute dailyMinTemperature; dailyMinPrecipitation

    // is computed every 24 steps
    void computeDailyMinimum(const std::string nameSpace,
                             std::string variableName, uint32_t entryID);

    void computeDailyMin(void);
private:
};

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_ */
