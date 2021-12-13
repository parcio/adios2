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
    size_t m_DayIntervall = 24;
    bool m_computeCFD = false;
    bool m_computeFD = false;

    // #define declare_type(T)                                                        \
//     T *currentDailyMinTemperature;                                  \
//     T *currentDailyMaxTemperature;                                  \
//     void PutSomething(Variable<T> &, T *) final;
    //     ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
    // #undef declare_type

    void precomputeCFD(void);

    // compute dailyMinTemperature; dailyMinPrecipitation
    //
    void computeDailyMinimum(const std::string nameSpace,
                             std::string variableName, uint32_t entryID);

private:
};

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_CDOCLIMATEINDECES_H_ */
