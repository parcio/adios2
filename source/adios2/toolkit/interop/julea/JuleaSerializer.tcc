/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.tcc
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_TCC_

#include "JuleaSerializer.h"
#include <iostream>
#include <vector>

#include "adios2/helper/adiosFunctions.h"

namespace adios2
{
namespace interop
{

template <class T>
void JuleaDBDAISetMinMax(core::Variable<T> &variable, const T *data,
                         T &blockMin, T &blockMax, T &blockMean,
                         size_t currentStep, size_t currentBlockID)
{
    T min = 0;
    T max = 0;
    T stepMin = 0;
    T stepMax = 0;
    T sum = 0;
    T mean = 0;

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

    // TODO: check whether this is incorrect
    // there may be some cases where this is not working
    /*  to initialize the global min/max, they are set to the
        first min/max for the first block of the first step */
    if ((currentStep == 0) && (currentBlockID == 0))
    {
        variable.m_Min = min;
        variable.m_Max = max;
        stepMin = min;
        stepMax = max;
    }

    /* reduce only necessary if more than one process*/
    // if (m_WriterRank > 0)
    // {
    //     m_Comm.Reduce(&blockMin, &stepMin, 1, helper::Comm::Op::Min, 0);
    //     m_Comm.Reduce(&blockMax, &stepMax, 1, helper::Comm::Op::Max, 0);
    // }

    if (stepMin < variable.m_Min)
    {
        // std::cout << "updated global min from " << variable.m_Min << " to "
        //   << min << std::endl;
        variable.m_Min = stepMin;
    }

    if (stepMax > variable.m_Max)
    {
        // std::cout << "updated global max from "  << variable.m_Max << " to "
        // << max << std::endl;
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
void JuleaDBDAISetMinMax<std::string>(core::Variable<std::string> &variable,
                                      const std::string *data,
                                      std::string &blockMin,
                                      std::string &blockMax,
                                      std::string &blockMean,
                                      size_t currentStep, size_t currentBlockID)
{
    // TODO implement?
}

template <>
void JuleaDBDAISetMinMax<std::complex<float>>(
    core::Variable<std::complex<float>> &variable,
    const std::complex<float> *data, std::complex<float> &blockMin,
    std::complex<float> &blockMax, std::complex<float> &blockMean,
    size_t currentStep, size_t currentBlockID)
{
    // TODO implement?
}

template <>
void JuleaDBDAISetMinMax<std::complex<double>>(
    core::Variable<std::complex<double>> &variable,
    const std::complex<double> *data, std::complex<double> &blockMin,
    std::complex<double> &blockMax, std::complex<double> &blockMean,
    size_t currentStep, size_t currentBlockID)
{
    // TODO implement?
}

template <class T>
void JuleaSerializer::Write(core::Variable<T> &variable, const T *values)
{
    //
}

/* parse variable information to metadata struct to store in JULEA */
template <class T>
void JuleaSerializer::ParseVariable(core::Variable<T> &variable, const T *data,
                                    Metadata *metadata)
{
    //
}

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_TCC_ */
