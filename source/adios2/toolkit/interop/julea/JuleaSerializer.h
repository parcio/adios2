/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_H_

#include "JuleaMetadata.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop
// namespace!
#include "adios2/common/ADIOSMacros.h"
#include "adios2/common/ADIOSTypes.h"
#include "adios2/core/IO.h" // for CreateVar
#include "adios2/core/Variable.h"

#include <julea.h>

#include <string>

#include <stdexcept> // for Intel Compiler

namespace adios2
{
namespace interop
{

class JuleaSerializer
{

public:
    /**
     * Unique constructor
     * @param debugMode true: extra exception checks
     */
    JuleaSerializer(const bool debugMode);

    void ParseParameters(core::IO &io);
    void Init();

    void WriteAdiosSteps();
    void ReadAllVariables(core::IO &io);
    void ReadVariables(unsigned int ts, core::IO &io);

    template <class T>
    void Write(core::Variable<T> &variable, const T *values);

    // template <class T>
    // void ParseVariable(core::Variable<T> &variable, const T *data,
    // engine::Metadata *metadata);
    template <class T>
    void ParseVariable(core::Variable<T> &variable, const T *data,
                       Metadata *metadata);

    void Close();

    void PrintMiniPenguin();
    void PrintPenguinFamily();
    void PrintLargePenguin();

    unsigned int m_CurrentAdiosStep = 0;

private:
    const bool m_DebugMode;
    bool m_WriteMode = false;

    int m_CommRank = 0;
    int m_CommSize = 1;
};

template <class T>
void JuleaDBDAISetMinMax(core::Variable<T> &variable, const T *data,
                         T &blockMin, T &blockMax, T &blockMean,
                         size_t currentStep, size_t currentBlockID);

// Explicit declaration of the public template methods
#define declare_template_instantiation(T)                                      \
    extern template void JuleaSerializer::Write(core::Variable<T> &variable,   \
                                                const T *value);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

// Explicit declaration of the public template methods
#define declare_template_instantiation(T)                                      \
    extern template void JuleaSerializer::ParseVariable(                       \
        core::Variable<T> &variable, const T *data, Metadata *metadata);       \
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_H_ */
