/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaDeserializer.h
 *
 *  Created on: July 24, 2019
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaDeserializer_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaDeserializer_H_

#include "JuleaMetadata.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop namespace!
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

class JuleaDeserializer
{

public:
    /**
     * Unique constructor
     * @param debugMode true: extra exception checks
     */
    JuleaDeserializer(const bool debugMode);

    void ParseParameters(core::IO &io);
    void Init();

    // void WriteAdiosSteps();
    void ReadAllVariables(core::IO &io);
    void ReadVariables(unsigned int ts, core::IO &io);

    template <class T>
    void Read(core::Variable<T> &variable, const T *values);

    template <class T>
    void ParseVariable(core::Variable<T> &variable, const T *data, Metadata *metadata);

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

// Explicit declaration of the public template methods
#define declare_template_instantiation(T)                                      \
    extern template void JuleaDeserializer::Read(core::Variable<T> &variable,   \
                                                const T *value);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaDeserializer_H_ */
