/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONWRITER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONWRITER_H_

// #include "JuleaMetadata.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop namespace!
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

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


class JuleaKVInteractionReader : public JuleaInteraction
{

public:
private:

    //something private
};

// template <class T>
    // void JuleaDBDAISetMinMax(core::Variable<T> &variable, const T *data, T &blockMin,
                        //   T &blockMax, T &blockMean, size_t currentStep,
                        //   size_t currentBlockID);

// Explicit declaration of the public template methods
// #define declare_template_instantiation(T)                                      \
//     extern template void JuleaSerializer::Write(core::Variable<T> &variable,   \
//                                                 const T *value);
// ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
// #undef declare_template_instantiation

// // Explicit declaration of the public template methods
// #define declare_template_instantiation(T)                                      \
//     extern template void JuleaSerializer::ParseVariable(core::Variable<T> &variable,   \
//                                                 const T *data, Metadata *metadata); \
// ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
// #undef declare_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONWRITER_H_ */
