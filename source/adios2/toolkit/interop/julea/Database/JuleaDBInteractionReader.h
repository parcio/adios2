/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_H_

// #include "JuleaMetadata.h"
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

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

class JuleaDBInteractionReader : public JuleaInteraction
{

public:
    std::string m_JuleaNamespace = "adios2";
    // std::string m_VariableTableName; in DBInteractionWriter

// void SetMinMaxValueFields(std::string *minField, std::string *maxField,
//                           std::string *valueField, std::string *meanField,
//                           const adios2::DataType varType);
private:
    // something private
};
} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_H_ */
