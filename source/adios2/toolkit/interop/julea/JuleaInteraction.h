/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAINTERACTION_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAINTERACTION_H_

// #include "JuleaMetadata.h"
// #include "JuleaDbInteractionWriter.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop
// namespace! #include "adios2/common/ADIOSMacros.h" #include
// "adios2/common/ADIOSTypes.h" #include "adios2/core/IO.h" // for CreateVar
// #include "adios2/core/Variable.h"

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
#include <julea-db.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

#include "adios2/core/VariableBase.h"
// #include "JuleaMetadata.h"
// #include "JuleaDbInteractionWriter.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop
// namespace!
#include "adios2/common/ADIOSMacros.h"
#include "adios2/common/ADIOSTypes.h"
#include "adios2/core/IO.h" // for CreateVar
#include "adios2/core/Variable.h"

namespace adios2
{
namespace interop
{

class JuleaInteraction
{

public:
    /**
     * Unique constructor
     * @param debugMode true: extra exception checks
     */
    JuleaInteraction();
    // JuleaInteraction(const bool debugMode);
    // JuleaSerializer(const bool debugMode);

    void PrintMiniPenguin();
    void PrintPenguinFamily();
    void PrintLargePenguin();

    unsigned int m_CurrentAdiosStep = 0;
    const bool m_DebugMode;
    bool m_WriteMode = false;
    bool m_ReadMode = false;

    int m_CommRank = 0;
    int m_CommSize = 1;

    JSemantics *m_JuleaSemantics;
    // JSemantics *m_JuleaSemantics = J_SEMANTICS_TEMPLATE_DEFAULT;
    // TODO: batch?

    std::string m_JuleaNamespace = "adios2";
    std::string m_JuleaOSNamespace =
        "variableblocks"; // TODO: separation necessary into objects from kv
                          // engine and from db engine?
    std::string m_JuleaBackendDB = "DB";
    std::string m_JuleaBackendKV = "KV";
    // std::string m_VariableTableName; in DBInteractionWriter

    // for both db and kv engine
    template <class T>
    void PutVariableDataToJulea(core::Variable<T> &variable, const T *data,
                                const std::string nameSpace, uint32_t entryID);
    // TODO: GetVariableDataFromJulea

protected:
    int test = 42;

private:
    // something private
    int another_test = 42;
};

#define declare_template_instantiation(T)                                      \
    extern template void PutVariableDataToJulea(                               \
        core::Variable<T> &variable, const T *data,                            \
        const std::string nameSpace, uint32_t entryID);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAINTERACTION_H_ */
