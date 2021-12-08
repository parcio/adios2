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
#include "JuleaDbInteractionWriter.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop namespace!
// #include "adios2/common/ADIOSMacros.h"
// #include "adios2/common/ADIOSTypes.h"
// #include "adios2/core/IO.h" // for CreateVar
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

    JSemantics m_JuleaSemantics;
    //TODO: batch?

    std::string m_JuleaNamespace = "adios2";
    std::string m_JuleaOSNamespace = "variableblocks"; //TODO: separation necessary into objects from kv engine and from db engine?
    std::string m_JuleaBackendDB = "DB";
    std::string m_JuleaBackendKV = "KV";
    // std::string m_VariableTableName; in DBInteractionWriter

    // for both db and kv engine
    template <class T> void PutVariableDataToJulea(Variable<T> &variable, const T *data, const std::string nameSpace, uint32_t entryID);
    //TODO: GetVariableDataFromJulea

protected:

private:

    //something private
};


} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAINTERACTION_H_ */
