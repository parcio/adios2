/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAREADER_TCC_
#define ADIOS2_ENGINE_JULEAREADER_TCC_

#include "JuleaClientLogic-legacy.h"
#include "JuleaFormatReader.h"
#include "JuleaInteractionReader.h"
#include "JuleaKVReader.h"

#include <iostream>
// #include <fstream>
// #include <string>

namespace adios2
{
namespace core
{
namespace engine
{

template <>
void JuleaKVReader::GetSyncCommon(Variable<std::string> &variable,
                                  std::string *data)
{
    g_autoptr(JBatch) batch = NULL;
    g_autoptr(JSemantics) semantics;

    gboolean use_batch = TRUE;
    semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    batch = j_batch_new(semantics);

    gchar *name_space = strdup(m_Name.c_str());
    Metadata *metadata = g_slice_new(Metadata);
    metadata->name = g_strdup(variable.m_Name.c_str());

    std::cout << "Julea Reader " << m_ReaderRank
              << " Reached Get Sync Common (String, String) " << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank << " Namespace of variable "
              << m_Name << std::endl;
    // GetVarDataFromJulea(name_space, metadata->name, metadata->data_size,
    //                     (void *)(data), batch);

    // FIXME: additional metadata infos as "IsReadAsJoined" need to be stored in
    // ADIOS

    // std::cout << "JULEA ENGINE: GetSyncCommon (string data)" << std::endl;
    variable.m_Data = data;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << "     GetSync("
                  << variable.m_Name << ")\n";
    }
    j_batch_unref(batch);
}

// inline needed? is in skeleton-engine
template <class T>
void JuleaKVReader::GetSyncCommon(Variable<T> &variable, T *data)
{

    std::cout << "Julea Reader " << m_ReaderRank
              << " Reached Get Sync Common (T, T)" << std::endl;
    std::cout << "Julea Reader " << m_ReaderRank << " Namespace of variable "
              << m_Name << std::endl;

    auto bsonNames = bson_new();
    bson_t *bsonMetadata = bson_new();
    auto nameSpace = m_Name;
    unsigned int varCount = 0;
    long unsigned int dataSize = 0;

    /* all the additional metadata which is not used in InitVariables has to be
     * read again */
    GetVariableMetadataFromJulea(variable, bsonMetadata, nameSpace, &dataSize);
    GetVariableDataFromJulea(variable, data, nameSpace, dataSize);
}

template <class T>
void JuleaKVReader::GetDeferredCommon(Variable<T> &variable, T *data)
{
    // std::cout << "JULEA ENGINE: GetDeferredCommon" << std::endl;
    // returns immediately
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << "     GetDeferred("
                  << variable.m_Name << ")\n";
    }
    m_NeedPerformGets = true;
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif // ADIOS2_ENGINE_JULEAREADER_TCC_
