/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONREADER_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONREADER_TCC_

#include "JuleaKVInteractionReader.h"
// #include "JuleaDBDAIInteractionReader.h"
// #include "JuleaMetadata.h"

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

// TODO: filename etc. needed? or is just the entryID sufficient?
template <class T>
void JuleaKVInteractionReader::GetCountFromBlockMetadata(
    const std::string projectNamespace, const std::string fileName,
    const std::string varName, size_t step, size_t block, Dims *count,
    size_t entryID, bool isLocalValue, T *value)
{
    // std::cout << "------ GetCountFromBlockMetadata ----------" << std::endl;
    int err = 0;
    JDBType type;
}

// TODO: remove step, block from parameter list
template <class T>
std::unique_ptr<typename core::Variable<T>::Info>
JuleaKVInteractionReader::GetBlockMetadata(
    const core::Variable<T> &variable, std::string projectNamespace,
    // const std::string nameSpace, size_t step, size_t block,
    size_t entryID) const
{
    // std::cout << "--- DBGetBlockMetadata ---" << std::endl;
    std::unique_ptr<typename core::Variable<T>::Info> info(
        new (typename core::Variable<T>::Info));
    int err = 0;

    return info;
}

} // end namespace interop
} // end namespace adios2

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_TCC_ */
