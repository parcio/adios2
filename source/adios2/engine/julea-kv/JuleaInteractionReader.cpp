/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaInteractionReader.h"
#include "JuleaMetadata.h"

#include <julea-config.h>

#include <assert.h>
#include <bson.h>
#include <glib.h>
#include <string.h>

#include <iostream>
#include <julea-internal.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

/** -------------------------------------------------------------------------**/
/** -------------- TESTING GENERIC FUNCTIONS --------------------------------**/
/** -------------------------------------------------------------------------**/


/** ------------------------- DATA ------------------------------------------**/



#define declare_template_instantiation(T)                                      \
    template void GetVariableDataFromJulea(                               \
        Variable<T> &variable, const T *data, const char *name_space);         \
    template void GetVariableMetadataFromJulea(                           \
        Variable<T> &variable, bson_t *bsonMetadata, const char *name_space);  \
    template void GetAttributeDataFromJulea(                              \
        Attribute<T> &attribute, const T *data, const char *nameSpace);        \
    template void GetAttributeMetadataFromJulea(                          \
        Attribute<T> &attribute, bson_t *bsonMetadata, const char *nameSpace); \
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2
