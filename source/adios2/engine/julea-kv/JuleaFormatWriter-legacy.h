/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATWRITER_LEGACY_H_
#define ADIOS2_ENGINE_JULEAFORMATWRITER_LEGACY_H_

#include "JuleaMetadata.h"
#include "JuleaKVWriter.h"
// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
void ParseVariableToMetadataStruct(Variable<T> &variable, const T *data,
                                   Metadata *metadata);
template <class T>
void ParseVariableToMetadataStruct(Variable<T> &variable,
                                   const typename Variable<T>::Info &blockInfo,
                                   Metadata *metadata);

template <class T>
void ParseVariableToBSON(const typename Variable<T>::Info &blockInfo,
                         bson_t *bson_meta_data);

template <class T>
void ParseVarTypeToBSON(Variable<T> &variable, const T *data,
                        Metadata *metadata);
template <class T>
void ParseVarTypeToBSON(Variable<T> &variable,
                        const typename Variable<T>::Info &blockInfo,
                        Metadata *metadata);

#define declare_template_instantiation(T)                                      \
    extern template void ParseVariableToMetadataStruct(                        \
        Variable<T> &variable, const T *data, Metadata *metadata);             \
    extern template void ParseVariableToMetadataStruct(                        \
        Variable<T> &variable, const typename Variable<T>::Info &blockInfo,    \
        Metadata *metadata);                                                   \
    extern template void ParseVariableToBSON(                                  \
        Variable<T> &variable, const typename Variable<T>::Info &blockInfo,    \
        bson_t *bson_meta_data);                                               \
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAFORMATWRITER_LEGACY_H_ */
