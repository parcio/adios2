/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATWRITER_H_
#define ADIOS2_ENGINE_JULEAFORMATWRITER_H_

#include "JuleaMetadata.h"
#include "JuleaWriter.h"
// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
void ParseVariable(Variable<T> &variable, const T *data, Metadata *metadata);

// #define declare_template_instantiation(T)                                      \
//     extern template void ParseVariable(Variable<T> &variable, const T *data, \
//                    Metadata *metadata);              \
// ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
// #undef declare_template_instantiation

template <class T>
void ParseVariableType(Variable<T> &variable,
                       const typename Variable<T>::Info &blockInfo,
                       Metadata *metadata);
template <class T>
void ParseVariableType(Variable<T> &variable, const T *data,
                       Metadata *metadata);

template <>
void ParseVariableType<int8_t>(Variable<int8_t> &variable, const int8_t *data,
                               Metadata *metadata);
template <>
void ParseVariableType<int16_t>(Variable<int16_t> &variable,
                                const int16_t *data, Metadata *metadata);
template <>
void ParseVariableType<int32_t>(Variable<int32_t> &variable,
                                const int32_t *data, Metadata *metadata);
template <>
void ParseVariableType<int64_t>(Variable<int64_t> &variable,
                                const int64_t *data, Metadata *metadata);
template <>
void ParseVariableType<float>(Variable<float> &variable, const float *data,
                              Metadata *metadata);
} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAFORMATWRITER_H_ */
