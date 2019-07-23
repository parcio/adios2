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
// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

template <class T>
void parse_variable_type(Variable<T> &variable,
                         const typename Variable<T>::Info &blockInfo,
                         Metadata *metadata);
template <class T>
void parse_variable_type(Variable<T> &variable, const T *data,
                         Metadata *metadata);

template <>
void parse_variable_type<int8_t>(Variable<int8_t> &variable, const int8_t *data,
                         Metadata *metadata);
template <>
void parse_variable_type<int16_t>(Variable<int16_t> &variable, const int16_t *data,
                         Metadata *metadata);
template <>
void parse_variable_type<int32_t>(Variable<int32_t> &variable, const int32_t *data,
                         Metadata *metadata);
template <>
void parse_variable_type<int64_t>(Variable<int64_t> &variable, const int64_t *data,
                         Metadata *metadata);
template <>
void parse_variable_type<float>(Variable<float> &variable, const float *data,
                         Metadata *metadata);
} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAFORMATWRITER_H_ */
