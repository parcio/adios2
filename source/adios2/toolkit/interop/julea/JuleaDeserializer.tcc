/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaDeserializer.tcc
 *
 *  Created on: July 24, 2019
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaDeserializer_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaDeserializer_TCC_

#include "JuleaDeserializer.h"
#include <iostream>
#include <vector>

#include "adios2/helper/adiosFunctions.h"

namespace adios2
{
namespace interop
{

template <class T>
void JuleaDeserializer::Read(core::Variable<T> &variable, const T *values)
{
    //
}


// /* parse variable information to metadata struct to store in JULEA */
// template <class T>
// void JuleaDeserializer::ParseVariable(core::Variable<T> &variable, const T *data, Metadata
// 	 *metadata)
// {
//     //
// }

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JuleaDeserializer_TCC_ */
