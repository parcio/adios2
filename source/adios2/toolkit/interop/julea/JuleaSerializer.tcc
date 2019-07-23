/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.tcc
 *
 *  Created on: July 23, 2019
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_TCC_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_TCC_

#include "JuleaSerializer.h"
#include <iostream>
#include <vector>

#include "adios2/helper/adiosFunctions.h"

namespace adios2
{
namespace interop
{

template <class T>
void JuleaSerializer::Write(core::Variable<T> &variable, const T *values)
{
    //
}

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEASERIALIZER_TCC_ */
