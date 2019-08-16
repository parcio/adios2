/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 02, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATREADER_H_
#define ADIOS2_ENGINE_JULEAFORMATREADER_H_

#include "JuleaKVReader.h"
#include "JuleaMetadata.h"
// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

// void DefineAttributeInInit(core::IO *io, const std::string varName, int type,
// bool IsSingleValue);
// template <class T>
// void DefineAttributeInInit(core::IO *io, const std::string attrName, T *data,
//                            int type, bool IsSingleValue, size_t
//                            numberElements);

void GetVariableMetadataForInitFromBSON(const std::string nameSpace,
                                        const std::string varName,
                                        bson_t *bsonMetadata, int *type,
                                        Dims *shape, Dims *start, Dims *count,
                                        bool *constantDims);

void DefineVariableInInit(core::IO *io, const std::string varName, int type,
                          Dims shape, Dims start, Dims count,
                          bool constantDims);

template <class T>
void ParseVariableFromBSON(Variable<T> &variable, bson_t *bsonMetadata,
                           const std::string nameSpace,
                           long unsigned int *dataSize);

template <class T>
void ParseVarTypeFromBSON(Variable<T> &variable, bson_iter_t *b_iter);

template <class T>
void SetMinMax(Variable<T> &variable, const T *data);

void ParseAttributeFromBSON(const std::string nameSpace,
                            const std::string attrName, bson_t *bsonMetadata,
                            long unsigned int *dataSize, size_t *numberElements,
                            bool *IsSingleValue, int *type);

void GetAdiosTypeString(int type, std::string *typeString);

// extern template void DefineAttributeInInit(                                \
        core::IO *io, const std::string attrName, T *data, int type,           \
        bool IsSingleValue, size_t numberElements);                            \


#define variable_template_instantiation(T)                                     \
    extern template void ParseVariableFromBSON(                                \
        Variable<T> &variable, bson_t *bsonMetadata,                           \
        const std::string nameSpace, long unsigned int *dataSize);             \
    ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAFORMATREADER_H_ */
