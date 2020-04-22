/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEADBFORMATWRITER_H_
#define ADIOS2_ENGINE_JULEADBFORMATWRITER_H_

#include "JuleaDBWriter.h"
// #include "JuleaMetadata.h"

namespace adios2
{
namespace core
{
namespace engine
{

/** --- Variables --- */
/**
 * Set the minimum and maximum before storing the variable metadata.
 */
template <class T>
void SetMinMax(Variable<T> &variable, const T *data);

/**
 * Serialize the variable metadata that does not change from block to block.
 * Also needed for initialization of variables in reader.
 * @returns the buffer with the serialized metadata to store in JULEA key-value
 * store
 */
template <class T>
gpointer SerializeVariableMetadata(Variable<T> &variable, guint32 &buffer_len,
                                   size_t step);
/**
 *  Serialize the metadata for the given block in the given step.
 * @returns the buffer with the serialized metadata to store in JULEA key-value
 * store
 */
template <class T>
gpointer SerializeBlockMetadata(Variable<T> &variable, guint32 &buffer_len,
                                size_t step, size_t block,
                                const typename Variable<T>::Info &blockInfo);

#define variable_template_instantiation(T)                                     \
    SetMinMax(Variable<T> &variable, const T *data);                           \
    gpointer SerializeVariableMetadata(Variable<T> &variable, guint32 &len,    \
                                       size_t currStep);                       \
    gpointer SerializeBlockMetadata(                                           \
        Variable<T> &variable, guint32 &buffer_len, size_t step,               \
        const typename Variable<T>::Info &blockInfo),                          \
        size_t block;                                                          \
    ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

/** --- Attributes --- */
/**
 * The attributes are still stored using BSON. No real need to change it yet.
 */
template <class T>
void ParseAttributeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata);

template <class T>
void ParseAttrTypeToBSON(Attribute<T> &attribute, bson_t *bsonMetadata);
#define attribute_template_instantiation(T)                                    \
    extern template void ParseAttributeToBSON(Attribute<T> &attribute,         \
                                              bson_t *bsonMetadata);           \
    extern template void ParseAttrTypeToBSON(Attribute<T> &attribute,          \
                                             bson_t *bsonMetadata);            \
    ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(attribute_template_instantiation)
#undef attribute_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAFORMATWRITER_H_ */
