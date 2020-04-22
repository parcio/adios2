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

namespace adios2
{
namespace core
{
namespace engine
{
/**
 * Defines the variable in the passed io.
 * @param io           [description]
 * @param varName      [description]
 * @param type         [description]
 * @param shape        [description]
 * @param start        [description]
 * @param count        [description]
 * @param constantDims [description]
 */
void DefineVariableInInit(core::IO *io, const std::string varName,
                          std::string type, Dims shape, Dims start, Dims count,
                          bool constantDims);
/**
 * Initializes all the fields that ADIOS is relying on without actually
 * requiring when defining the variable at the io.
 * @param io          IO to define variable at
 * @param engine      engine that defined the variable
 * @param varName     variableName
 * @param blocks      array how many blocks there are per step (index starting
 * at 0)
 * @param numberSteps blocks-size (simple array does not know its length)
 * @param shapeID     ID whether it is a global/local array/value, important!
 */
void InitVariable(core::IO *io, core::Engine &engine, std::string varName,
                  size_t *blocks, size_t numberSteps, ShapeID shapeID);

/**
 * Deserialize the metadata for a variable that stays the same for each block.
 * This information is needed for the definition of the variable in the engines
 * init.
 * @param buffer           metadata buffer from JULEA key-value store
 * @param type             variableType
 * @param shape            dimension: shape
 * @param start            dimension: start
 * @param count            dimension: count
 * @param constantDims     isConstantDims
 * @param blocks           array how many blocks there are per step (index
 * starting at 0)
 * @param numberSteps      blocks-size (simple array does not know its length)
 * @param shapeID          ID whether it is a global/local array/value,
 * important!
 * @param readAsJoined     variable read as joined
 * @param readAsLocalValue read as local value
 * @param randomAccess     reading using either streaming oder random access
 */
void DeserializeVariableMetadata(gpointer buffer, std::string *type,
                                 Dims *shape, Dims *start, Dims *count,
                                 bool *constantDims, size_t **blocks,
                                 size_t *numberSteps, ShapeID *shapeID,
                                 bool *readAsJoined, bool *readAsLocalValue,
                                 bool *randomAccess);
/**
 * Deserialize the metadata of a single block of a step of a variable.
 * @param variable         variable
 * @param buffer           metadata buffer from JULEA key-value store
 * @param blockID          blockID (0 index)
 * @param info             info struct to store block infos in
 */
template <class T>
void DeserializeBlockMetadata(Variable<T> &variable, gpointer buffer,
                              size_t blockID,
                              typename core::Variable<T>::Info &info);

/**
 * Deserializes the passed buffer and returns the created info struct.
 *
 * Note: the variable is only passed, because it seems to be not possible to
 * have an info struct without a variable. Template type cannot be deduced then.
 * Variable is const as this function is called with bpls.
 * @param variable          variable
 * @param buffer            metadata buffer from JULEA key-value store
 * @returns smart pointer to info struct. So that the allocated memory is not
 * leaked in bpls
 */
template <class T>
std::unique_ptr<typename core::Variable<T>::Info>
GetDeserializedMetadata(const core::Variable<T> &variable, gpointer buffer);

/**
 * //TODO: currently not implemented. May not be necessary
 * Sets read block information from the available metadata information
 * @param variable
 * @param blockInfo
 */
template <class T>
void SetVariableBlockInfo(core::Variable<T> &variable,
                          typename core::Variable<T>::Info &blockInfo);

/**
 * Set the minimum and the maximum for the variable.
 */
template <class T>
void SetMinMax(Variable<T> &variable, const T *data);

/* --- old BSON stuff--- */

void ParseAttributeFromBSON(const std::string nameSpace,
                            const std::string attrName, bson_t *bsonMetadata,
                            long unsigned int *dataSize, size_t *numberElements,
                            bool *IsSingleValue, int *type);

void ParseAttributeFromBSON(const std::string nameSpace,
                            const std::string attrName, bson_t *bsonMetadata,
                            long unsigned int *completeSize,
                            size_t *numberElements, bool *IsSingleValue,
                            int *type, unsigned long **dataSizes);

void GetAdiosTypeString(int type, std::string *typeString);

#define variable_template_instantiation(T)                                     \
    extern template void DeserializeBlockMetadata(                             \
        Variable<T> &variable, gpointer buffer, size_t block,                  \
        typename core::Variable<T>::Info &info);                               \
    extern template std::unique_ptr<typename core::Variable<T>::Info>          \
    GetDeserializedMetadata(const core::Variable<T> &variable,                 \
                            gpointer buffer);                                  \
                                                                               \
    extern template void SetVariable(Variable<T> &variable, size_t *blocks,    \
                                     size_t numberSteps, ShapeID shapeID);     \
    extern template void SetVariableBlockInfo(                                 \
        core::Variable<T> &variable, typename core::Variable<T>::Info &info);  \
    ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAFORMATREADER_H_ */
