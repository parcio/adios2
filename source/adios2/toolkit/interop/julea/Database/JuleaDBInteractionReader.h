/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_H_

// #include "JuleaMetadata.h"
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop
// namespace!
#include "adios2/common/ADIOSMacros.h"
#include "adios2/common/ADIOSTypes.h"
#include "adios2/core/IO.h" // for CreateVar
#include "adios2/core/Variable.h"

#include <julea.h>

#include <string>

#include <stdexcept> // for Intel Compiler

namespace adios2
{
namespace interop
{

class JuleaDBInteractionReader : public JuleaInteraction
{

public:

   JuleaDBInteractionReader(helper::Comm const &comm);
    ~JuleaDBInteractionReader() = default;
    std::string m_JuleaNamespace = "adios2";
    // std::string m_VariableTableName; in DBInteractionWriter

    // void SetMinMaxValueFields(std::string *minField, std::string *maxField,
    //                           std::string *valueField, std::string
    //                           *meanField, const adios2::DataType varType);


// void DAIsetMinMaxValueFields(std::string *minField, std::string *maxField,
//                           std::string *valueField, std::string *meanField,
//                           const adios2::DataType varType);

void DefineVariableInInit(core::IO *io, const std::string varName,
                            std::string type, Dims shape, Dims start,
                            Dims count, bool constantDims, bool isLocalValue);

void CheckSchemas();

void InitVariablesFromDB(const std::string nameSpace, core::IO *io,
                         core::Engine &engine);


/**
 * Deserialize the metadata of a single block of a step of a variable.
 * @param variable         variable
 * @param buffer           metadata buffer from JULEA key-value store
 * @param blockID          blockID (0 index)
 * @param info             info struct to store block infos in
 */
// template <class T>
// void DeserializeBlockMetadata(Variable<T> &variable, gpointer buffer,
//                               size_t blockID,
//                               typename core::Variable<T>::Info &info);
template <class T>
void GetCountFromBlockMetadata(const std::string nameSpace,
                               const std::string varName, size_t step,
                               size_t block, Dims *count, size_t entryID,
                               bool isLocalValue, T *value);

template <class T>
std::unique_ptr<typename core::Variable<T>::Info>
GetBlockMetadata(const core::Variable<T> &variable,
                   // const std::string nameSpace, size_t step, size_t block,
                   size_t entryID) const;

// entryID: unique ID for entry in database
template <class T>
void GetBlockMetadataNEW(core::Variable<T> &variable,
                           typename core::Variable<T>::Info &blockInfo,
                           size_t entryID);

/* --- Variables --- */

/** Retrieves all variable names from key-value store. They are all stored
 * in one bson. */
void GetNamesFromJulea(const std::string nameSpace, bson_t **bsonNames,
                         unsigned int *varCount, bool isVariable);

/** Retrieves the metadata buffer for the variable metadata that do not vary
 * from block to block. The key is the variable name. */
void GetVariableMetadataFromJulea(const std::string nameSpace,
                                    const std::string varName, gpointer *buffer,
                                    guint32 *buffer_len);

/** Retrieves the block metadata buffer from the key-value store. The key is:
 * currentStep_currentBlock. The variable name and the nameSpace from the
 * key-value namespace. */
void GetBlockMetadataFromJulea(const std::string nameSpace,
                                 const std::string varName, gpointer *buffer,
                                 guint32 *buffer_len,
                                 const std::string stepBlockID);


private:
    // something private
};

#define variable_template_instantiation(T)                                     \
    extern template void JuleaDBInteractionReader::GetCountFromBlockMetadata(                            \
        const std::string nameSpace, const std::string varName, size_t step,   \
        size_t block, Dims *count, size_t entryID, bool isLocalValue,          \
        T *value);                                                             \
    extern template std::unique_ptr<typename core::Variable<T>::Info>          \
    JuleaDBInteractionReader::GetBlockMetadata(const core::Variable<T> &variable, size_t entryID) const;     \
                                                                               \
    extern template void JuleaDBInteractionReader::GetBlockMetadataNEW(                                \
        core::Variable<T> &variable, typename core::Variable<T>::Info &blockInfo,    \
        size_t entryID);                                                       
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONREADER_H_ */
