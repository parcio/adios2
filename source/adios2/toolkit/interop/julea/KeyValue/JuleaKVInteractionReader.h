/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaSerializer.h
 *
 *  Created on: December 08, 2021
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONREADER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONREADER_H_

// #include "JuleaMetadata.h"
// #include "adios2/engine/julea/JuleaMetadata.h" //FIXME: move to interop
// namespace!
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

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

class JuleaKVInteractionReader : public JuleaInteraction
{

public:
    // something private

    // template <class T>
    // void JuleaDBDAISetMinMax(core::Variable<T> &variable, const T *data, T
    // &blockMin,
    //   T &blockMax, T &blockMean, size_t currentStep,
    //   size_t currentBlockID);

    // Explicit declaration of the public template methods
    // #define declare_template_instantiation(T) \
//     extern template void JuleaSerializer::Write(core::Variable<T>
    //     &variable,
    //     \
//                                                 const T *value);
    // ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
    // #undef declare_template_instantiation

    // // Explicit declaration of the public template methods
    // #define declare_template_instantiation(T) \
//     extern template void JuleaSerializer::ParseVariable(core::Variable<T>
    //     &variable,   \
//                                                 const T *data, Metadata
    //                                                 *metadata); \
// ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
    // #undef declare_template_instantiation

    JuleaKVInteractionReader(helper::Comm const &comm);
    ~JuleaKVInteractionReader() = default;
    // std::string m_JuleaNamespace = "adios2";//TODO: needed?
    // std::string m_VariableTableName; in DBInteractionWriter

    // void SetMinMaxValueFields(std::string *minField, std::string *maxField,
    //                           std::string *valueField, std::string
    //                           *meanField, const adios2::DataType varType);

    // void DAIsetMinMaxValueFields(std::string *minField, std::string
    // *maxField,
    //                           std::string *valueField, std::string
    //                           *meanField, const adios2::DataType varType);

    // void DefineVariableInInit(core::IO *io, const std::string varName,
    //                           std::string type, Dims shape, Dims start,
    //                           Dims count, bool constantDims, bool
    //                           isLocalValue);
    void ParseVariableFromBSON(bson_t *bsonMetadata, ShapeID *shapeID,
                               int *varTypeAsInt, Dims *shape, Dims *start,
                               Dims *count, size_t *numberSteps,
                               size_t **blocks, bool *isConstantDims,
                               bool *isReadAsJoined, bool *isReadAsLocalValue,
                               bool *isRandomAccess, bool *isSingleValue);

    void InitVariable(core::IO *io, core::Engine &engine,
                      const std::string projectNamespace,
                      const std::string fileName, std::string varName,
                      size_t *blocks, size_t numberSteps, ShapeID shapeID,
                      bool isReadAsJoined, bool isReadAsLocalValue,
                      bool isRandomAccess, bool isSingleValue);

    void DefineVariableInEngineIO(core::IO *io, const std::string varName,
                                  adios2::DataType type, ShapeID shapeID,
                                  Dims shape, Dims start, Dims count,
                                  bool constantDims, bool isLocalValue);

    // void CheckSchemas(std::string projectNamespace);

    void InitVariablesFromKV(const std::string projectNamespace,
                             const std::string fileName, core::IO *io,
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
    void GetCountFromBlockMetadata(const std::string projectNamespace,
                                   const std::string fileName,
                                   const std::string varName, size_t step,
                                   size_t block, Dims *count, size_t entryID,
                                   bool isLocalValue, T *value);

    template <class T>
    std::unique_ptr<typename core::Variable<T>::Info>
    GetBlockMetadata(const core::Variable<T> &variable,
                     std::string projectNamespace,
                     // const std::string nameSpace, size_t step, size_t block,
                     size_t entryID) const;

    /* --- Variables --- */
    // FIXME: parameter description needs updating: new namespace
    /** Retrieves all variable names from key-value store. They are all stored
     * in one bson. */
    void GetVarNamesFromJulea(const std::string projectNamespace,
                              const std::string fileName, bson_t **bsonNames,
                              unsigned int *varCount);

    /** Retrieves the metadata buffer for the variable metadata that do not vary
     * from block to block. The key is the variable name. */
    void GetVariableMetadataFromJulea(const std::string projectNamespace,
                                      const std::string fileName,
                                      const std::string varName,
                                      bson_t *bsonMetadata);

    /** Retrieves the block metadata buffer from the key-value store. The key
     * is: currentStep_currentBlock. The variable name and the nameSpace from
     * the key-value namespace. */
    void GetBlockMetadataFromJulea(const std::string projectNamespace,
                                   const std::string fileName,
                                   const std::string varName, gpointer *buffer,
                                   guint32 *buffer_len,
                                   const std::string stepBlockID);

private:
    // something private
};

#define variable_template_instantiation(T)                                     \
    extern template void JuleaKVInteractionReader::GetCountFromBlockMetadata(  \
        const std::string projectNamespace, const std::string fileName,        \
        const std::string varName, size_t step, size_t block, Dims *count,     \
        size_t entryID, bool isLocalValue, T *value);                          \
    extern template std::unique_ptr<typename core::Variable<T>::Info>          \
    JuleaKVInteractionReader::GetBlockMetadata(                                \
        const core::Variable<T> &variable, std::string projectNamespace,       \
        size_t entryID) const;
ADIOS2_FOREACH_STDTYPE_1ARG(variable_template_instantiation)
#undef variable_template_instantiation

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEAKVINTERACTIONREADER_H_ */
