/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaKVWriter.h"
#include "JuleaKVWriter.tcc"

#include "JuleaClientLogic-legacy.h" //TODO: replace with new functionality

#include "JuleaFormatWriter.h"
#include "JuleaInteraction.h"

#include "adios2/core/IO.h"
#include "adios2/helper/adiosFunctions.h"
#include "adios2/toolkit/transport/file/FileFStream.h"

#include <iostream>
#include <julea-config.h> //needed?
#include <julea-object.h> //needed?

// #include <julea-adios.h>

namespace adios2
{
namespace core
{
namespace engine
{

JuleaKVWriter::JuleaKVWriter(IO &io, const std::string &name, const Mode mode,
                             MPI_Comm mpiComm)
: Engine("JuleaKVWriter", io, name, mode, mpiComm), m_Julea(io.m_DebugMode)
{
    // std::cout << "JULEA ENGINE: Constructor" << std::endl;
    // m_BP3Serializer(mpiComm, m_DebugMode),
    // m_FileDataManager(mpiComm, m_DebugMode),
    // m_EndMessage = " in call to JuleaKVWriter " + m_Name + " Open\n";
    MPI_Comm_rank(mpiComm, &m_WriterRank);
    Init();
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " Open(" << m_Name
                  << ") in constructor." << std::endl;
    }
}

JuleaKVWriter::~JuleaKVWriter()
{
    // DoClose();
    // if (m_Verbosity == 5)
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " deconstructor on "
                  << m_Name << "\n";
    }
}

/**
 * TODO
 * [JuleaKVWriter::BeginStep description]
 * @param  mode           [description]
 * @param  timeoutSeconds [description]
 * @return                [description]
 */
StepStatus JuleaKVWriter::BeginStep(StepMode mode, const float timeoutSeconds)
{
    // std::cout << "JULEA ENGINE: BeginStep" << std::endl;
    m_CurrentStep++; // 0 is the first step
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank
                  << "   BeginStep() new step " << m_CurrentStep << "\n";
    }
    m_DeferredVariables.clear();
    m_DeferredVariablesDataSize = 0;
    return StepStatus::OK;
}

/**
 * TODO
 * [JuleaWriter::CurrentStep description]
 * @return [description]
 */
size_t JuleaKVWriter::CurrentStep() const
{
    // std::cout << "JULEA ENGINE: CurrentStep" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank
                  << "   CurrentStep() returns " << m_CurrentStep << "\n";
    }
    return m_CurrentStep;
}

/**
 * TODO
 * [JuleaWriter::EndStep description]
 */
void JuleaKVWriter::EndStep()
{
    // std::cout << "JULEA ENGINE: EndStep" << std::endl;
    if (m_NeedPerformPuts)
    {
        PerformPuts();
    }
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "   EndStep()\n";
    }

    if (m_CurrentStep % m_FlushStepsCount == 0)
    {
        Flush();
    }
    // TODO: PutAttributes(m_IO)
    PutAttributes(m_IO);
}

/**
 * Called to guarantee that read/write are really executed and the results
 * available. [JuleaWriter::PerformPuts description]
 */
void JuleaKVWriter::PerformPuts()
{
    // std::cout << "JULEA ENGINE: PerformPuts" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PerformPuts()\n";
    }

    // is it actually necessary to differentiate? or is perform puts only called
    // with sync? probably not?! PSEUDO: how to get actual value?
    // bool deferred = true;
    // if(deferred){
    //     return;
    // }

    /** if there are no deferred variables there is nothing to do */
    if (m_DeferredVariables.empty())
    {
        return;
    }

    // m_BP3Serializer.ResizeBuffer(m_BP3Serializer.m_DeferredVariablesDataSize,
    //                              "in call to PerformPuts");

    /** Call PutSyncCommon for every variable that has been deferred */
    for (const std::string &variableName : m_DeferredVariables)
    {
        const std::string type = m_IO.InquireVariableType(variableName);
        if (type == "compound")
        {
            // not supported
            std::cout << "Julea Writer " << m_WriterRank << "     PerformPuts()"
                      << "compound variable type not supported \n";
        }
// KILLME! Who would want a template in a makro in a function?!
// FIXME: change to PutSyncCommon(variable,data);
// FIXME: still working without for loop over blockinfo?
#define declare_template_instantiation(T)                                      \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        Variable<T> &variable = FindVariable<T>(                               \
            variableName, "in call to PerformPuts, EndStep or Close");         \
                                                                               \
        PutSyncCommon(variable, variable.m_Data);                              \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation
    }
    m_DeferredVariables.clear();
    m_NeedPerformPuts = false;
}
// ADIOS2_FOREACH_TYPE_1ARG(declare_template_instantiation)

/**
 * TODO
 * [JuleaWriter::Flush description]
 * @param transportIndex [description]
 */
void JuleaKVWriter::Flush(const int transportIndex)
{
    DoFlush(false, transportIndex);
    // ResetBuffer(m_Data);

    if (m_CollectiveMetadata)
    {
        // WriteCollectiveMetadataFile
    }

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "   Flush()\n";
    }
}

/** --- PRIVATE FUNCTIONS --- */
/**
 * TODO
 * [JuleaWriter::Init description]
 */
void JuleaKVWriter::Init()
{
    std::cout << "\n*********************** JULEA ENGINE WRITER "
                 "*************************"
              << std::endl;
    std::cout << "JULEA WRITER: Init" << std::endl;
    std::cout
        << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
        << std::endl;

    // TODO: which order?
    m_Julea.Init();
    j_init();
    m_JuleaInfo = g_slice_new(JuleaInfo);
    m_JuleaInfo->semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    m_JuleaInfo->name_space = g_strdup(m_Name.c_str());

    // j_adios_init(m_JuleaInfo);

    InitParameters();
    InitTransports();
    InitVariables();
}

/**TODO
 * [JuleaWriter::InitParameters description]
 */
void JuleaKVWriter::InitParameters()
{
    // std::cout << "JULEA ENGINE: Init Parameters" << std::endl;
    for (const auto &pair : m_IO.m_Parameters)
    {
        std::string key(pair.first);
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);

        std::string value(pair.second);
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);

        if (key == "verbose")
        {
            m_Verbosity = std::stoi(value);
            if (m_DebugMode)
            {
                if (m_Verbosity < 0 || m_Verbosity > 5)
                    throw std::invalid_argument(
                        "ERROR: Method verbose argument must be an "
                        "integer in the range [0,5], in call to "
                        "Open or Engine constructor\n");
            }
        }
    }
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " InitParameters()\n";
    }
}

/**TODO
 * [JuleaWriter::InitTransports description]
 */
void JuleaKVWriter::InitTransports()
{
    // Nothing to process from m_IO.m_TransportsParameters
    // std::cout << "JULEA ENGINE: Init Transport" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " InitTransports()\n";
    }
}

/**
 * [JuleaWriter::InitVariables description]
 */
void JuleaKVWriter::InitVariables()
{
    // m_DeferredVariables.init() FIXME: how to init set of strings?
    // constructur? Nothing to process from m_IO.m_TransportsParameters
    // std::cout << "JULEA ENGINE: Init Transport" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " InitVariables()\n";
    }
}

/**TODO
 * [declare_type description]
 * @param  T [description]
 * @return   [description]
 */
// #define declare_type(T)                                                        \
//     void JuleaWriter::DoPutSync(Variable<T> &variable, const T *data)          \
//     {                                                                          \
//         PutSyncCommon(variable, variable.SetBlockInfo(data, CurrentStep()));   \
//         variable.m_BlocksInfo.clear();                                         \
//     }                                                                          \
//     void JuleaWriter::DoPutDeferred(Variable<T> &variable, const T *data)      \
//     {                                                                          \
//         PutDeferredCommon(variable, data);                                     \
//     }
// ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
// #undef declare_type
// ADIOS2_FOREACH_TYPE_1ARG(declare_type)

/**TODO
 * [declare_type description]
 * @param  T [description]
 * @return   [description]
 */
#define declare_type(T)                                                        \
    void JuleaKVWriter::DoPutSync(Variable<T> &variable, const T *data)        \
    {                                                                          \
        PutSyncCommon(variable, data);                                         \
    }                                                                          \
    void JuleaKVWriter::DoPutDeferred(Variable<T> &variable, const T *data)    \
    {                                                                          \
        PutDeferredCommon(variable, data);                                     \
    }
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
// ADIOS2_FOREACH_TYPE_1ARG(declare_type)

/**TODO
 * [JuleaWriter::DoClose description]
 * @param transportIndex [description]
 */
void JuleaKVWriter::DoClose(const int transportIndex)
{
    // std::cout << "JULEA ENGINE: Do close" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " Close(" << m_Name
                  << ")\n";
    }
    // m_JuleaInfo->semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

    g_free(m_JuleaInfo->name_space);
    g_slice_free(JuleaInfo, m_JuleaInfo);
}

/**TODO
 * [JuleaWriter::DoFlush description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
void JuleaKVWriter::DoFlush(const bool isFinal, const int transportIndex)
{
    // if (m_BP3Serializer.m_Aggregator.m_IsActive)
    // {
    AggregateWriteData(isFinal, transportIndex);
    // }
    // else
    // {
    WriteData(isFinal, transportIndex);
    // }
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " DoFlush \n";
    }
}

/**
 * TODO
 * [JuleaWriter::WriteData description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
void JuleaKVWriter::WriteData(const bool isFinal, const int transportIndex)
{
    Metadata *metadata;
    // TODO: parse variable from buffer to metadata struct members
    // DESIGN: check BP3Writer
    size_t dataSize = m_Data.m_Position;

    if (isFinal)
    {
        // m_BP3Serializer.CloseData(m_IO); DESIGN how to realize with JULEA?
        dataSize = m_Data.m_Position;
    }
    else
    {
        // m_BP3Serializer.CloseStream(m_IO);
        // parameter passed to constructor?!
    }

    // m_FileDataManager.WriteFiles(m_Data.m_Buffer.data(), //FIXME: Compiler ?!
    //                              dataSize, transportIndex);
    // m_FileDataManager.FlushFiles(transportIndex);

    // TODO: sufficient?
    // j_gmm_put (metadata, m_Data.m_Buffer.data());
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " WriteData\n";
    }
}

/**
 * TODO
 * [JuleaWriter::AggregateWriteData description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
void JuleaKVWriter::AggregateWriteData(const bool isFinal,
                                       const int transportIndex)
{
    // DESIGN: check BP3Writer
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " AggregateWriteData\n";
        std::cout << " Data aggregation for writing not yet supported."
                  << std::endl;
    }
    // TODO Implement!
    //
    if (isFinal) // Write metadata footer
    {
        // BufferSTL &bufferSTL = m_Data;
        // ResetBuffer(bufferSTL, false, false);

        // TODO Implement
        // m_BP3Serializer.AggregateCollectiveMetadata(
        //     m_Aggregator.m_Comm, bufferSTL, false);

        if (m_Aggregator.m_IsConsumer)
        {
            // m_FileDataManager.WriteFiles(bufferSTL.m_Buffer.data(),
            //                              bufferSTL.m_Position,
            //                              transportIndex);
            //

            // m_FileDataManager.FlushFiles(transportIndex);

            // Metadata *metadata //TODO pass buffer data meta data to c struct
            // j_gmm_put(metadata, bufferSTL.m_Buffer.data());
        }

        m_Aggregator.Close(); // MPIChain for communication tasks in aggregation
    }

    // m_Aggregator.ResetBuffers();
}

/**
 * Parsing the attribute types to enum defined in JULEA's Adios Client.
 * Great that types are handled as string here...
 */
void parse_attribute_type(std::string type, AttributeMetadata *attr_metadata)
{

    if (type == "string")
    {
        attr_metadata->attr_type = STRING;
    }
    if (type == "int8_t")
    {
        attr_metadata->attr_type = INT8;
    }
    if (type == "uint8_t")
    {
        attr_metadata->attr_type = UINT8;
    }
    if (type == "int16_t")
    {
        attr_metadata->attr_type = INT16;
    }
    if (type == "uint16_t")
    {
        attr_metadata->attr_type = UINT16;
    }
    if (type == "int32_t")
    {
        attr_metadata->attr_type = INT32;
    }
    if (type == "uint32_t")
    {
        attr_metadata->attr_type = UINT32;
    }
    if (type == "int64_t")
    {
        attr_metadata->attr_type = INT64;
    }
    if (type == "uint64_t")
    {
        attr_metadata->attr_type = UINT64;
    }
    else if (type == "float")
    {
        attr_metadata->attr_type = FLOAT;
    }
    else if (type == "double")
    {
        attr_metadata->attr_type = DOUBLE;
    }
    else if (type == "long double")
    {
        attr_metadata->attr_type = LONG_DOUBLE;
    }
    // else if(type == "floast complex")
    // {
    //     attr_metadata->attr_type = COMPLEX_FLOAT;
    // }
    // else if(type == "double complex")
    // {
    //     attr_metadata->attr_type = COMPLEX_DOUBLE;
    // }
}

/**
 *  Put attributes held in passed IO. Called from EndStep()
 * @param io [description]
 */
void JuleaKVWriter::PutAttributesOld(core::IO &io)
{
    const auto attributesDataMap = io.GetAttributesDataMap();

    for (const auto &attributePair : attributesDataMap)
    {
        JBatch *batch;
        AttributeMetadata *attr_metadata = NULL;

        batch = j_batch_new(m_JuleaInfo->semantics);
        const std::string type(attributePair.second.first);
        const std::string name(attributePair.first);

        attr_metadata->name = strdup(name.c_str());
        attr_metadata->number_elements =
            static_cast<uint32_t>(attributesDataMap.size());
        parse_attribute_type(type, attr_metadata);
        void *data;
        // j_adios_put_attribute(m_JuleaInfo->name_space, attr_metadata,data,
        // batch, true ); FIXME

        // each attribute is only written to output once
        // so filter out the ones already written
        auto it = m_SerializedAttributes.find(attr_metadata->name);
        if (it != m_SerializedAttributes.end())
        {
            continue;
        }

        if (type == "unknown")
        {
            std::cout << "Attribute type is 'unknown' " << std::endl;
        }
#define declare_type(T)                                                        \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        core::Attribute<T> &attribute = *io.InquireAttribute<T>(name);         \
        attr_metadata->is_single_value = attribute.m_IsSingleValue;            \
        if (attribute.m_IsSingleValue)                                         \
        {                                                                      \
            PutAttributeToJulea(m_JuleaInfo->name_space, attr_metadata,        \
                                &attribute.m_DataSingleValue, batch);          \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            PutAttributeToJulea(m_JuleaInfo->name_space, attr_metadata,        \
                                &attribute.m_DataArray, batch);                \
        }                                                                      \
    }
        ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(declare_type)
#undef declare_type

        j_batch_execute(batch);
        j_batch_unref(batch);
    }
}

/**
 *  Put attributes held in passed IO. Called from EndStep()
 * @param io [description]
 */
void JuleaKVWriter::PutAttributes(core::IO &io)
{
    // ParseAttributeToBSON();
    // ParseAttrTypeToBSON();
    // PutAttributeMetadataToJulea();
    // PutattributeDataToJulea();

    const auto attributesDataMap = io.GetAttributesDataMap();

    for (const auto &attributePair : attributesDataMap)
    {
        // JBatch *batch;
        // AttributeMetadata *attr_metadata = NULL;

        // batch = j_batch_new(m_JuleaInfo->semantics);

        // attr_metadata->name = strdup(name.c_str());
        // attr_metadata->number_elements =
        //     static_cast<uint32_t>(attributesDataMap.size());
        // parse_attribute_type(type, attr_metadata);
        // void *data;
        // j_adios_put_attribute(m_JuleaInfo->name_space, attr_metadata,data,
        // batch, true ); FIXME


        auto bsonMetadata = bson_new();
        const std::string type(attributePair.second.first);
        const std::string name(attributePair.first);
        const std::string attrName = strdup(name.c_str());

        // each attribute is only written to output once
        // so filter out the ones already written
        // FIXME: is m_SerializeAttributes already in use?
        auto it = m_SerializedAttributes.find(attrName);
        if (it != m_SerializedAttributes.end())
        {
            continue;
        }

        if (type == "unknown")
        {
            std::cout << "Attribute type is 'unknown' " << std::endl;
        }

        // Attribute<T> &attribute = io.InquireAttribute<T>(name);
        // attr_metadata->is_single_value = attribute.m_IsSingleValue; \

#define declare_type(T)                                                        \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        Attribute<T> &attribute = *io.InquireAttribute<T>(name);               \
        if (attribute.m_IsSingleValue)                                         \
        {                                                                      \
            ParseAttributeToBSON(&attribute, bsonMetadata);                    \
            ParseAttrTypeToBSON(&attribute, bsonMetadata);                    \
            PutAttributeDataToJulea(&attribute, &attribute.m_DataSingleValue,  \
                                    m_JuleaInfo->name_space);                  \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            ParseAttributeToBSON(&attribute, bsonMetadata);                    \
            ParseAttrTypeToBSON(&attribute, bsonMetadata);                    \
            PutAttributeDataToJulea(&attribute, &attribute.m_DataArray,        \
                                    m_JuleaInfo->name_space);                  \
        }                                                                      \
    }                                                                          \
    ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(declare_type)
#undef declare_type

        //         j_batch_execute(batch);
        //         j_batch_unref(batch);
    }
}

} // end namespace engine
} // end namespace core
} // end namespace adios2
