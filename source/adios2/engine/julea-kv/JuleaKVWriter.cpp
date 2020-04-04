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

#include "JuleaFormatWriter.h"
#include "JuleaInteractionWriter.h"

#include "adios2/core/IO.h"
#include "adios2/helper/adiosFunctions.h"
#include "adios2/toolkit/transport/file/FileFStream.h"

#include <iostream>
#include <julea-object.h> //needed?

namespace adios2
{
namespace core
{
namespace engine
{

JuleaKVWriter::JuleaKVWriter(IO &io, const std::string &name, const Mode mode,
                            helper::Comm comm)
: Engine("JuleaKVWriter", io, name, mode, std::move(comm)), m_BPSerializer(m_Comm,m_DebugMode)
// : Engine("JuleaKVWriter", io, name, mode, mpiComm), m_Julea(io.m_DebugMode)
{
    // std::cout << "JULEA ENGINE: Constructor" << std::endl;
    // m_BP3Serializer(mpiComm, m_DebugMode),
    // m_FileDataManager(mpiComm, m_DebugMode),
    // m_EndMessage = " in call to JuleaKVWriter " + m_Name + " Open\n";

    // MPI_Comm_rank(mpiComm, &m_WriterRank); //TODO: changed in release_25
    m_WriterRank = m_Comm.Rank();
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
    j_semantics_unref(m_JuleaSemantics);
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
    // m_CurrentStep++; // 0 is the first step
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank
                  << "   BeginStep() new step " << m_CurrentStep << "\n";
    }
    std::cout << "StepMode mode: " << mode << std::endl;
    m_StepMode = mode;
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
    // FIXME: append Step Data to Object store not replace it!
    //
    // std::cout << "JULEA ENGINE: EndStep" << std::endl;
    // if (m_NeedPerformPuts)
    if (m_DeferredVariables.size() > 0)
    {
        std::cout << "m_DeferredVariables.size() = "
                  << m_DeferredVariables.size() << std::endl;
        PerformPuts(); // FIXME
    }
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________EndStep _____________________"
                  << std::endl;
        // std::cout << "Julea Writer " << m_WriterRank << "   EndStep()\n";
    }
    // TODO
    // SerializeData in BP3
    // - Profiler ?!
    // - SerializeDataBuffer (write attributes?!)
    // --- CopyToBuffer
    // --- attributesSizeInData
    // -
    PutAttributes(m_IO);

    /* advance step */
    // ++m_MetadataSet.TimeStep;
    // ++m_MetadataSet.CurrentStep;
    ++m_TimeStep;
    ++m_CurrentStep;

    /* ------ original EndStep */
    const size_t currentStep = CurrentStep();
    const size_t flushStepsCount = m_FlushStepsCount;
    if (m_CurrentStep % m_FlushStepsCount == 0)
    {
        Flush();
    }
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
#define declare_template_instantiation(T)                                      \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        Variable<T> &variable = FindVariable<T>(                               \
            variableName, "in call to PerformPuts, EndStep or Close");         \
        std::cout << "ATTENTION" << std::endl;\
                                                                               \
        PerformPutCommon(variable);                                            \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation
    }
    m_DeferredVariables.clear();
    // m_NeedPerformPuts = false;
}
// ADIOS2_FOREACH_TYPE_1ARG(declare_template_instantiation)

/**
 * TODO
 * [JuleaWriter::Flush description]
 * @param transportIndex [description]
 */
// void JuleaKVWriter::Flush()
void JuleaKVWriter::Flush(const int transportIndex)
{
    std::cout << "\n______________Flush  _____________________" << std::endl;
    DoFlush(false);
    // ResetBuffer(m_Data);

    if (m_CollectiveMetadata)
    {
        // WriteCollectiveMetadataFile
    }

    if (m_Verbosity == 5)
    {
        // std::cout << "Julea Writer " << m_WriterRank << "   Flush()\n";
    }
}

/** --- PRIVATE FUNCTIONS --- */
/**
 * TODO
 * [JuleaWriter::Init description]
 */
void JuleaKVWriter::Init()
{
    if (m_OpenMode == Mode::Append)
    {
        throw std::invalid_argument("JuleaKVWriter: OpenMode   -- Append --   "
                                    "hasn't been implemented, yet");
    }
    std::cout << "\n*********************** JULEA ENGINE WRITER "
                 "*************************"
              << std::endl;
    std::cout << "JULEA WRITER: Init" << std::endl;
    std::cout
        << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
        << std::endl;

    std::cout << "JULEA WRITER: Namespace = " << m_Name << std::endl;
    // TODO: which order?

    m_JuleaSemantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

    // j_init();
    InitParameters();
    InitTransports();
    InitVariables();
}

/**TODO
 * see BP3Base InitParameters
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
        //  else if (key == "collectivemetadata")
        // {
        //     InitParameterCollectiveMetadata(value);
        // }
        else if (key == "flushstepscount")
        {
            InitParameterFlushStepsCount(value);
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
// void JuleaKVWriter::DoClose()
void JuleaKVWriter::DoClose(const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________DoClose_____________________"
                  << std::endl;
        // std::cout << "Julea Writer " << m_WriterRank << " Close(" << m_Name
        // << ")\n";
    }
    // TODO: free semantics
    /* Write deferred variables*/
    if (m_DeferredVariables.size() > 0)
    {
        PerformPuts(); // TODO: correct?
    }
    DoFlush(true, transportIndex);
    // TODO: Close Transports?!
}

/**TODO
 * [JuleaWriter::DoFlush description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
// void JuleaKVWriter::DoFlush(const bool isFinal)
void JuleaKVWriter::DoFlush(const bool isFinal, const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________DoFlush_____________________"
                  << std::endl;
        // std::cout << "Julea Writer " << m_WriterRank << " DoFlush \n";
    }
    if (m_Aggregator.m_IsActive)
    {
        // std::cout << "AggregateWriteData" << std::endl;
        AggregateWriteData(isFinal, transportIndex);
        // AggregateWriteData(isFinal);
    }
    else
    {
        // std::cout << "WriteData" << std::endl;
        WriteData(isFinal, transportIndex);
        // WriteData(isFinal);
    }
}

/**
 * TODO
 * [JuleaWriter::WriteData description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
// void JuleaKVWriter::WriteData(const bool isFinal)
void JuleaKVWriter::WriteData(const bool isFinal, const int transportIndex)
{
    std::cout << "\n______________WriteData_____________________" << std::endl;

    Metadata *metadata;
    // TODO: parse variable from buffer to metadata struct members
    // DESIGN: check BP3Writer
    size_t dataSize = m_Data.m_Position;

    if (isFinal)
    {
        // m_BP3Serializer.CloseData(m_IO); DESIGN how to realize with JULEA?
        if (!m_IsClosed)
        {
            PutAttributes(m_IO);
        }
        // dataSize = m_Data.m_Position;
    }
    else
    {
        // m_BP3Serializer.CloseStream(m_IO); //TODO needed?
        // TODO: write attributes?
    }

    // m_FileDataManager.WriteFiles(m_Data.m_Buffer.data(), dataSize,
    // transportIndex); m_FileDataManager.FlushFiles(transportIndex);

    // TODO: sufficient?
    // j_gmm_put (metadata, m_Data.m_Buffer.data());
    if (m_Verbosity == 5)
    {
        // std::cout << "Julea Writer " << m_WriterRank << " WriteData\n";
    }
}

/**
 * TODO
 * [JuleaWriter::AggregateWriteData description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
// void JuleaKVWriter::AggregateWriteData(const bool isFinal)
void JuleaKVWriter::AggregateWriteData(const bool isFinal,
                                       const int transportIndex)
{
    std::cout << "\n______________AggregateWriteData_____________________"
              << std::endl;
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
 *  Put attributes held in passed IO. Called from EndStep()
 * @param io [description]
 */
void JuleaKVWriter::PutAttributes(core::IO &io)
{
    std::cout << "\n______________PutAttributes_____________________"
              << std::endl;

    const auto attributesDataMap = io.GetAttributesDataMap();

    // count is known ahead of time
    const uint32_t attributesCount =
        static_cast<uint32_t>(attributesDataMap.size());

    std::cout << "attributesCount: " << attributesCount << std::endl;

    for (const auto &attributePair : attributesDataMap)
    {
        unsigned int dataSize = 0;
        const std::string type(attributePair.second.first);
        const std::string name(attributePair.first);

        auto bsonMetadata = bson_new();
        std::cout << "------------------------------------" << std::endl;
        std::cout << "-- PutAttributes: type " << type << std::endl;
        std::cout << "-- PutAttributes: name " << name << std::endl;

        // each attribute is only written to output once
        // so filter out the ones already written
        // FIXME: should this be guaranteed by the attributeMap of IO?
        // FIXME: is m_SerializeAttributes already in use?
        // auto it = m_SerializedAttributes.find(name);
        // if (it != m_SerializedAttributes.end())
        // {
        //     continue;
        // }

        if (type == "unknown")
        {
            std::cout << "Attribute type is 'unknown' " << std::endl;
        }

#define declare_attribute_type(T)                                              \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        Attribute<T> &attribute = *io.InquireAttribute<T>(name);               \
        std::cout << "-- PutAttributes: m_Elements " << attribute.m_Elements   \
                  << std::endl;                                                \
        ParseAttributeToBSON(attribute, bsonMetadata);                         \
        ParseAttrTypeToBSON(attribute, bsonMetadata);                          \
        PutAttributeMetadataToJuleaSmall(attribute, bsonMetadata, m_Name);     \
        PutAttributeDataToJulea(attribute, m_Name);                            \
        bson_destroy(bsonMetadata);                                            \
    }
        ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(declare_attribute_type)
#undef declare_attribute_type
        // free(attrName);
        // delete(&attrName);
    } // end for
}

void JuleaKVWriter::InitParameterFlushStepsCount(const std::string value)
{
    long long int flushStepsCount = -1;

    if (m_DebugMode)
    {
        bool success = true;
        std::string description;

        try
        {
            flushStepsCount = std::stoll(value);
        }
        catch (std::exception &e)
        {
            success = false;
            description = std::string(e.what());
        }

        if (!success || flushStepsCount < 1)
        {
            throw std::invalid_argument(
                "ERROR: value in FlushStepscount=value in IO SetParameters "
                "must be an integer >= 1 (default) \nadditional "
                "description: " +
                description + "\n, in call to Open\n");
        }
    }
    else
    {
        flushStepsCount = std::stoll(value);
    }

    m_FlushStepsCount = static_cast<size_t>(flushStepsCount);
}

} // end namespace engine
} // end namespace core
} // end namespace adios2
