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
: Engine("JuleaKVWriter", io, name, mode, std::move(comm))
{
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
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " deconstructor on "
                  << m_Name << "\n";
    }
    j_semantics_unref(m_JuleaSemantics);
}

/**
 * Begins a step. Clears the deferred variable set.
 */
StepStatus JuleaKVWriter::BeginStep(StepMode mode, const float timeoutSeconds)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank
                  << "   BeginStep() new step " << m_CurrentStep << "\n";
        std::cout << "StepMode mode: " << mode << std::endl;
    }

    m_StepMode = mode;

    /** still not completely sure why writes before first step are not forgotten
     * by clearing this set. */
    m_DeferredVariables.clear();

    return StepStatus::OK;
}

/**
 * Returns the current step.
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
 * Ends the current step. All deferred variables are written now.
 */
void JuleaKVWriter::EndStep()
{
    std::cout << "--- DEBUG : EndStep1" << std::endl;
    // if (m_NeedPerformPuts)
    if (m_DeferredVariables.size() > 0)
    {
        std::cout << "--- DEBUG : EndStep2" << std::endl;
        std::cout << "m_DeferredVariables.size() = "
                  << m_DeferredVariables.size() << std::endl;
        PerformPuts();
    }

    PutAttributes(m_IO);

    ++m_CurrentStep;

    if (m_CurrentStep % m_FlushStepsCount == 0)
    {
        Flush();
    }
    m_CurrentBlockID = 0;

    if (m_Verbosity == 5)
    {
        std::cout << "\n______________EndStep _____________________"
                  << std::endl;
    }
}

/**
 * Called to guarantee that read/write are really executed and the results
 * available.
 */
void JuleaKVWriter::PerformPuts()
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "     PerformPuts()\n";
    }

    /** if there are no deferred variables there is nothing to do */
    if (m_DeferredVariables.empty())
    {
        return;
    }

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
        std::cout << "ATTENTION" << std::endl;                                 \
                                                                               \
        PerformPutCommon(variable);                                            \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation
    }
    m_DeferredVariables.clear();
}
// ADIOS2_FOREACH_TYPE_1ARG(declare_template_instantiation)

/**
 * Flushes the aggregated data.
 * @param transportIndex [description]
 */
// void JuleaKVWriter::Flush()
void JuleaKVWriter::Flush(const int transportIndex)
{

    if (m_Verbosity == 5)
    {
        std::cout << "\n______________Flush  _____________________"
                  << std::endl;
        std::cout << "Julea Writer " << m_WriterRank << "   Flush()\n";
    }
    DoFlush(false);

    if (m_CollectiveMetadata)
    {
        // WriteCollectiveMetadataFile
    }
}

/** --- PRIVATE FUNCTIONS --- */
/**
 * Initalizes engine. Prints a lovely penguin :-)
 */
void JuleaKVWriter::Init()
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << "   Init() "
                  << std::endl;
    }

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

    m_JuleaSemantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

    InitParameters();
    InitVariables();
}

/**TODO needed?
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

/**
 * [JuleaWriter::InitVariables description]
 */
void JuleaKVWriter::InitVariables()
{
    // TODO: do something here with deferredVariables?
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Writer " << m_WriterRank << " InitVariables()\n";
    }
}

/**
 * Puts variable to JULEA object store. Afterwards deletes related blockinfo
 * struct from variable m_BlocksInfo vector [declare_type description]
 * @param variable      variable
 * @param data          variable data
 */

#define declare_type(T)                                                        \
    void JuleaKVWriter::DoPutSync(Variable<T> &variable, const T *data)        \
    {                                                                          \
        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(    \
            m_CurrentBlockID);                                                 \
        PutSyncCommon(variable, data);                                         \
        variable.m_BlocksInfo.pop_back();                                      \
        m_CurrentBlockID++;                                                    \
    }                                                                          \
    void JuleaKVWriter::DoPutDeferred(Variable<T> &variable, const T *data)    \
    {                                                                          \
        PutDeferredCommon(variable, data);                                     \
    }
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

/**TODO
 * [JuleaWriter::DoClose description]
 * @param transportIndex [description]
 */
void JuleaKVWriter::DoClose(const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________DoClose_____________________"
                  << std::endl;
        std::cout << "Julea Writer " << m_WriterRank << " Close(" << m_Name
                  << ")\n";
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
        std::cout << "Julea Writer " << m_WriterRank << " DoFlush \n";
    }
    // if (m_Aggregator.m_IsActive)
    // {
    //     // std::cout << "AggregateWriteData" << std::endl;
    //     AggregateWriteData(isFinal, transportIndex);
    //     // AggregateWriteData(isFinal);
    // }
    // else
    // {
    //     // std::cout << "WriteData" << std::endl;
    //     WriteData(isFinal, transportIndex);
    //     // WriteData(isFinal);
    // }
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
        PutAttributeMetadataToJulea(attribute, bsonMetadata, m_Name);          \
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
