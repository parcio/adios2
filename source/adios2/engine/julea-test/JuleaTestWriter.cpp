/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaTestWriter.h"
#include "JuleaTestWriter.tcc"

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

JuleaTestWriter::JuleaTestWriter(IO &io, const std::string &name, const Mode mode,
                         MPI_Comm mpiComm)
: Engine("JuleaTestWriter", io, name, mode, mpiComm)
{
    // std::cout << "JULEA ENGINE: Constructor" << std::endl;
    // m_BP3Serializer(mpiComm, m_DebugMode),
    // m_FileDataManager(mpiComm, m_DebugMode),
    // m_EndMessage = " in call to JuleaTestWriter " + m_Name + " Open\n";
    MPI_Comm_rank(mpiComm, &m_WriterRank);
    Init();
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << " Open(" << m_Name
                  << ") in constructor." << std::endl;
    }
}

JuleaTestWriter::~JuleaTestWriter()
{
    // DoClose();
    // if (m_Verbosity == 5)
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << " deconstructor on "
                  << m_Name << "\n";
    }
}

/**
 * TODO
 * [JuleaTestWriter::BeginStep description]
 * @param  mode           [description]
 * @param  timeoutSeconds [description]
 * @return                [description]
 */
StepStatus JuleaTestWriter::BeginStep(StepMode mode, const float timeoutSeconds)
{
    // std::cout << "JULEA ENGINE: BeginStep" << std::endl;
    m_CurrentStep++; // 0 is the first step
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank
                  << "   BeginStep() new step " << m_CurrentStep << "\n";
    }
    m_DeferredVariables.clear();
    m_DeferredVariablesDataSize = 0;
    return StepStatus::OK;
}

/**
 * TODO
 * [JuleaTestWriter::CurrentStep description]
 * @return [description]
 */
size_t JuleaTestWriter::CurrentStep() const
{
    // std::cout << "JULEA ENGINE: CurrentStep" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank
                  << "   CurrentStep() returns " << m_CurrentStep << "\n";
    }
    return m_CurrentStep;
}

/**
 * TODO
 * [JuleaTestWriter::EndStep description]
 */
void JuleaTestWriter::EndStep()
{
    // std::cout << "JULEA ENGINE: EndStep" << std::endl;
    if (m_NeedPerformPuts)
    {
        PerformPuts();
    }
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << "   EndStep()\n";
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
 * available. [JuleaTestWriter::PerformPuts description]
 */
void JuleaTestWriter::PerformPuts()
{
    // std::cout << "JULEA ENGINE: PerformPuts" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << "     PerformPuts()\n";
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
            std::cout << "Julea Test Writer " << m_WriterRank << "     PerformPuts()"
                      << "compound variable type not supported \n";
        }
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
 * [JuleaTestWriter::Flush description]
 * @param transportIndex [description]
 */
void JuleaTestWriter::Flush(const int transportIndex)
{
    DoFlush(false, transportIndex);
    // ResetBuffer(m_Data);

    if (m_CollectiveMetadata)
    {
        // WriteCollectiveMetadataFile
    }

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << "   Flush()\n";
    }
}

/** --- PRIVATE FUNCTIONS --- */
/**
 * TODO
 * [JuleaTestWriter::Init description]
 */
void JuleaTestWriter::Init()
{
    std::cout << "\n*********************** JULEA TEST ENGINE WRITER "
                 "*************************"
              << std::endl;
    InitParameters();
    InitTransports();
    InitVariables();
}

/**TODO
 * [JuleaTestWriter::InitParameters description]
 */
void JuleaTestWriter::InitParameters()
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
        std::cout << "Julea Test Writer " << m_WriterRank << " InitParameters()\n";
    }
}

/**TODO
 * [JuleaTestWriter::InitTransports description]
 */
void JuleaTestWriter::InitTransports()
{
    // Nothing to process from m_IO.m_TransportsParameters
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << " InitTransports()\n";
    }
}

/**
 * [JuleaTestWriter::InitVariables description]
 */
void JuleaTestWriter::InitVariables()
{
    // m_DeferredVariables.init() FIXME: how to init set of strings?
    // constructur? Nothing to process from m_IO.m_TransportsParameters
    // std::cout << "JULEA ENGINE: Init Transport" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << " InitVariables()\n";
    }
}

/**TODO
 * [declare_type description]
 * @param  T [description]
 * @return   [description]
 */
#define declare_type(T)                                                        \
    void JuleaTestWriter::DoPutSync(Variable<T> &variable, const T *data)          \
    {                                                                          \
        PutSyncCommon(variable, data);                                         \
    }                                                                          \
    void JuleaTestWriter::DoPutDeferred(Variable<T> &variable, const T *data)      \
    {                                                                          \
        PutDeferredCommon(variable, data);                                     \
    }
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

/**TODO
 * [JuleaTestWriter::DoClose description]
 * @param transportIndex [description]
 */
void JuleaTestWriter::DoClose(const int transportIndex)
{
    // std::cout << "JULEA ENGINE: Do close" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << " Close(" << m_Name
                  << ")\n";
    }
}

/**TODO
 * [JuleaTestWriter::DoFlush description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
void JuleaTestWriter::DoFlush(const bool isFinal, const int transportIndex)
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
        std::cout << "Julea Test Writer " << m_WriterRank << " DoFlush \n";
    }
}

/**
 * TODO
 * [JuleaTestWriter::WriteData description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
void JuleaTestWriter::WriteData(const bool isFinal, const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << " WriteData\n";
    }
}

/**
 * TODO
 * [JuleaTestWriter::AggregateWriteData description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
void JuleaTestWriter::AggregateWriteData(const bool isFinal,
                                     const int transportIndex)
{
    // DESIGN: check BP3Writer
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Writer " << m_WriterRank << " AggregateWriteData\n";
        std::cout << " Data aggregation for writing not yet supported."
                  << std::endl;
    }
}

/**
 *  Put attributes held in passed IO. Called from EndStep()
 * @param io [description]
 */
void JuleaTestWriter::PutAttributes(core::IO &io)
{

    const auto attributesDataMap = io.GetAttributesDataMap();

    for (const auto &attributePair : attributesDataMap)
    {
//         // each attribute is only written to output once
//         // so filter out the ones already written
//         auto it = m_SerializedAttributes.find(attr_metadata->name);
//         if (it != m_SerializedAttributes.end())
//         {
//             continue;
//         }

//         if (type == "unknown")
//         {
//             std::cout << "Attribute type is 'unknown' " << std::endl;
//         }
// #define declare_type(T)                                                        \
//     else if (type == helper::GetType<T>())                                     \
//     {                                                                          \
//         core::Attribute<T> &attribute = *io.InquireAttribute<T>(name);         \
//         attr_metadata->is_single_value = attribute.m_IsSingleValue;            \
//         if (attribute.m_IsSingleValue)                                         \
//         {                                                                      \
//             PutAttributeToJulea(m_JuleaInfo->name_space, attr_metadata,        \
//                                 &attribute.m_DataSingleValue, batch);          \
//         }                                                                      \
//         else                                                                   \
//         {                                                                      \
//             PutAttributeToJulea(m_JuleaInfo->name_space, attr_metadata,        \
//                                 &attribute.m_DataArray, batch);                \
//         }                                                                      \
//     }
//         ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(declare_type)
// #undef declare_type

    }
}


} // end namespace engine
} // end namespace core
} // end namespace adios2
