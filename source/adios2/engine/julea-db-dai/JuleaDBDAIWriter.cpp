/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaDBDAIWriter.h"
#include "JuleaDBDAIWriter.tcc"

#include "adios2/core/IO.h"
#include "adios2/helper/adiosFunctions.h"
#include "adios2/toolkit/transport/file/FileFStream.h"

#include <iostream>
#include <julea-object.h> //needed?

// #include <julea-adios.h>

namespace adios2
{
namespace core
{
namespace engine
{

// JuleaDBDAIWriter::JuleaDBDAIWriter(IO &io, const std::string &name, const
// Mode mode,
//                              helper::Comm comm)
// : Engine("JuleaDBDAIWriter", io, name, mode, std::move(comm)),
// m_JuleaDBInteractionWriter(m_Comm)

JuleaDBDAIWriter::JuleaDBDAIWriter(IO &io, const std::string &name,
                                   const Mode mode, helper::Comm comm)
: Engine("JuleaDBDAIWriter", io, name, mode, std::move(comm)),
  m_JuleaDBInteractionWriter(m_Comm)
{
    m_WriterRank = m_Comm.Rank();
    Init();
    if (m_Verbosity == 5)
    {
        m_Comm.Barrier();
        // std::cout << "Julea DB Writer " << m_WriterRank << " Open(" << m_Name
                //   << ") in constructor." << std::endl;
        std::cout << "JDB Writer (" << m_WriterRank << ") : Open(" << m_Name
                  << ")." << std::endl;
    }
}

JuleaDBDAIWriter::~JuleaDBDAIWriter()
{
    // DoClose();
    // if (m_Verbosity == 5)
    if (m_Verbosity == 5)
    {
        // std::cout << "Julea DB Writer " << m_WriterRank << " deconstructor on "
                //   << m_Name << "\n";
        std::cout << "JDB Writer (" << m_WriterRank << ") : deconstructor on " << m_Name << " \n";
    }
    
}

/**
 * Begins a step. Clears the deferred variable set.
 */
StepStatus JuleaDBDAIWriter::BeginStep(StepMode mode,
                                       const float timeoutSeconds)
{
    if (m_Verbosity == 5)
    {   
        if(m_WriterRank == 0)
        {
        std::cout << "\n______________BeginStep _____________________ step = " << m_CurrentStep 
                  << std::endl;
        }
        m_Comm.Barrier();
        std::cout << "JDB Writer (" << m_WriterRank << ") : BeginStep() " << "\n";
        // std::cout << "JDB Writer (" << m_WriterRank << ") : BeginStep() --- step = " << m_CurrentStep << "\n";

        // std::cout << "Julea DB Writer " << m_WriterRank
        //           << "   BeginStep() new step " << m_CurrentStep << "\n";
        // std::cout << "StepMode mode: " << mode << std::endl;
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
size_t JuleaDBDAIWriter::CurrentStep() const
{
    // std::cout << "JULEA ENGINE: CurrentStep" << std::endl;
    if (m_Verbosity == 5)
    {
        // std::cout << "Julea DB Writer " << m_WriterRank
                //   << "   CurrentStep() returns " << m_CurrentStep << "\n";
        std::cout << "JDB Writer (" << m_WriterRank << ") : CurrentStep() --- step = " << m_CurrentStep << "\n";

    }
    return m_CurrentStep;
}

/**
 * Ends the current step. All deferred variables are written now.
 */
void JuleaDBDAIWriter::EndStep()
{
    // std::cout << "--- DEBUG : EndStep1" << std::endl;
    // if (m_NeedPerformPuts)

    if (m_Verbosity == 5)
{
    std::cout << "JDB Writer (" << m_WriterRank << ") : EndStep()\n";
}


    if (m_DeferredVariables.size() > 0)
    {
        // std::cout << "--- DEBUG : EndStep" << std::endl;
        // std::cout << "m_DeferredVariables.size() = "
        // << m_DeferredVariables.size() << std::endl;
        PerformPuts();
    }

    PutAttributes(m_IO);

    ++m_CurrentStep;

    if (m_CurrentStep % m_FlushStepsCount == 0)
    {
        Flush();
    }
    m_CurrentBlockID = 0;

    // if (m_Verbosity == 5)
    // {
    //     std::cout << "\n______________EndStep _____________________"
    //               << std::endl;
    // }
     m_Comm.Barrier();
        if(m_WriterRank == 0)
        {
        std::cout << "______________EndStep _____________________ step = " << (m_CurrentStep - 1)
                  << "\n "<< std::endl;
        }
}

/**
 * Called to guarantee that read/write are really executed and the results
 * available.
 */
void JuleaDBDAIWriter::PerformPuts()
{
    if (m_Verbosity == 5)
    {
        // std::cout << "Julea DB Writer " << m_WriterRank
                //   << "     PerformPuts()\n";
        std::cout << "JDB Writer (" << m_WriterRank << ") : PerformPuts()\n";
    }

    /** if there are no deferred variables there is nothing to do */
    if (m_DeferredVariables.empty())
    {
        // std::cout << "deferred variables are empty " << std::endl;
        // TODO: blockID not set correctly when using put sync without
        // begin/end step and without bpls just with perform puts
        m_CurrentBlockID = 0;
        return;
    }

    /** Call PutSyncCommon for every variable that has been deferred */
    for (const std::string &variableName : m_DeferredVariables)
    {
        const DataType type = m_IO.InquireVariableType(variableName);
        if (type == DataType::Compound)
        {
            // not supported
            std::cout << "Julea DB Writer " << m_WriterRank
                      << "     PerformPuts()"
                      << "compound variable type not supported \n";
        }
#define declare_template_instantiation(T)                                      \
    else if (type == helper::GetDataType<T>())                                 \
    {                                                                          \
        Variable<T> &variable = FindVariable<T>(                               \
            variableName, "in call to PerformPuts, EndStep or Close");         \
                                                                               \
        PerformPutCommon(variable);                                            \
    }
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation
    }
    m_DeferredVariables.clear();
    m_CurrentBlockID = 0;
    // std::cout << "--- PerformPuts -- " << std::endl;
}
// ADIOS2_FOREACH_TYPE_1ARG(declare_template_instantiation)

/**
 * Flushes the aggregated data.
 * @param transportIndex [description]
 */
// void JuleaDBDAIWriter::Flush()
void JuleaDBDAIWriter::Flush(const int transportIndex)
{

    if (m_Verbosity == 5)
    {
        // std::cout << "\n______________Flush  _____________________"
        // std::cout << "\n___Flush___"
                //   << std::endl;
        // std::cout << "Julea DB Writer " << m_WriterRank << "   Flush()\n";
        std::cout << "JDB Writer (" << m_WriterRank << ") : Flush()\n";
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
void JuleaDBDAIWriter::Init()
{
    // if (m_Verbosity == 5)
    // {
    //     std::cout << "Julea DB Writer " << m_WriterRank << "   Init() "
    //               << std::endl;
    // }
        if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (" << m_WriterRank
                  << ") : Init()\n";
    }

    if (m_OpenMode == Mode::Append)
    {
        throw std::invalid_argument(
            "JuleaDBDAIWriter: OpenMode   -- Append --   "
            "hasn't been implemented, yet");
    }
    if (m_Penguin == 42)
    {
        std::cout << "\n*********************** JULEA ENGINE WRITER "
                     "*************************"
                  << std::endl;
        std::cout << "JULEA DB WRITER: Init" << std::endl;
        std::cout
            << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
            << std::endl;

        std::cout << "JULEA WRITER: Namespace = " << m_Name << std::endl;
    }

    m_JuleaSemantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

    InitParameters();
    if (m_WriterRank == 0)
    {
        std::cout << "JDB Writer (" << m_WriterRank << ") : InitDBSchemas()\n";
        // std::cout << "InitDBSchemas" << std::endl;
        
        m_JuleaDBInteractionWriter.InitDBSchemas();
        // Init();
        std::cout << "JDB Writer (" << m_WriterRank << ") : InitDBSchemas finished()\n";
    }
    // DAIInitDBSchemas();
    //TODO: nothing happening in InitVariables
    // InitVariables();
}

// void JuleaDBDAIWriter::InitDB() { m_JuleaDBInteractionWriter.InitDBSchemas(); }

/**TODO needed?
 * see BP3Base InitParameters
 */
void JuleaDBDAIWriter::InitParameters()
{
    if (m_Verbosity == 5)
    {
        std::cout << "JDB Writer (Rank " << m_WriterRank
                  << ") : InitParameters()\n";
    }
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
            // if (m_DebugMode)
            // {
            if (m_Verbosity < 0 || m_Verbosity > 5)
                throw std::invalid_argument(
                    "ERROR: Method verbose argument must be an "
                    "integer in the range [0,5], in call to "
                    "Open or Engine constructor\n");
            // }
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
}

/**
 * [JuleaWriter::InitVariables description]
 */
void JuleaDBDAIWriter::InitVariables()
{
    // TODO: do something here with deferredVariables?
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank << " InitVariables()\n";
    }
}

/**
 * Puts variable to JULEA object store. Afterwards deletes related blockinfo
 * struct from variable m_BlocksInfo vector [declare_type description]
 * @param variable      variable
 * @param data          variable data
 */

#define declare_type(T)                                                        \
    void JuleaDBDAIWriter::DoPutSync(Variable<T> &variable, const T *data)     \
    {                                                                          \
        if (m_Verbosity == 5)\
        {\
            std::cout << "JDB Writer (" << m_WriterRank << ") : DoPutSync()\n";\
        }\
        SetBlockID(variable);                                                  \
        PutSyncCommon(variable, data);                                         \
        variable.m_BlocksInfo.pop_back();                                      \
        m_CurrentBlockID++;                                                    \
    }                                                                          \
    void JuleaDBDAIWriter::DoPutDeferred(Variable<T> &variable, const T *data) \
    {                                                                          \
    if (m_Verbosity == 5)\
        {\
            std::cout << "JDB Writer (" << m_WriterRank << ") : DoPutDeferred()\n";\
        }\
        PutDeferredCommon(variable, data);                                     \
    }
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

/**TODO
 * [JuleaWriter::DoClose description]
 * @param transportIndex [description]
 */
void JuleaDBDAIWriter::DoClose(const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        // std::cout << "\n______________DoClose_____________________"
        // std::cout << "\n___DoClose___"
                //   << std::endl;
        // std::cout << "Julea DB Writer " << m_WriterRank << " Close(" << m_Name
                //   << ")\n";
        std::cout << "JDB Writer (" << m_WriterRank << ") : DoClose()\n";

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

/**TODO: necessary?
 * [JuleaWriter::DoFlush description]
 * @param isFinal        [description]
 * @param transportIndex [description]
 */
// void JuleaDBDAIWriter::DoFlush(const bool isFinal)
void JuleaDBDAIWriter::DoFlush(const bool isFinal, const int transportIndex)
{
    // if (m_Verbosity == 5)
    // {
    //     // std::cout << "\n______________DoFlush_____________________"
    //     std::cout << "\n___DoFlush___"
    //               << std::endl;
    //     // std::cout << "Julea DB Writer " << m_WriterRank << " DoFlush \n";
    //     std::cout << "JDB Writer (" << m_WriterRank << ") : DoFlush()\n";

    // }
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
void JuleaDBDAIWriter::PutAttributes(core::IO &io)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n --- Put Attributes currently not implemented!"
                  << std::endl;
    }
}

void JuleaDBDAIWriter::InitParameterFlushStepsCount(const std::string value)
{
    if (m_Verbosity == 5)
{
    std::cout << "JDB Writer (" << m_WriterRank << ") : InitParameterFlushStepsCount()\n";
}

    long long int flushStepsCount = -1;

    // if (m_DebugMode)
    // {
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
    // }
    // else //FIXME: is this still necessary?
    // {
    //     flushStepsCount = std::stoll(value);
    // }

    m_FlushStepsCount = static_cast<size_t>(flushStepsCount);
}

} // end namespace engine
} // end namespace core
} // end namespace adios2
