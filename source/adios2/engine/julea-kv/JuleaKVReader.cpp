/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaKVReader.h"
// #include "JuleaKVDAIInteractionReader.h"
#include "JuleaKVReader.tcc"

#include "adios2/helper/adiosFunctions.h" // CSVToVector

#include <iostream>

#include <glib.h>
#include <julea-object.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

// JuleaKVReader::JuleaKVReader(IO &io, const std::string &name, const
// Mode mode,
//                                MPI_Comm mpiComm)
// : Engine("JuleaKVReader", io, name, mode, mpiComm),
//   m_BP3Deserializer(mpiComm, m_DebugMode)

JuleaKVReader::JuleaKVReader(IO &io, const std::string &name, const Mode mode,
                             helper::Comm comm)
: Engine("JuleaKVReader", io, name, mode, std::move(comm)),
  m_JuleaKVInteractionReader(m_Comm)

{
    // m_EndMessage = " in call to IO Open JuleaKVReader " + m_Name + "\n";
    // MPI_Comm_rank(mpiComm, &m_ReaderRank);
    m_ReaderRank = m_Comm.Rank();
    m_SizeMPI = m_Comm.Size();
    // if (m_ReaderRank == 0)
    // {
    // std::cout << "Init - " << m_ReaderRank << std::endl;
    Init();
    // std::cout << "Init finished - " << m_ReaderRank << std::endl;
    // }
    // m_Comm.Barrier();

    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank << " Open(" << m_Name
                  << ") in constructor." << std::endl;
    }

    // std::map<std::string, Params> GetAvailableVariables() noexcept;
    // DataMap variables = io.GetAvailableVariables();
}

JuleaKVReader::~JuleaKVReader()
{
    /* m_Skeleton deconstructor does close and finalize */
    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank << " deconstructor on "
                  << m_Name << "\n";
    }
}

StepStatus JuleaKVReader::BeginStep(const StepMode mode,
                                    const float timeoutSeconds)
{
    // if (m_DebugMode)
    // {
    // FIXME: NextAvailable is no longer a StepMode
    // if (mode != StepMode::NextAvailable)
    // {
    //     throw std::invalid_argument(
    //         "ERROR: mode is not supported yet, "
    //         "only NextAvailable is valid for "
    //         "engine BP3 with adios2::Mode::Read, in call to "
    //         "BeginStep\n");
    // }

    if (!m_DeferredVariables.empty())
    {
        throw std::invalid_argument(
            "ERROR: existing variables subscribed with "
            "GetDeferred, did you forget to call "
            "PerformGets() or EndStep()?, in call to BeginStep\n");
    }
    // }
    m_IO.m_ReadStreaming = true;
    m_IO.m_EngineStep = m_CurrentStep;

    m_StepMode = mode;
    m_DeferredVariables.clear();
    m_DeferredVariablesDataSize = 0;

    // first param is "zero-init" which initializes stepsStart to 0
    m_IO.ResetVariablesStepSelection(false,
                                     "in call to JULEA Reader BeginStep");

    return StepStatus::OK;
}

size_t JuleaKVReader::CurrentStep() const
{
    // std::cout << "JULEA ENGINE: CurrentStep" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank
                  << "   CurrentStep() returns " << m_CurrentStep << "\n";
    }
    return m_CurrentStep;
}

void JuleaKVReader::EndStep()
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________EndStep _____________________"
                  << std::endl;
        std::cout << "JKV Reader " << m_ReaderRank << "   EndStep()\n";
    }

    if (m_DeferredVariables.size() > 0)
    {
        // std::cout << "m_DeferredVariables.size() = "
        // << m_DeferredVariables.size() << std::endl;
        PerformGets();
    }
    ++m_CurrentStep;
    m_CurrentBlockID = 0;
}

void JuleaKVReader::PerformGets()
{
    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank << "     PerformGets()\n";
    }

    /** if there are no deferred variables there is nothing to do */
    if (m_DeferredVariables.empty())
    {
        return;
    }
    size_t i = 0;
    /** Call GetSyncCommon for every variable that has been deferred */
    for (const std::string &variableName : m_DeferredVariables)
    {
        const adios2::DataType type = m_IO.InquireVariableType(variableName);
        // std::cout << "the data type in PerfomGets() is: " << type <<
        // std::endl;

        if (type == DataType::Compound)
        {
            // not supported
            std::cout << "Julea Reader " << m_ReaderRank << "     PerformGets()"
                      << "compound variable type not supported \n";
        }
#define declare_type(T)                                                        \
    else if (type == helper::GetDataType<T>())                                 \
    {                                                                          \
        Variable<T> &variable = FindVariable<T>(                               \
            variableName, "in call to PerformGets, EndStep or Close");         \
        if (variable.m_ShapeID == ShapeID::GlobalValue ||                      \
            variable.m_ShapeID == ShapeID::GlobalArray)                        \
        {                                                                      \
            ReadVariableBlocks(variable);                                      \
        }                                                                      \
        else if (variable.m_ShapeID == ShapeID::LocalArray ||                  \
                 variable.m_ShapeID == ShapeID::LocalValue)                    \
        {                                                                      \
            for (auto &blockInfo : variable.m_BlocksInfo)                      \
            {                                                                  \
                T *data = variable.m_BlocksInfo[i].Data;                       \
                ReadBlock(variable, data, i);                                  \
                i++;                                                           \
            }                                                                  \
        }                                                                      \
        variable.m_BlocksInfo.clear();                                         \
    }
        // ADIOS2_FOREACH_TYPE_1ARG(declare_type) //TODO:Why is this different
        // from Writer?
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
    }
    m_DeferredVariables.clear();
    // ReadVariableBlocks(variable);                                          \
        //
}

#define declare_type(T)                                                        \
    void JuleaKVReader::DoGetSync(Variable<T> &variable, T *data)              \
    {                                                                          \
        GetSyncCommon(variable, data);                                         \
    }                                                                          \
    void JuleaKVReader::DoGetDeferred(Variable<T> &variable, T *data)          \
    {                                                                          \
        GetDeferredCommon(variable, data);                                     \
    }
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

void JuleaKVReader::Init()
{
    if (m_Penguin == 42)
    {
        std::cout << "\n*********************** JULEA ENGINE READER "
                     "*************************"
                  << std::endl;
        std::cout << "JKV READER: Init" << std::endl;
        std::cout
            << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
            << std::endl;
        std::cout << "JKV READER: Namespace = " << m_Name << std::endl;
    }

    // if (m_DebugMode)
    // {
    if (m_OpenMode != Mode::Read)
    {
        throw std::invalid_argument(
            "ERROR: JuleaReader only supports OpenMode::Read from" + m_Name +
            " " + m_EndMessage);
    }
    // }
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << " Init()\n";
    }
    m_JuleaSemantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

    // InitParameters();
    // InitTransports();
    // m_JuleaKVInteractionReader.CheckSchemas(m_ProjectNamespace);
    InitVariables();
    // InitAttributes(); //TODO
}

/**
 * Initializes variables so that InquireVariable can find the variable in the IO
 * map. Since the m_Variables is private the only solution is to define each
 * variable with the according parameters.
 */
// template <class T>
void JuleaKVReader::InitVariables()
{
    // bson_iter_t b_iter;
    // bson_t *bsonNames;
    // std::string varName;
    // std::string nameSpace = m_Name;
    // std::string kvName = "variable_names";
    // unsigned int varCount = 0;

    // GetNamesFromJulea(nameSpace, &bsonNames, &varCount, true);
    m_JuleaKVInteractionReader.InitVariablesFromKV(m_ProjectNamespace, m_Name,
                                                   &m_IO, *this);
    
}

void JuleaKVReader::InitParameters()
{
    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank << " InitParameters()\n";
    }
}

void JuleaKVReader::InitTransports()
{
    // Nothing to process from m_IO.m_TransportsParameters
    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank << " InitTransports()\n";
    }
}

void JuleaKVReader::DoClose(const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        std::cout << "JKV Reader " << m_ReaderRank << " Close(" << m_Name
                  << ")\n";
    }
}

#define declare_type(T)                                                        \
    std::map<size_t, std::vector<typename Variable<T>::Info>>                  \
    JuleaKVReader::DoAllStepsBlocksInfo(const Variable<T> &variable) const     \
    {                                                                          \
        return AllStepsBlocksInfo(variable);                                   \
    }                                                                          \
    std::vector<std::vector<typename Variable<T>::Info>>                       \
    JuleaKVReader::DoAllRelativeStepsBlocksInfo(const Variable<T> &variable)   \
        const                                                                  \
    {                                                                          \
        return AllRelativeStepsBlocksInfo(variable);                           \
    }                                                                          \
                                                                               \
    std::vector<typename Variable<T>::Info> JuleaKVReader::DoBlocksInfo(       \
        const Variable<T> &variable, const size_t step) const                  \
    {                                                                          \
        return BlocksInfo(variable, step);                                     \
    }

ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
} // end namespace engine
} // end namespace core
} // end namespace adios2
