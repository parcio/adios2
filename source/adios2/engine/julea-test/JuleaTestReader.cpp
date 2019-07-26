/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaTestReader.h"
#include "JuleaTestReader.tcc"

#include "adios2/helper/adiosFunctions.h" // CSVToVector

#include <iostream>

namespace adios2
{
namespace core
{
namespace engine
{

// JuleaTestReader::JuleaTestReader(IO &io, const std::string &name, const Mode mode,
//                                MPI_Comm mpiComm)
// : Engine("JuleaTestReader", io, name, mode, mpiComm),
//   m_BP3Deserializer(mpiComm, m_DebugMode)

JuleaTestReader::JuleaTestReader(IO &io, const std::string &name, const Mode mode,
                         MPI_Comm mpiComm)
: Engine("JuleaTestReader", io, name, mode, mpiComm)

{
    // m_EndMessage = " in call to IO Open JuleaTestReader " + m_Name + "\n";
    MPI_Comm_rank(mpiComm, &m_ReaderRank);
    Init();
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << " Open(" << m_Name
                  << ") in constructor." << std::endl;
    }

    // std::map<std::string, Params> GetAvailableVariables() noexcept;
    // DataMap variables = io.GetAvailableVariables();
}

JuleaTestReader::~JuleaTestReader()
{
    /* m_Skeleton deconstructor does close and finalize */
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << " deconstructor on "
                  << m_Name << "\n";
    }
}

StepStatus JuleaTestReader::BeginStep(const StepMode mode,
                                  const float timeoutSeconds)
{
    if (m_DebugMode)
    {
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
    }
}

size_t JuleaTestReader::CurrentStep() const
{
    // std::cout << "JULEA ENGINE: CurrentStep" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank
                  << "   CurrentStep() returns " << m_CurrentStep << "\n";
    }
    return m_CurrentStep;
}

void JuleaTestReader::EndStep()
{
    // EndStep should call PerformGets() if there are unserved GetDeferred()
    // requests
    if (m_NeedPerformGets)
    {
        PerformGets();
    }

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << "   EndStep()\n";
    }
    // TODO: Can reading happen in steps?
    // if (m_CurrentStep % m_FlushStepsCount == 0){
    //     Flush();
    // }
}


void JuleaTestReader::PerformGets()
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << "     PerformGets()\n";
    }

    /** if there are no deferred variables there is nothing to do */
    if (m_DeferredVariables.empty())
    {
        return;
    }

    /** Call GetSyncCommon for every variable that has been deferred */
    for (const std::string &variableName : m_DeferredVariables)
    {
        const std::string type = m_IO.InquireVariableType(variableName);

        if (type == "compound")
        {
            // not supported
            std::cout << "Julea Test Reader " << m_ReaderRank << "     PerformGets()"
                      << "compound variable type not supported \n";
        }
#define declare_type(T)                                                        \
    else if (type == helper::GetType<T>())                                     \
    {                                                                          \
        Variable<T> &variable = FindVariable<T>(                               \
            variableName, "in call to PerformGets, EndStep or Close");         \
        for (auto &blockInfo : variable.m_BlocksInfo)                          \
        {                                                                      \
            GetSyncCommon(variable, variable.m_Data);                          \
        }                                                                      \
        variable.m_BlocksInfo.clear();                                         \
    }
        // ADIOS2_FOREACH_TYPE_1ARG(declare_type) //TODO:Why is this different
        // from Writer?
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
    }
    m_DeferredVariables.clear();
    m_NeedPerformGets = false; // TODO: needed?
}

// PRIVATE

#define declare_type(T)                                                        \
    void JuleaTestReader::DoGetSync(Variable<T> &variable, T *data)                \
    {                                                                          \
        GetSyncCommon(variable, data);                                         \
    }                                                                          \
    void JuleaTestReader::DoGetDeferred(Variable<T> &variable, T *data)            \
    {                                                                          \
        GetDeferredCommon(variable, data);                                     \
    }
// ADIOS2_FOREACH_TYPE_1ARG(declare_type)
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

void JuleaTestReader::Init()
{
    std::cout << "\n*********************** JULEA TEST ENGINE READER "
                 "*************************"
              << std::endl;
    InitParameters();
    InitTransports();
    InitVariables();
}

/**
 * Initializes variables so that InquireVariable can find the variable in the IO
 * map. Since the m_Variables is private the only solution is to define each
 * variable with the according parameters.
 */
// template <class T>
void JuleaTestReader::InitVariables()
{
    gchar **names;
    int *types;
    unsigned int variable_count = 0;
    int size = 0;

    /* Get variables from storage*/

    for (unsigned int i = 0; i < variable_count; i++)
    {
        Dims shape;
        Dims start;
        Dims count;


        switch (types[i])
        {
        // case COMPOUND:
        //     //TODO
        //     break;
        // case UNKNOWN:
        //     //TODO
        //     break;
        // case STRING:
        //     m_IO.DefineVariable<std::string>(names[i], shape, start, count,
        //                                      constantdims);
        //     break;
        // case INT8:
        //     m_IO.DefineVariable<int8_t>(names[i], shape, start, count,
        //                                 constantdims);
        //     break;
        // case UINT8:
        //     m_IO.DefineVariable<uint8_t>(names[i], shape, start, count,
        //                                  constantdims);
        //     break;
        // case INT16:
        //     m_IO.DefineVariable<int16_t>(names[i], shape, start, count,
        //                                  constantdims);
        //     break;
        // case UINT16:
        //     m_IO.DefineVariable<uint16_t>(names[i], shape, start, count,
        //                                   constantdims);
        //     break;
        // case INT32:
        //     m_IO.DefineVariable<int32_t>(names[i], shape, start, count,
        //                                  constantdims);
        //     break;
        // case UINT32:
        //     m_IO.DefineVariable<uint32_t>(names[i], shape, start, count,
        //                                   constantdims);
        //     break;
        // case INT64:
        //     m_IO.DefineVariable<int64_t>(names[i], shape, start, count,
        //                                  constantdims);
        //     break;
        // case UINT64:
        //     m_IO.DefineVariable<uint64_t>(names[i], shape, start, count,
        //                                   constantdims);
        //     break;
        // case FLOAT:
        //     m_IO.DefineVariable<float>(names[i], shape, start, count,
        //                                constantdims);
        //     break;
        // case DOUBLE:
        //     m_IO.DefineVariable<double>(names[i], shape, start, count,
        //                                 constantdims);
        //     break;
        // case LONG_DOUBLE:
        //     m_IO.DefineVariable<long double>(names[i], shape, start, count,
        //                                      constantdims);
            break;
            // case FLOAT_COMPLEX:
            //     //TODO
            //     break;
            // case DOUBLE_COMPLEX:
            //     //TODO
            //     break;
        }
    }
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << " InitVariables()\n";
    }
}

void JuleaTestReader::InitParameters()
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << " InitParameters()\n" ;
    }
}

void JuleaTestReader::InitTransports()
{
    // Nothing to process from m_IO.m_TransportsParameters
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << " InitTransports()\n";
    }
}

void JuleaTestReader::DoClose(const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Test Reader " << m_ReaderRank << " Close(" << m_Name
                  << ")\n";
    }
}
} // end namespace engine
} // end namespace core
} // end namespace adios2
