/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JuleaDBDAIWRITER_H_
#define ADIOS2_ENGINE_JuleaDBDAIWRITER_H_

#include "adios2/core/Engine.h"
// #include "adios2/toolkit/format/bp3/BP3.h" //BP3Serializer
// #include "adios2/toolkit/format/bp/bp3/BP3Serializer.h"
// #include "adios2/toolkit/interop/julea/JuleaSerializer.h"
#include "adios2/toolkit/interop/julea/Database/JuleaDBInteractionWriter.h"
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

#include "adios2/toolkit/transportman/TransportMan.h" //transport::TransportsMan

#include <complex.h>
#include <glib.h>
#include <julea.h>

// #include "julea.h"

//#include <jgmm.h>

namespace adios2
{
namespace core
{
namespace engine
{

class JuleaDBDAIWriter : public core::Engine
{

public:
    /**
     * Constructor for Writer
     * @param name unique name given to the engine
     * @param accessMode
     * @param mpiComm
     * @param method
     * @param debugMode
     */
    JuleaDBDAIWriter(IO &adios, const std::string &name, const Mode mode,
                     helper::Comm comm);

    ~JuleaDBDAIWriter(); // was =default -> meaning?

    StepStatus BeginStep(StepMode mode,
                         const float timeoutSeconds = -1.0) final;
    size_t CurrentStep() const final;
    void PerformPuts() final;
    void EndStep() final;
    void Flush(const int transportIndex = -1) final;

    // interop::JuleaInteraction m_JuleaDBInteractionWriter;
private:
    interop::JuleaDBInteractionWriter m_JuleaDBInteractionWriter;
    // interop::JuleaSerializer m_JuleaSerializer;
    // interop::JuleaDBInteraction m_JuleaDBInteractionWriter;
    // interop::JuleaDBInteractionWriter m_JuleaDBInteractionWriter;
    // interop::JuleaInteraction m_JuleaDBInteractionWriter;

    JSemantics *m_JuleaSemantics;
    StepMode m_StepMode = StepMode::Append;

    int m_Verbosity = 5; // change for debugging info from 0 to 5
    int m_Penguin = 0;   // change for debugging info from 0 to 42
    int m_WriterRank;    // my rank in the writers' comm

    /** true: Close was called, Engine will call this many times for different
     * transports */
    bool m_IsClosed = false;

    /** Default: write collective metadata in Capsule metadata. */
    bool m_CollectiveMetadata = true;

    /** Used for streaming a large number of steps */
    size_t m_CurrentStep = 0; // starts at 0

    size_t m_CurrentBlockID = 0; // starts at 0

    /** Parameter to flush transports at every number of steps, to be used at
     * EndStep */
    size_t m_FlushStepsCount = 1;

    size_t m_DayIntervall = 24;

    /** CDO stuff*/
    // Temperature buffer
    std::vector<double> m_DailyTempsBuffer;     // 24 hours
    std::vector<double> m_MonthlyTempsBuffer;   //30 days

    // precipitation buffer
    std::vector<double> m_DailyPrecipsBuffer;   // 24 hour
    std::vector<double> m_MonthlyPrecipsBuffer; // 30 days


    /** manages all communication tasks in aggregation */
    // aggregator::MPIChain m_Aggregator;

    /** tracks Put and Get variables in deferred mode */
    std::set<std::string> m_DeferredVariables;

    /** tracks all variables written (not BP, new for JULEA)
    This way the kv-store does not need to be asked every time if a variable
    name is already in the BSON */
    std::set<std::string> m_WrittenVariableNames;

    /** attributes are serialized only once, this set contains the names of ones
     * already serialized.
     */
    // std::unordered_set<std::string> m_SerializedAttributes; // TODO: needed?

    /** statistics verbosity, only 0 is supported */
    unsigned int m_StatsLevel = 0;

    void Init() final;

    /** Parses parameters from IO SetParameters */
    void InitParameters() final;

    void InitVariables();
    void InitDB();

#define declare_type(T)                                                        \
    void DoPutSync(Variable<T> &variable, const T *) final;                    \
    void DoPutDeferred(Variable<T> &variable, const T *) final;
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    template <class T>
    void PutSyncCommon(Variable<T> &variable,
                       const typename Variable<T>::Info &blockInfo);

    template <class T>
    void PutSyncCommon(Variable<T> &variable, const T *values);

    template <class T>
    void PutSyncToJulea(Variable<T> &variable, const T *values,
                        const typename Variable<T>::Info &blockInfo);

    template <class T>
    void PutDeferredCommon(Variable<T> &variable, const T *values);

    void DoFlush(const bool isFinal = false, const int transportIndex = -1);

    template <class T>
    void SetBlockID(Variable<T> &variable);

    template <class T>
    void JuleaDBDAISetMinMax(Variable<T> &variable, const T *data, T &blockMin,
                             T &blockMax, T &blockMean, size_t currentStep,
                             size_t currentBlockID);

    /**
     * Closes a single transport or all transports
     * @param transportIndex, if -1 (default) closes all transports,
     * otherwise it closes a transport in m_Transport[transportIndex].
     * In debug mode the latter is bounds-checked.
     */
    void DoClose(const int transportIndex = -1) final;

    /**
     * Put Attributes to file.
     * @param io [description]
     */
    void PutAttributes(core::IO &io);

    template <class T>
    void PerformPutCommon(Variable<T> &variable);

    void InitParameterFlushStepsCount(const std::string value);
};

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JuleaDBDAIWRITER_H_ */
