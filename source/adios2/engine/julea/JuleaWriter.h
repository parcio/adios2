/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

// FIXME: Why is it ...MPIWRITER...? But not ...MPIREADER?
#ifndef ADIOS2_ENGINE_JULEAMPIWRITER_H_
#define ADIOS2_ENGINE_JULEAMPIWRITER_H_

#include "JuleaMetadata.h"

#include "adios2/core/Engine.h"
#include "adios2/toolkit/format/bp3/BP3.h" //BP3Serializer
#include "adios2/toolkit/format/bp3/BP3Serializer.h"
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

class JuleaWriter : public Engine
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
    JuleaWriter(IO &adios, const std::string &name, const Mode mode,
                MPI_Comm mpiComm);

    ~JuleaWriter(); // was =default -> meaning?

    // TODO: why is there no set StepMode in the Write engine?
    StepStatus BeginStep(StepMode mode,
                         const float timeoutSeconds = -1.0) final;
    size_t CurrentStep() const final;
    void PerformPuts() final;
    void EndStep() final;
    void Flush(const int transportIndex = -1) final;

private:
    JuleaInfo *m_JuleaInfo;

    int m_Verbosity = 5;       // changed for debugging info from 0 to 5
    int m_WriterRank;          // my rank in the writers' comm
    size_t m_CurrentStep = -1; // steps start from 0

    /** EndStep must call PerformPuts if necessary */
    bool m_NeedPerformPuts = false; // DESIGN: suggested in SkeletonWriter

    /** TODO: needed? */
    bool m_Flushed = false; // DESIGN: used in HDF5Writer

    /**  --- DESIGN: the following is similar to BP3Writer and BP3Base --- */

    /** Default: write collective metadata in Capsule metadata. */
    bool m_CollectiveMetadata = true;

    /** Parameter to flush transports at every number of steps, to be used at
     * EndStep */
    size_t m_FlushStepsCount = 1;

    /** manages all communication tasks in aggregation */
    aggregator::MPIChain m_Aggregator;

    /** tracks Put and Get variables in deferred mode */
    std::set<std::string> m_DeferredVariables;

    /** attributes are serialized only once, this set contains the names of ones
     * already serialized.
     */
    std::unordered_set<std::string> m_SerializedAttributes; // TODO: needed?

    /** tracks the overall size of deferred variables */
    size_t m_DeferredVariablesDataSize = 0;

    /** statistics verbosity, only 0 is supported */
    unsigned int m_StatsLevel = 0;

    /** contains data buffer for this rank */
    BufferSTL m_Data;

    /** contains collective metadata buffer, only used by rank 0 */
    BufferSTL m_Metadata;

    // DESIGN
    /** Manage BP data files Transports from IO AddTransport */
    // transportman::TransportMan m_FileDataManager; //FIXME: compiler?!

    /** Manages the optional collective metadata files */
    // transportman::TransportMan m_FileMetadataManager; FIXME: compiler?!

    void Init() final;

    /** Parses parameters from IO SetParameters */
    void InitParameters() final;
    /** Parses transports and parameters from IO AddTransport */
    void InitTransports() final;

    void InitVariables();

//FIXME: const T* BlockInfo oder const T * values?
#define declare_type(T)                                                        \
    void DoPutSync(Variable<T> &variable, const T *) final;                            \
    void DoPutDeferred(Variable<T> &variable, const T *) final;
    // ADIOS2_FOREACH_TYPE_1ARG(declare_type)
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    /**
     * TODO: why has skeleton engine Info struct as param when this is only used by inline engine?
     * Common function for primitive PutSync, puts variables in buffer
     * @param variable
     * @param values
     */
    template <class T>
    void PutSyncCommon(Variable<T> &variable,
                       const typename Variable<T>::Info &blockInfo);

    /**
     * Probably this is more useful than the version with the Info struct as param
     */
    template <class T>
    void PutSyncCommon(Variable<T> &variable, const T *values);

    template <class T>
    void PutDeferredCommon(Variable<T> &variable, const T *values);

    void DoFlush(const bool isFinal = false, const int transportIndex = -1);
    /**
     * Closes a single transport or all transports
     * @param transportIndex, if -1 (default) closes all transports,
     * otherwise it closes a transport in m_Transport[transportIndex].
     * In debug mode the latter is bounds-checked.
     */
    void DoClose(const int transportIndex = -1) final;

    /**
     * DESIGN
     * N-to-N data buffers writes, including metadata file
     * @param transportIndex
     */
    void WriteData(const bool isFinal, const int transportIndex = -1);

    /**
     * DESIGN
     * N-to-M (aggregation) data buffers writes, including metadata file
     * @param transportIndex
     */
    void AggregateWriteData(const bool isFinal, const int transportIndex = -1);

    /**
     * Put Attributes to file.
     * @param io [description]
     */
    void PutAttributes(core::IO &io);

    /**
     * Sets buffer's positions to zero and fill buffer with zero char
     * @param bufferSTL buffer to be reset
     * @param resetAbsolutePosition true: both bufferSTL.m_Position and
     * bufferSTL.m_AbsolutePosition set to 0,   false(default): only
     * bufferSTL.m_Position
     * is set to zero,
     */
    void ResetBuffer(BufferSTL &bufferSTL,
                     const bool resetAbsolutePosition = false,
                     const bool zeroInitialize = true);

    /**
     * DESIGN
     * Returns the estimated variable index size. Used by ResizeBuffer public
     * function
     * @param variableName input
     * @param count input variable local dimensions
     */
    size_t GetBPIndexSizeInData(const std::string &variableName,
                                const Dims &count) const noexcept;
    /** Return type of the CheckAllocation function. */
    enum class ResizeResult
    {
        Failure,   //!< FAILURE, caught a std::bad_alloc
        Unchanged, //!< UNCHANGED, no need to resize (sufficient capacity)
        Success,   //!< SUCCESS, resize was successful
        Flush      //!< FLUSH, need to flush to transports for current variable
    };

    /**
     * DESIGN
     * Resizes the data buffer to hold new dataIn size
     * @param dataIn input size for new data
     * @param hint for exception handling
     * @return
     * -1: allocation failed,
     *  0: no allocation needed,
     *  1: reallocation is sucessful
     *  2: need a transport flush
     */
    ResizeResult ResizeBuffer(const size_t dataIn, const std::string hint);
};

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAMPIWRITER_H_ */
