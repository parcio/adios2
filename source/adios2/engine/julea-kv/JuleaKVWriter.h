/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAKVWRITER_H_
#define ADIOS2_ENGINE_JULEAKVWRITER_H_

#include "JuleaMetadata.h"

#include "adios2/core/Engine.h"
#include "adios2/toolkit/format/bp/bp3/BP3Serializer.h"
#include "adios2/toolkit/format/buffer/heap/BufferSTL.h"
#include "adios2/toolkit/interop/julea/JuleaSerializer.h"
#include "adios2/toolkit/transportman/TransportMan.h" //transport::TransportsMan

#include <complex.h>
#include <glib.h>
#include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

class JuleaKVWriter : public Engine
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
    JuleaKVWriter(IO &adios, const std::string &name, const Mode mode,
                  helper::Comm comm);

    ~JuleaKVWriter(); // was =default -> meaning?

    // TODO: why is there no set StepMode in the Write engine?
    StepStatus BeginStep(StepMode mode,
                         const float timeoutSeconds = -1.0) final;
    size_t CurrentStep() const final;
    void PerformPuts() final;
    void EndStep() final;
    void
    Flush(const int transportIndex = -1) final; // TODO: transportindex needed?
    // void Flush();

    /** Step related metadata for a variable broken down to put into JULEA  */
    struct CStepMetadata
    {
        bool isConstantDims;
        size_t typeSize = 0;
        char *type = nullptr;
        size_t shapeSize = 0;
        size_t *shape = nullptr;
        size_t startSize = 0;
        size_t *start = nullptr;
        size_t countSize = 0;
        size_t *count = nullptr;

        size_t numberSteps = 0;
        size_t *blocks = nullptr;
        // int type;
    };

    /** Step related metadata for a variable:  */
    struct StepMetadata
    {
        Dims shape;
        Dims start;
        Dims count;
        std::string type;
        bool isConstantDims;
        size_t numberSteps = 0;
        size_t *blocks = nullptr;
        // int type;
    };

    /** Operators metadata info */
    struct Operation
    {
        /** reference to object derived from Operator class,
         *  needs a pointer to enable assignment operator (C++ class) */
        core::Operator *Op;
        /** Variable specific parameters */
        Params Parameters;
        /** resulting information from executing Operation (e.g. buffer size) */
        Params Info;
    };

    template <class T>
    struct Metadata
    {
        // TODO: needed?
        // std::map<size_t, std::vector<helper::SubStreamBoxInfo>>
        // StepBlockSubStreamsInfo;
        // struct helper::BlockDivisionInfo SubBlockInfo;
        // SelectionType Selection = SelectionType::BoundingBox;

        Dims Shape;
        Dims Start;
        Dims Count;
        Dims MemoryStart;
        Dims MemoryCount;

        // std::vector<core::IO::Operation> Operations;
        std::vector<core::VariableBase::Operation> Operations;
        // std::vector<T> Values;
        // std::vector<T> MinMaxs; // sub-block level min-max

        // T *Data = nullptr;
        T Min = T();
        T Max = T();
        // T Value = T();   //TODO: not set in variable?!

        // size_t Step = 0; // TODO: currentStep? variable has no Step itself
        size_t StepsStart = 0;
        size_t StepsCount = 0;
        size_t BlockID = 0;

        size_t CurrentStep = 0; // Julea Engine
        size_t BlockNumber = 0; // Julea Engine

        // int WriterID = 0; //TODO: what exactly is this and when used?

        /** Global array was written as Joined array, so read accordingly */
        bool IsReadAsJoined = false;

        /** Global array was written as Local value, so read accordingly */
        bool IsReadAsLocalValue = false;

        /** For read mode, false: streaming */
        bool IsRandomAccess = true;

        /** is single value or array */
        bool IsValue = false;
        /** if reader and writer have different ordering (column vs row major)
         */
        bool IsReverseDimensions = false;
    };

private:
    // interop::JuleaSerializer m_Julea;
    JSemantics *m_JuleaSemantics;
    StepMode m_StepMode = StepMode::Append;

    int m_Verbosity = 5; // changed for debugging info from 0 to 5
    int m_WriterRank;    // my rank in the writers' comm

    /** EndStep must call PerformPuts if necessary */
    // bool m_NeedPerformPuts = false; // DESIGN: suggested in SkeletonWriter,
    // probably as a shortcut to avoid m_DeferredVariables

    /** TODO: needed? */
    // bool m_Flushed = false; // DESIGN: used in HDF5Writer

    /**  --- DESIGN: the following is similar to BP3Writer and BP3Base --- */

    /** true: Close was called, Engine will call this many times for different
     * transports */
    bool m_IsClosed = false;

    /** Default: write collective metadata in Capsule metadata. */
    bool m_CollectiveMetadata = true;

    /* -------------- see BP3Base MetadataSet struct ------------------------*/

    /**
     * updated with EndStep, if append it will be updated to last,
     * starts with one in BPBase! legacy of Adios 1?!
     */
    uint32_t m_TimeStep = 1; // starts at 1

    /** Similar to TimeStep, but uses uint64_t and start from zero. Used for
     * streaming a large number of steps */
    size_t m_CurrentStep = 0; // starts at 0

    size_t m_CurrentBlockID = 0; // starts at 0

    /** Parameter to flush transports at every number of steps, to be used at
     * EndStep */
    size_t m_FlushStepsCount = 1;

    /** manages all communication tasks in aggregation */
    aggregator::MPIChain m_Aggregator;

    /** tracks Put and Get variables in deferred mode */
    std::set<std::string> m_DeferredVariables;

    /** tracks all variables written (not BP, new for JULEA) */
    std::set<std::string> m_WrittenVariableNames;

    /** tracks the overall size of deferred variables */
    size_t m_DeferredVariablesDataSize = 0; // TODO: needed?

    /** attributes are serialized only once, this set contains the names of ones
     * already serialized.
     */
    std::unordered_set<std::string> m_SerializedAttributes; // TODO: needed?

    /** statistics verbosity, only 0 is supported */
    unsigned int m_StatsLevel = 0;

    /** contains data buffer for this rank */
    format::BufferSTL m_Data;

    /** contains collective metadata buffer, only used by rank 0 */
    format::BufferSTL m_Metadata; // FIXME: needed? useful for julea?

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

// FIXME: const T* BlockInfo oder const T * values?
#define declare_type(T)                                                        \
    void DoPutSync(Variable<T> &variable, const T *) final;                    \
    void DoPutDeferred(Variable<T> &variable, const T *) final;
    // ADIOS2_FOREACH_TYPE_1ARG(declare_type)
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    template <class T>
    void PutSyncCommon(Variable<T> &variable,
                       const typename Variable<T>::Info &blockInfo);

    template <class T>
    void PutSyncCommon(Variable<T> &variable, const T *values);

    template <class T>
    void PutSyncToJulea(Variable<T> &variable, const T *values);

    template <class T>
    void PutDeferredCommon(Variable<T> &variable, const T *values);

    void DoFlush(const bool isFinal = false, const int transportIndex = -1);
    // void DoFlush(const bool isFinal = false);
    /**
     * Closes a single transport or all transports
     * @param transportIndex, if -1 (default) closes all transports,
     * otherwise it closes a transport in m_Transport[transportIndex].
     * In debug mode the latter is bounds-checked.
     */
    void DoClose(const int transportIndex = -1) final;

    // void DoClose();

    /**
     * DESIGN
     * N-to-N data buffers writes, including metadata file
     * @param transportIndex
     */
    void WriteData(const bool isFinal, const int transportIndex = -1);
    // void WriteData(const bool isFinal);

    /**
     * DESIGN
     * N-to-M (aggregation) data buffers writes, including metadata file
     * @param transportIndex
     */
    void AggregateWriteData(const bool isFinal, const int transportIndex = -1);
    // void AggregateWriteData(const bool isFinal);

    /**
     * Put Attributes to file.
     * @param io [description]
     */
    void PutAttributes(core::IO &io);

    template <class T>
    void PerformPutCommon(Variable<T> &variable);

    // void InitParameters(const Params &parameters);

    void InitParameterFlushStepsCount(const std::string value);

    // /**
    //  * Sets buffer's positions to zero and fill buffer with zero char
    //  * @param bufferSTL buffer to be reset
    //  * @param resetAbsolutePosition true: both bufferSTL.m_Position and
    //  * bufferSTL.m_AbsolutePosition set to 0,   false(default): only
    //  * bufferSTL.m_Position
    //  * is set to zero,
    //  */
    // void ResetBuffer(BufferSTL &bufferSTL,
    //                  const bool resetAbsolutePosition = false,
    //                  const bool zeroInitialize = true);

    // /**
    //  * DESIGN
    //  * Returns the estimated variable index size. Used by ResizeBuffer public
    //  * function
    //  * @param variableName input
    //  * @param count input variable local dimensions
    //  */
    // size_t GetBPIndexSizeInData(const std::string &variableName,
    //                             const Dims &count) const noexcept;
    // /** Return type of the CheckAllocation function. */
    // enum class ResizeResult
    // {
    //     Failure,   //!< FAILURE, caught a std::bad_alloc
    //     Unchanged, //!< UNCHANGED, no need to resize (sufficient capacity)
    //     Success,   //!< SUCCESS, resize was successful
    //     Flush      //!< FLUSH, need to flush to transports for current
    //     variable
    // };

    // /**
    //  * DESIGN
    //  * Resizes the data buffer to hold new dataIn size
    //  * @param dataIn input size for new data
    //  * @param hint for exception handling
    //  * @return
    //  * -1: allocation failed,
    //  *  0: no allocation needed,
    //  *  1: reallocation is sucessful
    //  *  2: need a transport flush
    //  */
    // ResizeResult ResizeBuffer(const size_t dataIn, const std::string hint);
};

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAKVWRITER_H_ */
