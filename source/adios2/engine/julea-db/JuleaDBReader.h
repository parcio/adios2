/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAREADER_H_
#define ADIOS2_ENGINE_JULEAREADER_H_

#include "JuleaDBMetadata.h"

// #include "adios2/ADIOSConfig.h"  //FIXME: missing
#include "adios2/core/ADIOS.h"
#include "adios2/core/Engine.h"
#include "adios2/helper/adiosFunctions.h"
#include "adios2/toolkit/format/bp3/BP3.h" //BP3Deserializer
#include "adios2/toolkit/format/bp3/BP3Serializer.h"
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

/** used for Variables and Attributes, name, type, type-index */
using DataMap =
    std::unordered_map<std::string, std::pair<std::string, unsigned int>>;

class JuleaDBReader : public Engine
{
public:
    /**
     * Constructor for single BP capsule engine, writes in BP format into a
     * single
     * heap capsule
     * @param name unique name given to the engine
     * @param accessMode
     * @param mpiComm
     * @param method
     * @param debugMode
     * @param hostLanguage
     */
    JuleaDBReader(IO &adios, const std::string &name, const Mode mode,
                MPI_Comm mpiComm);

    ~JuleaDBReader();
    // StepStatus BeginStep(StepMode mode = StepMode::NextAvailable,
    //                      const float timeoutSeconds = -1.0) final;
    StepStatus BeginStep(StepMode mode = StepMode::Read,
                         const float timeoutSeconds = -1.0) final;
    size_t CurrentStep() const final;
    void EndStep() final;
    void PerformGets() final;

private:
    JuleaDBInfo *m_JuleaDBInfo;
    int m_Verbosity = 5; // TODO: changed to 5 for debugging
    int m_ReaderRank;    // my rank in the readers' comm

    // step info should be received from the writer side in BeginStep()
    size_t m_CurrentStep = -1;
    bool m_FirstStep = true;

    // EndStep must call PerformGets if necessary
    bool m_NeedPerformGets = false;

    bool m_CollectiveMetadata = true;

    /** Parameter to flush transports at every number of steps, to be used at
     * EndStep */
    size_t m_FlushStepsCount = 1;

    /** manages all communication tasks in aggregation */
    aggregator::MPIChain m_Aggregator;

    /** tracks Put and Get variables in deferred mode */
    std::set<std::string> m_DeferredVariables;

    /** tracks the overall size of deferred variables */
    size_t m_DeferredVariablesDataSize = 0;

    /** statistics verbosity, only 0 is supported */
    unsigned int m_StatsLevel = 0;

    /** contains data buffer for this rank */
    BufferSTL m_Data;

    /** contains collective metadata buffer, only used by rank 0 */
    BufferSTL m_Metadata;

    // HELP! how do I actually get the compiler to accept MetadataSet as a type?
    /** contains bp1 format metadata indices*/ // DESIGN: needed?
    format::BP3Base::MetadataSet m_MetadataSet;

    // format::BP3Deserializer m_BP3Deserializer;  //HELP! is this really a good
    // idea?

    // DESIGN
    /** Manage BP data files Transports from IO AddTransport */
    // transportman::TransportMan m_FileDataManager; //FIXME: compiler?!

    /** Manages the optional collective metadata files */
    // transportman::TransportMan m_FileMetadataManager; FIXME: compiler?!

    void Init() final; ///< called from constructor, gets the selected Skeleton
                       /// transport method from settings

    /** Parses parameters from IO SetParameters */
    void InitParameters() final;
    /** Parses transports and parameters from IO AddTransport */
    void InitTransports() final;

    // template <class T>
    void InitVariables(); // needs to be final? HELP

// #define declare_type(T)                                                        \
//     void DoGetSync(Variable<T> &, T *) final;                                  \
//     void DoGetDeferred(Variable<T> &, T *) final;
//     ADIOS2_FOREACH_TYPE_1ARG(declare_type)
// #undef declare_type
#define declare_type(T)                                                        \
    void DoGetSync(Variable<T> &, T *) final;                                  \
    void DoGetDeferred(Variable<T> &, T *) final;
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    template <class T>
    void GetSyncCommon(Variable<T> &variable, T *data);

    template <class T>
    void GetDeferredCommon(Variable<T> &variable, T *data);

    // template <class T>
    // void ReadVariableBlocks(Variable<T> &variable); //TODO:needed?

    void DoClose(const int transportIndex = -1);

    // void ReadData();

    // void AggregateReadData();

    /**
     * DESIGN: is this function needed here? is there something one would want
     * to do different with a variable coming from JULEA?
     *
     * Sets read block information from the available metadata information
     * @param variable
     * @param blockInfo
     */
    template <class T>
    void SetVariableBlockInfo(core::Variable<T> &variable,
                              typename core::Variable<T>::Info &blockInfo);

    // #define declare_type(T)                                                        \
//     std::map<size_t, std::vector<typename Variable<T>::Info>>                  \
//     DoAllStepsBlocksInfo(const Variable<T> &variable) const final;             \
//                                                                                \
//     std::vector<typename Variable<T>::Info> DoBlocksInfo(                      \
//         const Variable<T> &variable, const size_t step) const final;

    //     ADIOS2_FOREACH_TYPE_1ARG(declare_type)
    // #undef declare_type
};

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAREADER_H_ */
