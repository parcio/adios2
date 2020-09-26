/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEADB_DO_READER_H_
#define ADIOS2_ENGINE_JULEADB_DO_READER_H_

// #include "adios2/ADIOSConfig.h"  //FIXME: missing
#include "adios2/core/ADIOS.h"
#include "adios2/core/Engine.h"
#include "adios2/helper/adiosFunctions.h"
// #include "adios2/toolkit/format/bp3/BP3.h" //BP3Deserializer
#include "adios2/toolkit/format/bp/bp3/BP3Serializer.h"
#include "adios2/toolkit/transportman/TransportMan.h" //transport::TransportsMan

#include <complex.h>
#include <glib.h>
#include <julea.h>
#include <mutex>

namespace adios2
{
namespace core
{
namespace engine
{

class JuleaDB_DO_Reader : public Engine
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
    JuleaDB_DO_Reader(IO &adios, const std::string &name, const Mode mode,
                      helper::Comm comm);

    ~JuleaDB_DO_Reader();
    // StepStatus BeginStep(StepMode mode = StepMode::NextAvailable,
    //                      const float timeoutSeconds = -1.0) final;
    StepStatus BeginStep(StepMode mode = StepMode::Read,
                         const float timeoutSeconds = -1.0) final;
    size_t CurrentStep() const final;
    void EndStep() final;
    void PerformGets() final;

private:
    JSemantics *m_JuleaSemantics;
    StepMode m_StepMode = StepMode::Append;

    /**
     * This is not at all beautiful!
     * Caution! This assumes that only bpls calls AllStepsBlocksInfo!
     *
     * However, I found no other way to ensure that the keys for the key-value
     * store and the object store are correct for all of the following
     * scenarios:
     * - begin step, setblockselection, get, endstep
     * - setstepselection, get
     * - begin, multiple get, endstep (this should always return the same
     * block!)
     * - bpls: allstepsblocksinfo = return all blocks for all steps without
     * actually increasing the step (no endStep called)
     *
     * Either stepsstart and co are working fine or m_CurrentBlockID and
     * m_CurrentStep. Trying to implement a macro/function/... in bpls when
     * adding Julea to the engine list was not working.
     *
     */
    mutable bool m_UseKeysForBPLS = false;

    static std::mutex m_Mutex;

    int m_Verbosity = 0; // change to 5 for debugging
    int m_Penguin = 0;   // change for debugging info from 0 to 42
    int m_ReaderRank;    // my rank in the readers' comm

    bool m_CollectiveMetadata = true;

    // step info should be received from the writer side in BeginStep()
    size_t m_CurrentStep = 0; // starts at 0

    size_t m_CurrentBlockID = 0; // starts at 0

    bool m_FirstStep = true;

    /** Parameter to flush transports at every number of steps, to be used at
     * EndStep */
    size_t m_FlushStepsCount = 1;

    /** manages all communication tasks in aggregation */
    // aggregator::MPIChain m_Aggregator;

    /** tracks Put and Get variables in deferred mode */
    std::set<std::string> m_DeferredVariables;

    /** tracks all variables written (not BP, new for JULEA) */
    std::set<std::string> m_WrittenVariableNames;

    /** tracks the overall size of deferred variables */
    size_t m_DeferredVariablesDataSize = 0;

    /** statistics verbosity, only 0 is supported */
    unsigned int m_StatsLevel = 0;

    void Init() final; ///< called from constructor, gets the selected Skeleton
                       /// transport method from settings

    /** Parses parameters from IO SetParameters */
    void InitParameters() final;
    /** Parses transports and parameters from IO AddTransport */
    void InitTransports() final;

    // template <class T>
    void InitVariables();

#define declare_type(T)                                                        \
    void DoGetSync(Variable<T> &, T *) final;                                  \
    void DoGetDeferred(Variable<T> &, T *) final;
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

    template <class T>
    void GetSyncCommon(Variable<T> &variable, T *data);

    template <class T>
    void GetDeferredCommon(Variable<T> &variable, T *data);

    void DoClose(const int transportIndex = -1);

    // void ReadData();

    // void AggregateReadData();

    /**
     * Initializes a block inside variable.m_BlocksInfo
     * @param variable input
     * @param data user data pointer
     * @return a reference inside variable.m_BlocksInfo (invalidated if called
     * twice)
     */
    template <class T>
    typename core::Variable<T>::Info &
    InitVariableBlockInfo(core::Variable<T> &variable, T *data);

    /**
     * Sets read block information from the available metadata information
     * @param variable
     * @param blockInfo
     */
    template <class T>
    void
    SetVariableBlockInfo(core::Variable<T> &variable,
                         typename core::Variable<T>::Info &blockInfo) const;

    template <class T>
    void ReadBlockMD(typename core::Variable<T>::Info &blockInfo,
                     size_t blockID);

    template <class T>
    void ReadVariableBlocks(Variable<T> &variable);

    template <class T>
    void ReadBlock(Variable<T> &variable, T *data, size_t blockID);

    template <class T>
    std::vector<typename core::Variable<T>::Info>
    BlocksInfoCommon(const core::Variable<T> &variable,
                     const std::vector<size_t> &blocksIndexOffsets,
                     size_t step) const;

    /** ---------- the following functions are mainly used for bpls */
    template <class T>
    std::map<size_t, std::vector<typename core::Variable<T>::Info>>
    AllStepsBlocksInfo(const core::Variable<T> &variable) const;

    template <class T>
    std::map<size_t, std::vector<typename core::Variable<T>::Info>>
    DoAllStepsBlocksInfo(const core::Variable<T> &variable) const;

    template <class T>
    std::vector<std::vector<typename core::Variable<T>::Info>>
    AllRelativeStepsBlocksInfo(const core::Variable<T> &variable) const;

    template <class T>
    std::vector<std::vector<typename core::Variable<T>::Info>>
    DoAllRelativeStepsBlocksInfo(const core::Variable<T> &variable) const;

    template <class T>
    std::vector<typename core::Variable<T>::Info>
    BlocksInfo(const core::Variable<T> &variable, const size_t step) const;

    template <class T>
    std::vector<typename core::Variable<T>::Info>
    DoBlocksInfo(const core::Variable<T> &variable, const size_t step) const;

#define declare_type(T)                                                        \
    std::map<size_t, std::vector<typename Variable<T>::Info>>                  \
    DoAllStepsBlocksInfo(const Variable<T> &variable) const final;             \
                                                                               \
    std::vector<std::vector<typename Variable<T>::Info>>                       \
    DoAllRelativeStepsBlocksInfo(const Variable<T> &) const final;             \
                                                                               \
    std::vector<typename Variable<T>::Info> DoBlocksInfo(                      \
        const Variable<T> &variable, const size_t step) const final;

    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
};

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEADBREADER_H_ */
