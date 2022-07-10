/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Aug 01, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONWRITER_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONWRITER_H_

#include "adios2/toolkit/interop/julea/JuleaCDO.h"
#include "adios2/toolkit/interop/julea/JuleaInteraction.h"

// #include "JuleaDBDAIWriter.h"
// #include "JuleaMetadata.h"

namespace adios2
{
namespace interop
{

class JuleaDBInteractionWriter : public JuleaInteraction
{

public:
    JuleaDBInteractionWriter(helper::Comm const &comm);
    ~JuleaDBInteractionWriter() = default;

    /** --- Variables --- */
    void InitDBSchemas(std::string projectNamespace, bool isOriginalFormat);
    void InitTagTables(std::string projectNamespace);

    /**
     * Put the metadata for a specific block in a specific step to JULEA
     * database.
     */
    template <class T>
    void PutVariableMetadataToJulea(core::Variable<T> &variable,
                                    const std::string projectNamespace,
                                    const std::string fileName,
                                    const std::string varName, size_t currStep,
                                    size_t block, bool original);
    /**
     * Put the metadata for a specific block in a specific step to JULEA
     * database.
     */
    template <class T>
    void PutBlockMetadataToJulea(
        core::Variable<T> &variable, const std::string projectNamespace,
        const std::string fileName, const std::string varName, size_t step,
        size_t block, const typename core::Variable<T>::Info &blockInfo,
        T &blockMin, T &blockMax, T &blockMean, T &blockSum, T &blockVar,
        uint32_t &entryID, bool original);

    template <class T>
    void AddEntriesForTagTable(const std::string projectNamespace,
                               const std::string tagName,
                               const std::string fileName,
                               const std::string varName, size_t currentStep,
                               size_t block, const T data);
    /** --- Attributes --- */
    // TODO: support attributes again

    // stores all tags that should be marked.
    // map
    // key: pair of filename and variable name
    // value: list of tags (struct)
    std::map<std::pair<std::string, std::string>,
             std::list<adios2::interop::JuleaCDO::Tag>>
        m_Tags;

private:
    /**
     *   ------------- Create table schemas ----------------
     */
    // schemas for CDO related statistics
    void AddFieldsForClimateIndexTable(JDBSchema *schema);
    // void AddFieldsForYearlyLocalStatsTable(JDBSchema *schema);
    void AddFieldsForDailyGlobalStatsTable(JDBSchema *schema);
    void AddFieldsForDailyLocalStatsTable(JDBSchema *schema);

    void AddFieldsForTagTable(JDBSchema *schema);
    void AddFieldsForPrecomputationTable(JDBSchema *schema);

    /**  Tables contain only original information (= BP formats) */
    void AddFieldsForVariableMD_Original(JDBSchema *schema);
    void AddFieldsForBlockMD_Original(JDBSchema *schema);

    /**  Tables contain only doubles and original information (= BP formats) */
    void AddFieldsForVariableMD_OriginalDouble(JDBSchema *schema);
    void AddFieldsForBlockMD_OriginalDouble(JDBSchema *schema);

    /** Tables contain additional statistics (mean, sum, var) */
    void AddFieldsForBlockMD_AllTypes_AdditionalStats(JDBSchema *schema);

    /** Tables contain only doubles additional statistics (mean, sum, var) */
    void AddFieldsForVariableMD_Eval(JDBSchema *schema);
    void AddFieldsForBlockMD_Eval(JDBSchema *schema);

    /**
     *   ------------- Add metadata to tables ----------------
     */
    // should only be called from master
    void AddEntriesForClimateIndexTable(const std::string projectNamespace,
                                        const std::string fileName,
                                        const std::string varName,
                                        size_t currentStep,
                                        interop::JuleaCDO &JuleaCDO);
    // void AddEntriesForYearlyLocalStatsTable(const std::string nameSpace,
    //                                         const std::string varName,
    //                                         size_t currentStep,
    //                                         interop::JuleaCDO &JuleaCDO,
    //                                         int writerRank);
    void AddEntriesForDailyGlobalStatsTable(const std::string projectNamespace,
                                            const std::string fileName,
                                            const std::string varName,
                                            size_t currentStep,
                                            interop::JuleaCDO &JuleaCDO,
                                            int writerRank, int year, int month,
                                            int day);
    // void AddEntriesForDailyLocalStatsTable(JDBSchema *schema);
    void AddEntriesForCDOStatistics(const std::string nameSpace,
                                    const std::string varName,
                                    size_t currentStep,
                                    interop::JuleaCDO &m_JuleaCDO);
    // void AddEntriesForPrecomputation(const std::string nameSpace,
    //                                 const std::string varName,
    //                                 size_t currentStep,
    //                                 interop::JuleaCDO &m_JuleaCDO);

}; // end namespace JuleaDBInteractionWriter

#define declare_template_instantiation(T)                                      \
    extern template void JuleaDBInteractionWriter::AddEntriesForTagTable(      \
        const std::string projectNamespace, const std::string tagName,         \
        const std::string fileName, const std::string varName,                 \
        size_t currentStep, size_t block, const T data);                       \
    extern template void JuleaDBInteractionWriter::PutVariableMetadataToJulea( \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName,                 \
        size_t currStep, size_t block, bool original);                         \
    extern template void JuleaDBInteractionWriter::PutBlockMetadataToJulea(    \
        core::Variable<T> &variable, const std::string projectNamespace,       \
        const std::string fileName, const std::string varName, size_t step,    \
        size_t block, const typename core::Variable<T>::Info &blockInfo,       \
        T &blockMin, T &blockMax, T &blockMean, T &blockSum, T &blockVar,      \
        uint32_t &entryID, bool original);
ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation

} // end namespace interop
} // end namespace adios2

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_JULEADBINTERACTIONWRITER_H_ */
