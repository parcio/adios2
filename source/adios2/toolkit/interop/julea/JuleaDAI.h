/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JuleaDAI.cpp
 *
 *  Created on: May 13, 2022
 *      Author: Kira Duwe
 */

#ifndef ADIOS2_TOOLKIT_INTEROP_JULEA_DAI_H_
#define ADIOS2_TOOLKIT_INTEROP_JULEA_DAI_H_

// #include "adios2/toolkit/interop/julea/JuleaInteraction.h"
// #include "JuleaMetadata.h"
// #include "adios2/common/ADIOSMacros.h"
// #include "adios2/common/ADIOSTypes.h"

#include "adios2/core/IO.h" // for CreateVar
// #include "adios2/core/Variable.h"

#include <julea-dai.h>      // for enums
#include <julea.h>

#include <string>

#include <stdexcept> // for Intel Compiler

namespace adios2
{
namespace interop
{

class JuleaDAI
{
public:
    JuleaDAI(helper::Comm const &comm);
    //JuleaDAI();
    ~JuleaDAI() = default;

    helper::Comm const &m_Comm; ///< multi-process communicator from Engine
    // helper::Comm m_Comm; ///< multi-process communicator from Engine
    int m_Verbosity = 5;
    int m_WriterRank;

    struct Tag
    {
        // std::string m_projectNamespace; //TODO: check whether sensible to store here...
        std::string m_TagName;          //determines the table name
        std::string m_FileName;
        std::string m_VariableName;
        // feature that should be tagged -> could be a stat, could be new feature -> more flexible
        std::string m_FeatureName;      // default "mean"

        // only one of them is used but since JULEA has not many types these two
        // are sufficient for now
        size_t m_Threshold_i;
        float m_Threshold_f;

        // JDAIStatistic m_Statistic;
        JDAIOperator m_Operator;            //default ">"
        JDAIGranularity m_Granularity;      // default "block level"
    };

    // this is the struct holding the information which functions should be
    // precomputed
    struct Precompute
    {
        // std::string m_projectNamespace; //TODO: check whether sensible to store here... probably better in engine itself
        std::string m_FileName;
        std::string m_VariableName;

        JDAIStatistic m_Statistic;
        JDAIGranularity m_Granularity; //default block level
    };

    std::vector<Tag> m_Tags;
    std::vector<Precompute> m_Precomputes;


    // 1) read all those new tables that the DAI component created and create
    // the tables accordingly 2) add compute functions to list that needs to be
    // computed
    void
    trackFeature();
    void customTag();
    void createTag();

    void Init(helper::Comm const &comm);

    bool IsCriteriumMet_i(JDAIOperator op, size_t threshold, size_t data);
    bool IsCriteriumMet_d(JDAIOperator op, double threshold, double data);
private:


};

} // end namespace interop
} // end namespace adios

#endif /* ADIOS2_TOOLKIT_INTEROP_JULEA_DAI_H_ */
