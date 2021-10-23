/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 26, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEADBWRITER_TCC_
#define ADIOS2_ENGINE_JULEADBWRITER_TCC_

// #include "adios2/helper/adiosCommMPI.h"

#include "JuleaDBInteractionWriter.h"
#include "JuleaDBWriter.h"

// #include <adios2_c.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include <mpi.h>

namespace adios2
{
namespace core
{
namespace engine
{

// void SetGlobalMinMax()
// {
//         //find out global min/max over all blocks of this step
//         std::map<int, double>::iterator it;
//         std::map<int, double>::iterator it2;

//         for(it=m_MaxMap.begin(); it!=m_MaxMap.end(); ++it)
//         {
//             // cout << it->first << " => " << it->second << '\n';
//             // if (it->second > variable.m_Max)
//             // {
//             //     variable.m_Max = it->second;
//             // }
//         }
// }

template <class T>
void JuleaDBWriter::JuleaDBSetMinMax(Variable<T> &variable, const T *data, T &blockMin,
                      T &blockMax, T &blockMean, size_t currentStep, size_t currentBlockID)
{
    T min = 0;
    T max = 0;
    T sum = 0;
    T mean = 0;

    auto number_elements = adios2::helper::GetTotalSize(variable.m_Count);
    adios2::helper::GetMinMax(data, number_elements, min, max);

    for (size_t i = 0; i < number_elements; ++i)
    {
        sum += data[i];
    }

    // TODO: cast to T ?
    mean = sum / (double)number_elements;

    blockMin = min;
    blockMax = max;
    blockMean = mean;

    //TODO: check whether this is incorrect
    // there may be some cases where this is not working
    /*  the global min and max will be set to the first min and max, so
      that they are not still initialized with something like 0*/
    if ((currentStep == 0) && (currentBlockID == 0))
    {
        std::cout << "Set Min/Max to 0 " << std::endl;
        variable.m_Min = min;
        variable.m_Max = max;
    }

    if (min < variable.m_Min)
    {
        // std::cout << "updated global min" << std::endl;
        std::cout << "updated global min from "  << variable.m_Min << " to " << min << std::endl;
        variable.m_Min = min;
    }
    if (max > variable.m_Max)
    {
        std::cout << "updated global max from "  << variable.m_Max << " to " << max << std::endl;
        variable.m_Max = max;
    }

//     template <typename T>
// void Comm::Reduce(const T *sendbuf, T *recvbuf, size_t count, Op op, int root,
//                   const std::string &hint) const
    // Comm::Reduce(sendbuf, recvbuf, count, op, root, hint);

    // m_Comm.Reduce(nReaderPerWriter.data(), nReaderPerWriter.data(),
    //                   nReaderPerWriter.size(), helper::Comm::Op::Sum,
    //                   m_ReaderRootRank);

    // sendbuf;
    // recvbuf;


    //     template <typename T>
    // void Reduce(const T *sendbuf, T *recvbuf, size_t count, Op op, int root,
    //             const std::string &hint = std::string()) const;

    // MPI_Reduce(&localMax, &globalMax, 1, MPI_DOUBLE, MPI_MAX, m_Comm);
    // MPI_Reduce(&localMax, &globalMax, 1, MPI_DOUBLE, MPI_MAX, comm);

     // Writer ID -> number of peer readers
    // std::vector<int> nReaderPerWriter(m_RankAllPeers.size());
    // m_Comm.Reduce(&globalMax, &globalMax, 1, MPI_DOUBLE, MPI_MAX, m_Comm);
    // m_Comm.Reduce(&max, &globalMax, 1,helper::CommImpl::ToMPI(variable.m_Type),helper::Comm::Op::Max, m_Comm);
    
    // m_Comm.Reduce(&max, &globalMax, 1, helper::Comm::Op::Max, 0);
    m_Comm.Reduce(&blockMin, &variable.m_Min, 1, helper::Comm::Op::Min, 0);
    m_Comm.Reduce(&blockMax, &variable.m_Max, 1, helper::Comm::Op::Max, 0);

    /* when using something else than a float in form of T everything works with normal ADIOS2*/
    // double localMin = 0;
    // double globalMin = 0;
    // double localMax = 0;
    // double globalMax = 0;
    // m_Comm.Reduce(&localMax, &globalMax, 1, helper::Comm::Op::Max, 0);
    // m_Comm.Reduce(&localMin, &globalMin, 1, helper::Comm::Op::Min, 0);

    // std::cout << "globalMax = " << globalMax << std::endl;
    std::cout << "variable.m_Max = " << variable.m_Max << std::endl;
    std::cout << "globalMin = " << globalMin << std::endl;
    // m_Comm.Reduce(&localMax, &globalMax, 1,variable.m_Type,helper::Comm::Op::Max);
    // m_Comm.Reduce(&localMax, &globalMax, 1,variable.m_Type,helper::Comm::Op::Max, m_Comm);

    // m_Comm.Reduce(&globalMax, &globalMax, 1,MPI_DOUBLE,helper::Comm::Op::Max, m_Comm);
    // m_Comm.Reduce(&globalMax, &globalMax, 1, helper::Comm::ToMPI(variable.m_Type),helper::Comm::Op::Max, m_Comm);
    // m_Comm.Reduce(&globalMax, &globalMax, 1, helper::ToMPI(variable.m_Type),helper::Comm::Op::Max, m_Comm);

    // MPI_Allreduce(&writeTime, &maxWriteTime, 1, MPI_DOUBLE, MPI_MAX, comm);

    // m_MaxMap.insert(std::make_pair(m_WriterRank,(double) variable.m_Max));
    // m_MinMap.insert(std::make_pair(m_WriterRank,(double) variable.m_Min));

    // m_Comm.Barrier();

    // if (m_WriterRank == 0)
    // {
    //      std::map<int, double>::iterator it;
    //     std::map<int, double>::iterator it2;

    //     for(it=m_MaxMap.begin(); it!=m_MaxMap.end(); ++it)
    //     {
    //         std::cout << it->first << " => " << it->second << '\n';
    //         if (it->second > variable.m_Max)
    //         {
    //             variable.m_Max = it->second;
    //         }
    //     }
    // }

    if (false)
    {
        std::cout << "min: " << min << std::endl;
        std::cout << "global min: " << variable.m_Min << std::endl;
        std::cout << "max: " << max << std::endl;
        std::cout << "global max: " << variable.m_Max << std::endl;
    }
}

template <>
void JuleaDBWriter::JuleaDBSetMinMax<std::string>(Variable<std::string> &variable,
                                   const std::string *data,
                                   std::string &blockMin, std::string &blockMax,
                                   std::string &blockMean, size_t currentStep, size_t currentBlockID)
{
    // TODO implement?
}

template <>
void JuleaDBWriter::JuleaDBSetMinMax<std::complex<float>>(
    Variable<std::complex<float>> &variable, const std::complex<float> *data,
    std::complex<float> &blockMin, std::complex<float> &blockMax,
    std::complex<float> &blockMean, size_t currentStep, size_t currentBlockID)
{
    // TODO implement?
}

template <>
void JuleaDBWriter::JuleaDBSetMinMax<std::complex<double>>(
    Variable<std::complex<double>> &variable, const std::complex<double> *data,
    std::complex<double> &blockMin, std::complex<double> &blockMax,
    std::complex<double> &blockMean, size_t currentStep, size_t currentBlockID)
{
    // TODO implement?
}

template <class T>
void JuleaDBWriter::SetBlockID(Variable<T> &variable)
{
    if (variable.m_ShapeID == ShapeID::GlobalValue ||
        variable.m_ShapeID == ShapeID::GlobalArray)
    {
        // std::cout << "GlobalValue/GlobalArray: m_CurrentBlockID =
        // m_WriterRank"
        // << std::endl;
        m_CurrentBlockID = m_WriterRank;
        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].resize(
            m_Comm.Size());
        variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].at(
            m_WriterRank) = m_CurrentBlockID;
    }
    else if (variable.m_ShapeID == ShapeID::JoinedArray)
    {
        std::cout << "JoinedArray: Currently not implemented yet." << std::endl;
    }
    else if (variable.m_ShapeID == ShapeID::LocalArray ||
             variable.m_ShapeID == ShapeID::LocalValue)
    {
        if (m_Comm.Size() == 1)
        {
            std::cout << "LocalValue/: Nothing to do?! Only increment "
                         "after put."
                      << std::endl;
            variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
                m_CurrentBlockID);
        }
        else
        {
            std::cout << "LocalArray: Have fun with synchronized counter "
                         "across processes."
                      << std::endl;
            variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
                m_CurrentBlockID);
        }
    }
    else
    {
        std::cout << "Shape Type not known." << std::endl;
    }
}

template <class T>
void JuleaDBWriter::PutSyncToJulea(Variable<T> &variable, const T *data,
                                   const typename Variable<T>::Info &blockInfo)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank
                  << "     PutSyncToJulea(" << variable.m_Name
                  << " ---- BlockID: " << m_CurrentBlockID << std::endl;
    }
    T blockMin;
    T blockMax;
    T blockMean;
    uint32_t entryID = 0;

    JuleaDBSetMinMax(variable, data, blockMin, blockMax, blockMean, m_CurrentStep, m_CurrentBlockID);
    
    auto stepBlockID =
        g_strdup_printf("%lu_%lu", m_CurrentStep, m_CurrentBlockID);
    // std::cout << "    stepBlockID: " << stepBlockID << std::endl;

    // check whether variable name is already in variable_names DB
    auto itVariableWritten = m_WrittenVariableNames.find(variable.m_Name);
    if (itVariableWritten == m_WrittenVariableNames.end())
    {
        if (m_Verbosity == 5)
        {
            std::cout << "--- Variable name not yet written with this writer "
                      << std::endl;
        }

        m_WrittenVariableNames.insert(variable.m_Name);
    }

    // std::cout << "Variable names written to the names DB: " << std::endl;
    if (m_Verbosity == 5)
    {
        for (auto it = m_WrittenVariableNames.begin();
             it != m_WrittenVariableNames.end(); ++it)
        {
            // std::cout << "___ Written variables:" << ' ' << *it << std::endl;
        }
    }


//     // const DataType type = T;
//     const DataType type = variable.m_Type;
//      if (type == DataType::Compound || type == DataType::None)
//     {
//     }
//     else if(type == DataType::FloatComplex)
//     {

//     } 
//     else if (type == DataType::DoubleComplex)
//     {

//     }
//     else if (type == DataType::String)
//     {
//         std::cout << "DataType not supported" << std::endl;
//     }
// #define declare_type(T)                                                        \
//     else if (type == helper::GetDataType<T>())                                 \
//     {                                 \
//         m_MinMap.insert(std::make_pair(m_WriterRank,(double) variable.m_Min));          \
//     }
//     ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
// #undef declare_type


        // m_MaxMap.insert(std::make_pair(m_WriterRank,(double) variable.m_Max));\
    // m_MinMap.insert(std::make_pair(m_WriterRank,variable.m_Min));
    // m_MaxMap.insert(std::make_pair(m_WriterRank,variable.m_Max));
    // m_minMap.append(m_WriterRank, (double) variable.m_Min);
    // m_maxMap.append(m_WriterRank, (double) variable.m_Max);

    // m_Comm.Barrier();

    // TODO: check if there really is no case for global variables to have
    // different features across different blocks
    if (m_WriterRank == 0)
    {
        // //find out global min/max over all blocks of this step
        // std::map<int, double>::iterator it;
        // std::map<int, double>::iterator it2;

        // for(it=m_MaxMap.begin(); it!=m_MaxMap.end(); ++it)
        // {
        //     // cout << it->first << " => " << it->second << '\n';
        //     // if (it->second > variable.m_Max)
        //     // {
        //     //     variable.m_Max = it->second;
        //     // }
        // }

        // TODO: add mean value to DB
        /** updates the variable metadata as there is a new block now */
        DBPutVariableMetadataToJulea(variable, m_Name, variable.m_Name,
                                     m_CurrentStep, m_CurrentBlockID);
    }

    /** put block metadata to DB */
    DBPutBlockMetadataToJulea(variable, m_Name, variable.m_Name, m_CurrentStep,
                              m_CurrentBlockID, blockInfo, blockMin, blockMax,
                              blockMean, entryID);

    // std::cout << "entryID: " << entryID << std::endl;
    /** put data to object store */
    // DBPutVariableDataToJulea(variable, data, m_Name, m_CurrentStep,
    // m_CurrentBlockID);
    DBPutVariableDataToJulea(variable, data, m_Name, entryID);
}

template <class T>
void JuleaDBWriter::PutSyncCommon(Variable<T> &variable,
                                  const typename Variable<T>::Info &blockInfo)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
        std::cout << "\n_________________________PutSyncCommon "
                     "BlockInfo_____________________________"
                  << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Namespace: " << m_Name << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Variable name: " << variable.m_Name << std::endl;

        std::cout << "    CurrentStep: " << m_CurrentStep << std::endl;
    }

    PutSyncToJulea(variable, blockInfo.Data, blockInfo);
}

template <class T>
void JuleaDBWriter::PutSyncCommon(Variable<T> &variable, const T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank << "     PutSync("
                  << variable.m_Name << ")\n";
        std::cout << "\n___________________________PutSyncCommon "
                     "T__________________________"
                  << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Namespace: " << m_Name << std::endl;
        std::cout << "Julea DB Writer " << m_WriterRank
                  << " Variable name: " << variable.m_Name << std::endl;

        std::cout << "    CurrentStep: " << m_CurrentStep << std::endl;
    }

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());
    PutSyncToJulea(variable, data, blockInfo);
}

template <class T>
void JuleaDBWriter::PutDeferredCommon(Variable<T> &variable, const T *data)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea DB Writer " << m_WriterRank << "     PutDeferred("
                  << variable.m_Name << ")\n";
        std::cout << "\n___________________________PutDeferred "
                     "T____________________________"
                  << std::endl;
        std::cout << "data[0]: " << data[0] << std::endl;
        // std::cout << "data[1]: " << data[1] << std::endl;
    }

    const typename Variable<T>::Info blockInfo =
        variable.SetBlockInfo(data, CurrentStep());

    // if (variable.m_SingleValue)
    // {
    // std::cout << "variable.m_SingleValue: " << variable.m_SingleValue
    // << std::endl;
    // DoPutSync(variable, data); // causes issues with blockID when no
    // steps and no bpls return;
    // }

    m_DeferredVariables.insert(variable.m_Name);
}

template <class T>
void JuleaDBWriter::PerformPutCommon(Variable<T> &variable)
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________PerformPutCommon T_____________________"
                  << std::endl;
        std::cout << "BlockInfo.size = " << variable.m_BlocksInfo.size()
                  << std::endl;
    }
    for (size_t i = 0; i < variable.m_BlocksInfo.size(); ++i)
    {
        // std::cout << "variable: " << variable.m_Name << "--- i: " << i
        // << std::endl;
        // variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].push_back(
        // m_CurrentBlockID);

        SetBlockID(variable);

        //  if (variable.m_ShapeID == ShapeID::GlobalValue ||
        //     variable.m_ShapeID == ShapeID::GlobalArray)
        // {
        //     std::cout
        //         << "GlobalValue/GlobalArray: m_CurrentBlockID = m_WriterRank"
        //         << std::endl;
        //     m_CurrentBlockID = m_WriterRank;
        //     variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].resize(
        //         m_Comm.Size());
        //     variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep].at(
        //         m_WriterRank) = m_CurrentBlockID;
        // }
        // else if (variable.m_ShapeID == ShapeID::JoinedArray)
        // {
        //     std::cout << "JoinedArray: Currently not implemented yet."
        //               << std::endl;
        // }
        // else if (variable.m_ShapeID == ShapeID::LocalArray ||
        //          variable.m_ShapeID == ShapeID::LocalValue)
        // {
        //     if (m_Comm.Size() == 1)
        //     {
        //         std::cout << "LocalValue/: Nothing to do?! Only increment "
        //                      "after put."
        //                   << std::endl;
        //         variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep]
        //             .push_back(m_CurrentBlockID);
        //     }
        //     else
        //     {
        //         std::cout << "LocalArray: Have fun with synchronized counter
        //         "
        //                      "across processes."
        //                   << std::endl;
        //         variable.m_AvailableStepBlockIndexOffsets[m_CurrentStep]
        //             .push_back(m_CurrentBlockID);
        //     }
        // }
        // else
        // {
        //     std::cout << "Shape Type not known." << std::endl;
        // }

        /** if there are no SpanBlocks simply put every variable */
        auto itSpanBlock = variable.m_BlocksSpan.find(i);
        if (itSpanBlock == variable.m_BlocksSpan.end())
        {
            PutSyncCommon(variable, variable.m_BlocksInfo[i]);
            // m_CurrentBlockID = m_CurrentBlockID + i + 1;
            m_CurrentBlockID = m_CurrentBlockID + 1;
        }
        // else
        // {
        //     m_BP3Serializer.PutSpanMetadata(variable, itSpanBlock->second);
        // }
    }

    variable.m_BlocksInfo.clear();
    variable.m_BlocksSpan.clear();
}

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEADBWRITER_TCC_ */
