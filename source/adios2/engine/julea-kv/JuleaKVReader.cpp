/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#include "JuleaKVReader.h"
#include "JuleaKVReader.tcc"

#include "JuleaFormatReader.h"
#include "JuleaInteractionReader.h"

#include "adios2/helper/adiosFunctions.h" // CSVToVector

#include <iostream>

namespace adios2
{
namespace core
{
namespace engine
{

// JuleaReader::JuleaReader(IO &io, const std::string &name, const Mode mode,
//                                MPI_Comm mpiComm)
// : Engine("JuleaReader", io, name, mode, mpiComm),
//   m_BP3Deserializer(mpiComm, m_DebugMode)

JuleaKVReader::JuleaKVReader(IO &io, const std::string &name, const Mode mode,
                             helper::Comm comm)
: Engine("JuleaReader", io, name, mode, std::move(comm))

{
    // m_EndMessage = " in call to IO Open JuleaReader " + m_Name + "\n";

    // MPI_Comm_rank(mpiComm, &m_ReaderRank); //TODO: change in release_25
    m_ReaderRank = m_Comm.Rank();
    Init();
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << " Open(" << m_Name
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
        std::cout << "Julea Reader " << m_ReaderRank << " deconstructor on "
                  << m_Name << "\n";
    }
    j_semantics_unref(m_JuleaSemantics);
}

StepStatus JuleaKVReader::BeginStep(const StepMode mode,
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

    if (m_FirstStep)
    {
        m_FirstStep = false;
    }
    else
    {
        // HELP! what is this comment supposed to mean?!
        // step info should be received from the writer side in BeginStep()
        // so this forced increase should not be here
        ++m_CurrentStep;
    }

    // used to inquire for variables in streaming mode
    m_IO.m_ReadStreaming = true;
    m_IO.m_EngineStep = m_CurrentStep;

    // HELP! check this brilliant comment...
    // If we reach the end of stream (writer is gone or explicitly tells the
    // reader)
    // we return EndOfStream to the reader application
    // if (m_CurrentStep == 2)
    if (m_CurrentStep >= m_MetadataSet.StepsCount)
    {
        std::cout << "Julea Reader " << m_ReaderRank
                  << "   forcefully returns End of Stream at this step\n";
        m_IO.m_ReadStreaming = false;

        // HELP! comment?!
        // We should block until a new step arrives or reach the timeout
        // m_IO Variables and Attributes should be defined at this point
        // so that the application can inquire them and start getting data
        return StepStatus::EndOfStream;
    }

    m_IO.ResetVariablesStepSelection(false,
                                     "in call to Julea Reader BeginStep");

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank
                  << "   BeginStep() new step " << m_CurrentStep << "\n";
    }
    return StepStatus::OK;
}

size_t JuleaKVReader::CurrentStep() const
{
    // std::cout << "JULEA ENGINE: CurrentStep" << std::endl;
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank
                  << "   CurrentStep() returns " << m_CurrentStep << "\n";
    }
    return m_CurrentStep;
}

void JuleaKVReader::EndStep()
{
    // EndStep should call PerformGets() if there are unserved GetDeferred()
    // requests
    if (m_NeedPerformGets)
    {
        PerformGets();
    }

    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << "   EndStep()\n";
    }
    // TODO: Can reading happen in steps?
    // if (m_CurrentStep % m_FlushStepsCount == 0){
    //     Flush();
    // }
}

/*
    PerformGets similar to BP-Engine

 */
void JuleaKVReader::PerformGets()
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << "     PerformGets()\n";
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
            std::cout << "Julea Reader " << m_ReaderRank << "     PerformGets()"
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
    void JuleaKVReader::DoGetSync(Variable<T> &variable, T *data)              \
    {                                                                          \
        GetSyncCommon(variable, data);                                         \
    }                                                                          \
    void JuleaKVReader::DoGetDeferred(Variable<T> &variable, T *data)          \
    {                                                                          \
        GetDeferredCommon(variable, data);                                     \
    }
// ADIOS2_FOREACH_TYPE_1ARG(declare_type)
ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type

void JuleaKVReader::Init()
{
    std::cout << "\n*********************** JULEA ENGINE READER "
                 "*************************"
              << std::endl;
    std::cout << "JULEA READER: Init" << std::endl;
    std::cout
        << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
        << std::endl;
    std::cout << "JULEA READER: Namespace = " << m_Name << std::endl;

    if (m_DebugMode)
    {
        if (m_OpenMode != Mode::Read)
        {
            throw std::invalid_argument(
                "ERROR: JuleaReader only supports OpenMode::Read from" +
                m_Name + " " + m_EndMessage);
        }
    }
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << " Init()\n";
    }

    // j_init();
    m_JuleaSemantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

    InitParameters();
    InitTransports();
    InitVariables();
    InitAttributes();
}

/**
 * Initializes variables so that InquireVariable can find the variable in the IO
 * map. Since the m_Variables is private the only solution is to define each
 * variable with the according parameters.
 */
// template <class T>
void JuleaKVReader::InitVariables()
{
    bson_iter_t b_iter;
    bson_t *bsonNames;
    std::string varName;
    std::string nameSpace = m_Name;
    std::string kvName = "variable_names";
    unsigned int varCount = 0;

    GetVarNamesFromJulea(nameSpace, &bsonNames, &varCount);

    if (varCount == 0)
    {
        std::cout << "++ InitVariables: no variables stored in KV" << std::endl;
    }
    else
    {
        bson_iter_init(&b_iter, bsonNames);

        std::cout << "-- bsonNames length: " << bsonNames->len << std::endl;

        while (bson_iter_next(&b_iter))
        {
            // bson_t *bsonMetadata;
            Dims shape = nullptr; //FIXME
            Dims start = nullptr;
            Dims count = nullptr;
            bool constantDims;
            int type;
            guint32 buffer_len;
            gpointer md_buffer = nullptr;
            // JuleaKVReader::StepMetadata md;
            // varName = g_strdup(bson_iter_key(&b_iter));
            std::string varName(bson_iter_key(&b_iter));

            std::cout << "-- Variable name " << varName << std::endl;
            GetVariableMetadataFromJuleaNew(nameSpace, varName, &md_buffer,
                                            &buffer_len);
            std::cout << "buffer_len = " << buffer_len << std::endl;
            std::cout << "buffer_len = " << &buffer_len << std::endl;
            DeserializeVariableMetadata(md_buffer, &type, &shape, &start,
                                        &count, &constantDims);
            // GetVariableMetadataFromJuleaNew(nameSpace, varName, md_buffer,
            // buffer_len);
            // TODO: rewrite variablemetadata so that the variable knows shape,
            // start, count so it is not necessary to ask every block?! check BP

            // GetVariableBSONFromJulea(nameSpace, varName, &bsonMetadata);
            std::cout << "DEBUG 1" << std::endl;
            // std::cout << "numberSteps" << md.numberSteps << std::endl;
            // std::cout << "type" << md.type << std::endl;
            // std::cout << "isConstantDims" << md.isConstantDims << std::endl;

            // GetVariableMetadataForInitFromBSON(nameSpace, varName,
            // bsonMetadata,
            //                                    &type, &shape, &start, &count,
            //                                    &constantDims);
            // bson_destroy(bsonMetadata);
            DefineVariableInInit(&m_IO, varName, type, shape, start, count,
                                 constantDims);
            // free(varName);
            // free(&varName);
        }
        // free(varName);
        // TODO how to free varName?
        bson_destroy(bsonNames);
    }
}

void JuleaKVReader::InitAttributes()
{
    bson_iter_t b_iter;
    bson_t *bsonNames;

    // std::string *attrName;
    std::string nameSpace = m_Name;
    std::string kvName = "attribute_names";

    unsigned int attrCount;
    long unsigned int completeSize;
    unsigned long *dataSizes;

    std::cout << "\n______________InitAttributes_____________________"
              << std::endl;
    GetVarNamesFromJulea(nameSpace, &bsonNames,
                         &attrCount); // TODO: get all attribute names

    if (attrCount == 0)
    {
        return;
    }
    else
    {
        bson_iter_init(&b_iter, bsonNames);
    }

    std::cout << "-- bsonNames length: " << bsonNames->len << std::endl;

    while (bson_iter_next(&b_iter))
    {
        std::string typeString;
        // bson_t *bsonMetadata;
        attrCount = 0;
        completeSize = 0;
        dataSizes = NULL;
        // attrName = g_strdup(bson_iter_key(&b_iter));
        // *attrName = strdup(bson_iter_key(&b_iter));
        std::string attrName(bson_iter_key(&b_iter));
        int type = 0;
        size_t numberElements = 0;
        bool IsSingleValue = false;

        std::cout << "-----------------------------------" << std::endl;
        std::cout << "-- Attribute name " << attrName << std::endl;

        // GetAttributeMetadataFromJulea(attrName, bsonMetadata, nameSpace,
        //                               &completeSize, &numberElements,
        //                               &IsSingleValue, &type, &dataSizes);
        GetAttributeMetadataFromJulea(attrName, nameSpace, &completeSize,
                                      &numberElements, &IsSingleValue, &type,
                                      &dataSizes);
        // std::cout << "Data size = " << completeSize << std::endl;

        GetAdiosTypeString(type, &typeString);

#define declare_attribute_type(T)                                              \
    if (typeString == helper::GetType<T>())                                    \
    {                                                                          \
        if (typeString == "string")                                            \
        {                                                                      \
            char *data = new char[completeSize];                               \
            GetAttributeStringDataFromJulea(attrName, data, nameSpace,         \
                                            completeSize, IsSingleValue,       \
                                            numberElements);                   \
            if (IsSingleValue)                                                 \
            {                                                                  \
                std::string dataString(data);                                  \
                std::cout << "Data: " << dataString << std::endl;              \
                m_IO.DefineAttribute<std::string>(attrName, dataString);       \
            }                                                                  \
            else                                                               \
            {                                                                  \
                std::vector<std::string> dataStringArray;                      \
                unsigned long offset = 0;                                      \
                for (size_t i = 0; i < numberElements; ++i)                    \
                {                                                              \
                    dataStringArray.push_back(data + offset);                  \
                    std::cout << "data[" << offset << "]: " << data + offset   \
                              << std::endl;                                    \
                    offset += dataSizes[i];                                    \
                }                                                              \
                m_IO.DefineAttribute<std::string>(                             \
                    attrName, dataStringArray.data(), numberElements);         \
            }                                                                  \
            delete[] data;                                                     \
            delete[] dataSizes;                                                \
        }                                                                      \
        else                                                                   \
        {                                                                      \
            T *dataBuf = NULL;                                                 \
            dataBuf = (T *)g_slice_alloc(completeSize);                        \
            GetAttributeDataFromJulea(attrName, dataBuf, nameSpace,            \
                                      completeSize);                           \
            if (IsSingleValue)                                                 \
            {                                                                  \
                std::cout << "Data: " << *dataBuf << std::endl;                \
                m_IO.DefineAttribute<T>(attrName, *dataBuf);                   \
            }                                                                  \
            else                                                               \
            {                                                                  \
                std::cout << "Data: " << *dataBuf << std::endl;                \
                m_IO.DefineAttribute<T>(attrName, dataBuf, numberElements);    \
            }                                                                  \
            g_slice_free(T, dataBuf);                                          \
        }                                                                      \
    }
        ADIOS2_FOREACH_ATTRIBUTE_STDTYPE_1ARG(declare_attribute_type)
#undef declare_attribute_type
        // delete (&attrName); // FIXME: how to free?
        // free(attrName);
    } // end while
    bson_destroy(bsonNames);
    std::cout << "_________________________________________________\n"
              << std::endl;
}

void JuleaKVReader::InitParameters()
{
    // for (const auto &pair : m_IO.m_Parameters)
    // {
    //     std::string key(pair.first);
    //     std::transform(key.begin(), key.end(), key.begin(), ::tolower);

    //     std::string value(pair.second);
    //     std::transform(value.begin(), value.end(), value.begin(), ::tolower);

    //     if (key == "verbose")
    //     {
    //         m_Verbosity = std::stoi(value);
    //         if (m_DebugMode)
    //         {
    //             if (m_Verbosity < 0 || m_Verbosity > 5)
    //                 throw std::invalid_argument(
    //                     "ERROR: Method verbose argument must be an "
    //                     "integer in the range [0,5], in call to "
    //                     "Open or Engine constructor\n");
    //         }
    //     }
    // }
    // if (m_Verbosity == 5)
    // {
    //     std::cout << "Julea Reader " << m_ReaderRank << "
    //     InitParameters()\n";
    // }
}

void JuleaKVReader::InitTransports()
{
    // FIXME
    // DESIGN
    // if (m_IO.m_TransportsParameters.empty())
    // {
    //     Params defaultTransportParameters;
    //     defaultTransportParameters["transport"] = "File";
    //     m_IO.m_TransportsParameters.push_back(defaultTransportParameters);
    // }
    // // TODO Set Parameters

    // if (m_BP3Deserializer.m_RankMPI == 0)
    // {
    //     const std::string metadataFile(
    //         m_BP3Deserializer.GetBPMetadataFileName(m_Name));

    //     const bool profile = m_BP3Deserializer.m_Profiler.IsActive;
    //     //FIXME: m_FileManager needed? or different solution?
    //     // m_FileManager.OpenFiles({metadataFile}, adios2::Mode::Read,
    //                             // m_IO.m_TransportsParameters, profile);
    // }

    // Nothing to process from m_IO.m_TransportsParameters
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << " InitTransports()\n";
    }
}

void JuleaKVReader::DoClose(const int transportIndex)
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << " Close(" << m_Name
                  << ")\n";
    }
    // g_free(m_JuleaInfo->nameSpace);
    // g_slice_free(JuleaInfo, m_JuleaInfo);
}
/// FIXME: implement AllStepsBlocksInfo
// return m_BP4Deserializer.AllStepsBlocksInfo(variable);                 \
        return m_BP4Deserializer.BlocksInfo(variable, step);                   \

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
//                                                                            \
    // std::vector<typename Variable<T>::Info> JuleaKVReader::DoBlocksInfo(       \
    //     const Variable<T> &variable, const size_t step)                   \
    // {                                                                          \
    //     TAU_SCOPED_TIMER("JuleaKVReader::BlocksInfo");                         \
    //     BlocksInfo(variable,step);\
    // }

} // end namespace engine
} // end namespace core
} // end namespace adios2
