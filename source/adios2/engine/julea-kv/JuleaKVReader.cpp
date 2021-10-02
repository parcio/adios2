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

JuleaKVReader::JuleaKVReader(IO &io, const std::string &name, const Mode mode,
                             helper::Comm comm)
: Engine("JuleaReader", io, name, mode, std::move(comm))

{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank << " Open(" << m_Name
                  << ") in constructor." << std::endl;
    }

    // MPI_Comm_rank(mpiComm, &m_ReaderRank); //TODO: change in release_25
    m_ReaderRank = m_Comm.Rank();
    Init();
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
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank
                  << "   BeginStep() new step " << m_CurrentStep << "\n";
    }
    // if (m_DebugMode)
    // {
        if (!m_DeferredVariables.empty())
        {
            throw std::invalid_argument(
                "ERROR: existing variables subscribed with "
                "GetDeferred, did you forget to call "
                "PerformGets() or EndStep()?, in call to BeginStep\n");
        }
    // }

    m_IO.m_ReadStreaming = true;
    m_IO.m_EngineStep = m_CurrentStep;

    m_StepMode = mode;
    m_DeferredVariables.clear();
    m_DeferredVariablesDataSize = 0;

    // first param is "zero-init" which initializes stepsStart to 0
    m_IO.ResetVariablesStepSelection(true, "in call to JULEA Reader BeginStep");

    return StepStatus::OK;
}

size_t JuleaKVReader::CurrentStep() const
{
    if (m_Verbosity == 5)
    {
        std::cout << "Julea Reader " << m_ReaderRank
                  << "   CurrentStep() returns " << m_CurrentStep << "\n";
    }
    return m_CurrentStep;
}

void JuleaKVReader::EndStep()
{
    if (m_Verbosity == 5)
    {
        std::cout << "\n______________EndStep _____________________"
                  << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank << "   EndStep()\n";
    }

    if (m_DeferredVariables.size() > 0)
    {
        // std::cout << "m_DeferredVariables.size() = "
                  // << m_DeferredVariables.size() << std::endl;
        PerformGets();
    }

    // PutAttributes(m_IO); //TODO: get Attributes?

    /* advance step */
    ++m_CurrentStep;

    // if (m_CurrentStep % m_FlushStepsCount == 0)
    // {
    //     Flush();
    // }
    m_CurrentBlockID = 0;
}

void JuleaKVReader::PerformGets()
{
    if (m_Verbosity == 5)
    {
        std::cout << "--- PerformGets --- " << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank << "     PerformGets()\n";
    }

    /** if there are no deferred variables there is nothing to do */
    if (m_DeferredVariables.empty())
    {
        return;
    }
    size_t i = 0;

    /** Call GetSyncCommon for every variable that has been deferred */
    for (const std::string &variableName : m_DeferredVariables)
    {
        const DataType type = m_IO.InquireVariableType(variableName);
        if (type == DataType::Compound)
        {
            // not supported
            std::cout << "Julea Reader " << m_ReaderRank << "     PerformGets()"
                      << "compound variable type not supported \n";
        }
#define declare_type(T)                                                        \
    else if (type == helper::GetDataType<T>())                                     \
    {                                                                          \
        Variable<T> &variable = FindVariable<T>(                               \
            variableName,"in call to PerformGets, EndStep or Close");         \
        for (auto &blockInfo : variable.m_BlocksInfo)                          \
        {                                                                      \
            T *data = variable.m_BlocksInfo[i].Data;                           \
            ReadBlock(variable, data, i);                                      \
            i++;                                                               \
        }                                                                      \
        variable.m_BlocksInfo.clear();                                         \
    }
        // ADIOS2_FOREACH_TYPE_1ARG(declare_type) //TODO:Why is this different
        // from Writer?
        ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
    }
    m_DeferredVariables.clear();
}
// ReadVariableBlocks(variable);                                          \

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

/**
 * Init the engine. Prints a lovely penguin at the beginning :-).
 */
void JuleaKVReader::Init()
{

    // if (m_DebugMode)
    // {
        if (m_OpenMode != Mode::Read)
        {
            throw std::invalid_argument(
                "ERROR: JuleaReader only supports OpenMode::Read from" +
                m_Name + " " + m_EndMessage);
        }
    // }
    if (m_Verbosity == 5)
    {
    std::cout << "\n*********************** JULEA ENGINE READER "
                 "*************************"
              << std::endl;
    std::cout << "JULEA KV READER: Init" << std::endl;
    std::cout
        << "      .___. \n     /     \\ \n    | O _ O | \n    /  \\_/  \\ \n  .' / \
    \\ `. \n / _|       |_ \\ \n(_/ |       | \\_) \n    \\       / \n   __\\_>-<_/__ \
         \n   ~;/     \\;~"
        << std::endl;
    std::cout << "JULEA KV READER: Namespace = " << m_Name << std::endl;
        std::cout << "Julea Reader " << m_ReaderRank << " Init()\n";
    }

    m_JuleaSemantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

    // InitParameters();
    // InitTransports();
    InitVariables();
    InitAttributes();
}

/**
 * Initializes variables so that InquireVariable can find the variable in the IO
 * map. Since the m_Variables is private the only solution is to define each
 * variable with the according parameters.
 */
void JuleaKVReader::InitVariables()
{
    bson_iter_t b_iter;
    bson_t *bsonNames;
    std::string varName;
    std::string nameSpace = m_Name;
    std::string kvName = "variable_names";
    unsigned int varCount = 0;

    GetNamesFromJulea(nameSpace, &bsonNames, &varCount, true);

    if (varCount == 0)
    {
        if (m_Verbosity == 5)
        {
            std::cout << "++ InitVariables: no variables stored in KV"
                      << std::endl;
        }
    }
    else
    {
        bson_iter_init(&b_iter, bsonNames);

        // std::cout << "-- bsonNames length: " << bsonNames->len << std::endl;

        while (bson_iter_next(&b_iter))
        {
            Dims shape;
            Dims start;
            Dims count;
            ShapeID shapeID = ShapeID::Unknown;

            bool constantDims;
            bool isReadAsJoined;
            bool isReadAsLocalValue;
            bool isRandomAccess;

            std::string type;
            guint32 buffer_len;
            gpointer md_buffer = nullptr;
            size_t *blocks = nullptr;
            size_t numberSteps = 0;

            std::string varName(bson_iter_key(&b_iter));

            if(varName =="time")
            {
                continue;
            }
            GetVariableMetadataFromJulea(nameSpace, varName, &md_buffer,
                                         &buffer_len);

            DeserializeVariableMetadata(md_buffer, &type, &shape, &start,
                                        &count, &constantDims, &blocks,
                                        &numberSteps, &shapeID, &isReadAsJoined,
                                        &isReadAsLocalValue, &isRandomAccess);

            DefineVariableInInit(&m_IO, varName, type, shape, start, count,
                                 constantDims);

            /** there are several fields that need to be set in a variable that
             * are not required when defining a variable in the IO. Therefore
             * they need to be set now. */
            InitVariable(&m_IO, *this, varName, blocks, numberSteps, shapeID);
            delete[] blocks;

            // TODO necessary? 2.10.21 does not seem so
            // const std::string testtype = m_IO.InquireVariableType(varName);
        }
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

    if (m_Verbosity == 5)
    {
        std::cout << "\n______________InitAttributes_____________________"
                  << std::endl;
    }
    GetNamesFromJulea(nameSpace, &bsonNames, &attrCount,
                      false); // TODO: get all attribute names

    if (attrCount == 0)
    {
        return;
    }
    else
    {
        bson_iter_init(&b_iter, bsonNames);
    }

    // std::cout << "-- bsonNames length: " << bsonNames->len << std::endl;

    while (bson_iter_next(&b_iter))
    {
        // std::string typeString;
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

        // std::cout << "-----------------------------------" << std::endl;
        // std::cout << "-- Attribute name " << attrName << std::endl;

        // GetAttributeMetadataFromJulea(attrName, bsonMetadata, nameSpace,
        //                               &completeSize, &numberElements,
        //                               &IsSingleValue, &type, &dataSizes);
        GetAttributeMetadataFromJulea(attrName, nameSpace, &completeSize,
                                      &numberElements, &IsSingleValue, &type,
                                      &dataSizes);
        // std::cout << "Data size = " << completeSize << std::endl;

        // GetAdiosTypeString(type, &typeString);

#define declare_attribute_type(T)                                              \
    if (type == helper::GetDataType<T>())                                    \
    {                                                                          \
        if (typeString == DataType::String)                                            \
        {                                                                      \
            char *data = new char[completeSize];                               \
            GetAttributeStringDataFromJulea(attrName, data, nameSpace,         \
                                            completeSize, IsSingleValue,       \
                                            numberElements);                   \
            if (IsSingleValue)                                                 \
            {                                                                  \
                std::string dataString(data);                                  \
                m_IO.DefineAttribute<std::string>(attrName, dataString);       \
            }                                                                  \
            else                                                               \
            {                                                                  \
                std::vector<std::string> dataStringArray;                      \
                unsigned long offset = 0;                                      \
                for (size_t i = 0; i < numberElements; ++i)                    \
                {                                                              \
                    dataStringArray.push_back(data + offset);                  \
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
                m_IO.DefineAttribute<T>(attrName, *dataBuf);                   \
            }                                                                  \
            else                                                               \
            {                                                                  \
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
    // std::cout << "_________________________________________________\n"
              // << std::endl;
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

} // end namespace engine
} // end namespace core
} // end namespace adios2
