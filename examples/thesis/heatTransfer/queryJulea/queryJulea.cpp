/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * main.cpp
 *
 * Analyses heatTransfer Output
 *
 * Created on: Oct 2020
 *     Author: Kira Duwe
 *
 */
#include <mpi.h>

#include <fstream>
#include <mpi.h>

#include "adios2.h"

#include <chrono>
using namespace std::chrono;
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <math.h>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "JuleaQueryPrintDataStep.h"
#include "JuleaQuerySettings.h"

#include <julea-db.h>
#include <julea-dai.h>
#include <julea.h>

auto startRead = high_resolution_clock::now();
auto stopRead = high_resolution_clock::now();

auto startCompute = high_resolution_clock::now();
auto stopCompute = high_resolution_clock::now();

void printElements(std::vector<double> Tin)
{
    // std::ofstream outFile;
    // outFile.open(engineType + "-readOutput.txt");
    // std::cout << "engine type: " << engineType << std::endl;
    // outFile.open(inIO.EngineType() + "-readOutput.txt");
    // std::cout << "engine type: " << inIO.EngineType() << std::endl;
    /*
     * Print every element in Tin
     */
    double sum = 0;
    int i = 0;
    for (auto &el : Tin)
    {
        sum += el;
        if (i % 10 == 0)
        {
            std::cout << std::endl;
            // outFile << std::endl;
        }
        std::cout << el << " ";
        // outFile << el << " ";
        i++;
    }
    std::cout << "\n"
              << "sum: " << sum << std::endl;
    // outFile << "\n"
    // << "sum: " << sum << std::endl;
}

void printUsage()
{
    std::cout << "Usage: heatQuery  config  input  output N  M \n"
              << "  config:  XML config file to use\n"
              << "  input:   name of input data file/stream\n"
              << std::endl;
    // << "  output:  name of output data file/stream\n"
    // << "  N:       number of processes in X dimension\n"
    // << "  M:       number of processes in Y dimension\n\n";
}

void printQueryDurations(
    std::chrono::time_point<std::chrono::high_resolution_clock> stopRead,
    std::chrono::time_point<std::chrono::high_resolution_clock> startRead,
    std::chrono::time_point<std::chrono::high_resolution_clock> stopCompute,
    std::chrono::time_point<std::chrono::high_resolution_clock> startCompute,
    std::chrono::time_point<std::chrono::high_resolution_clock> stopAnalysis,
    std::chrono::time_point<std::chrono::high_resolution_clock> startAnalysis)
{
    auto durationRead = duration_cast<microseconds>(stopRead - startRead);

    auto durationAnalysis =
        duration_cast<microseconds>(stopAnalysis - startAnalysis);
    auto durationCompute =
        duration_cast<microseconds>(stopCompute - startCompute);

    std::cout << durationRead.count() << " \t " << durationCompute.count()
              << " \t " << durationAnalysis.count() << std::endl;
}

void JuleaReadMetadata(std::string projectNamespace, std::string fileName,
                       std::string variableName, size_t step,
                       std::vector<double> &means)
{
    size_t err = 0;
    size_t db_length = 0;
    gchar *db_field = NULL;
    JDBType type;
    JDBSchema *schema = NULL;
    JDBEntry *entry = NULL;
    JDBIterator *iterator = NULL;
    JDBSelector *selector = NULL;

    std::string adiosType = "double";
    std::string meanField = "mean_float64";

    uint32_t *tmpID;
    double *mean;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "block-metadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "projectNamespace",
                            J_DB_SELECTOR_OPERATOR_EQ,
                            projectNamespace.c_str(),
                            strlen(projectNamespace.c_str()) + 1, NULL);
    j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
                            fileName.c_str(), strlen(fileName.c_str()) + 1,
                            NULL);
    j_db_selector_add_field(selector, "variableName", J_DB_SELECTOR_OPERATOR_EQ,
                            variableName.c_str(),
                            strlen(variableName.c_str()) + 1, NULL);
    j_db_selector_add_field(selector, "step", J_DB_SELECTOR_OPERATOR_EQ, &step,
                            sizeof(step), NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);

    // TODO sort after blockID not entryID
    while (j_db_iterator_next(iterator, NULL))
    {
        j_db_iterator_get_field(iterator, "_id", &type, (gpointer *)&tmpID,
                                &db_length, NULL);
        j_db_iterator_get_field(iterator, meanField.c_str(), &type,
                                (gpointer *)&mean, &db_length, NULL);
        // std::cout << "_id: " << *tmpID << std::endl;
        means.push_back(*mean);
        // std::cout << "mean: " << *mean << std::endl;
    }

    j_db_schema_unref(schema);
    j_db_selector_unref(selector);
}

// void ReadAllInRange(std::string projectNamespace, std::string fileName)
// {
//     // TODO: call DAI function
//     double* means;
//     j_dai_get_results_in_range_d(projectNamespace.c_str(), fileName.c_str(), "T", J_DAI_STAT_MEAN, -42, 42, J_DAI_GRAN_BLOCK, &means);
// }

void ComputeAllInRange(std::string projectNamespace, std::string fileName)
{
    // TODO: call DAI function
}

// get all blocks where the temperature is between -42 and 42
void QueryAllInRange(std::string projectNamespace, std::string fileName)
{
    // double* means;
    size_t* entryIDs;
    size_t* coordinates;
    size_t numberResults;
    size_t nDims;

    startRead = high_resolution_clock::now();
    j_dai_get_entries_in_range_d(projectNamespace.c_str(), fileName.c_str(), "T", J_DAI_STAT_MEAN, -42, 42, J_DAI_GRAN_BLOCK, &numberResults, &entryIDs);

    // for (size_t entry: entryIDs)
    for (int i = 0; i < numberResults; ++i)
    {
        j_dai_get_coordinates_for_entry(projectNamespace.c_str(), entryIDs[i], &nDims, &coordinates);
        //TODO: remove for evaluation
        for(int j = 0; j < nDims; ++j)
        {
            std::cout << "X:" << coordinates[0] << "\n";
            std::cout << "Y:" << coordinates[1] << "\n";
        }
    }
    stopRead = high_resolution_clock::now();
    startCompute = high_resolution_clock::now();
    //nothing to compute here
    stopCompute = high_resolution_clock::now();

}

// Find the highest mean value for variable
void QueryHighestMean(std::string projectNamespace, std::string fileName)
{
    double result;
    startRead = high_resolution_clock::now();
    j_dai_get_max_stat_d(projectNamespace.c_str(), fileName.c_str(), "T", J_DAI_STAT_MEAN, J_DAI_GRAN_BLOCK, &result);
        stopRead = high_resolution_clock::now();
    startCompute = high_resolution_clock::now();
    //nothing to compute here
    stopCompute = high_resolution_clock::now();
}

// Find biggest difference in max temperature between step 1 and step 8760 -> one year later
void QueryDrasticLocalChangeInTimeInterval(std::string projectNamespace, std::string fileName)
{
    //get max temperatures for step 1 and step 8760
    // compute difference for each block
    // find max difference
    size_t* coordinates;
    size_t* nDims;

    void j_dai_get_global_coordinates(projectNamespace.c_str(), fileName.c_str(), size_t enentry_idtryID, size_t* n_dims, &coordinates);
    void j_dai_get_entry_ids(projectNamespace.c_str(), fileName.c_str(), "T", JDAIStatistic statistic, size_t threshold, JDAIOperator op, JDAIGranularity granularity, size_t** entry_ids)
    // while (i < nDims)
    // {
        for (int j= 0; j < coordinates[i]; ++j )
        {
            for(int k = 0; k < coordinates[i+1]; ++k)
            {
                void j_dai_entry_get_stat_d(gchar const* name_space,size_t entry_id, JDAIStatistic statistic, double* result);

            }
        }
        // j_dai_get_block_result_d((projectNamespace.c_str(), fileName.c_str(), "T", 0, size_t block, JDAIStatistic statistic, size_t threshold, J_DAI_OP_NULL, double** result);
        
    // }
}

// void ReadQuery(JuleaQuerySettings::JuleaQueryID queryID, std::string projectNamespace, std::string fileName)
void Query(JuleaQuerySettings::JuleaQueryID queryID, std::string projectNamespace, std::string fileName)
{
    switch (queryID)
    {
    case JuleaQuerySettings::JQUERY_ALL_IN_RANGE:
        QueryAllInRange(projectNamespace, fileName);
        break;
    case JuleaQuerySettings::JQUERY_HIGHEST_MEAN:
        QueryHighestMean(projectNamespace, fileName);
        break;
    case JuleaQuerySettings::JQUERY_DRASTIC_LOCAL_CHANGE_IN_TIME:
        // Do stuff
        break;
    case JuleaQuerySettings::JQUERY_CI_DAYS:
        // Do stuff
        break;
    case JuleaQuerySettings::JQUERY_NUMBER_DAYS_COLDER_THAN:
        // Do stuff
        break;
    case JuleaQuerySettings::JQUERY_LOWEST_VARIANCE_OVER_FILES:
        // Do stuff
        break;
    case JuleaQuerySettings::JQUERY_RAIN_TEMP_COMBINED:
        // Do stuff
        break;
    }
}

void SetupQueries(std::vector<JuleaQuerySettings::JuleaQueryID> allQueries)
{
    allQueries.push_back(JuleaQuerySettings::JQUERY_ALL_IN_RANGE);
    allQueries.push_back(JuleaQuerySettings::JQUERY_HIGHEST_MEAN);
    allQueries.push_back(JuleaQuerySettings::JQUERY_DRASTIC_LOCAL_CHANGE_IN_TIME);
    allQueries.push_back(JuleaQuerySettings::JQUERY_CI_DAYS);
    allQueries.push_back(JuleaQuerySettings::JQUERY_NUMBER_DAYS_COLDER_THAN);
    allQueries.push_back(JuleaQuerySettings::JQUERY_LOWEST_VARIANCE_OVER_FILES);
    allQueries.push_back(JuleaQuerySettings::JQUERY_RAIN_TEMP_COMBINED);
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    /* When writer and reader is launched together with a single mpirun command,
       the world comm spans all applications. We have to split and create the
       local 'world' communicator mpiHeatTransferComm for the writer only.
       When writer and reader is launched separately, the mpiHeatTransferComm
       communicator will just equal the MPI_COMM_WORLD.
     */

    int wrank, wnproc;
    MPI_Comm_rank(MPI_COMM_WORLD, &wrank);
    MPI_Comm_size(MPI_COMM_WORLD, &wnproc);

    // const unsigned int color = 1;
    const unsigned int color = 2;

    MPI_Comm mpiQueryComm;
    MPI_Comm_split(MPI_COMM_WORLD, color, wrank, &mpiQueryComm);

    int rank, nproc;
    MPI_Comm_rank(mpiQueryComm, &rank);
    MPI_Comm_size(mpiQueryComm, &nproc);

    // auto startRead = high_resolution_clock::now();
    // auto stopRead = high_resolution_clock::now();

    // auto startCompute = high_resolution_clock::now();
    // auto stopCompute = high_resolution_clock::now();

    auto startAnalysis = high_resolution_clock::now();
    auto stopAnalysis = high_resolution_clock::now();

    try
    {
        double timeStart = MPI_Wtime();
        JuleaQuerySettings settings(argc, argv, rank, nproc);
        // std::cout << settings.inputfile << std::endl;
        // std::cout << "steps = " << settings.steps << std::endl;
        settings.m_ProjectNamespace = "Postprocess_evaluation";
        std::vector<JuleaQuerySettings::JuleaQueryID> allQueries;
        SetupQueries(allQueries);
        

        if (rank == 0)
        {
            std::cout << "\n# Read \t Compute \t Analysis" << std::endl;
        }

        // evaluate all post-processing queries
        for (auto &element : allQueries)
        {
            startAnalysis = high_resolution_clock::now();
            // startRead = high_resolution_clock::now();

            Query(element, settings.m_ProjectNamespace, settings.m_Inputfile);
            // ReadQuery(element, settings.m_ProjectNamespace, settings.m_Inputfile);

            // stopRead = high_resolution_clock::now();
            // startCompute = high_resolution_clock::now();

            // ComputeQuery(element, settings.m_ProjectNamespace, settings.m_Inputfile);

            // stopCompute = high_resolution_clock::now();
            stopAnalysis = high_resolution_clock::now();

            printQueryDurations(stopRead, startRead, stopCompute, startCompute,
                                stopAnalysis, startAnalysis);
        }

        double timeEnd = MPI_Wtime();
        if (rank == 0)
            std::cout << "Total runtime = " << timeEnd - timeStart << "s\n"
                      << std::endl;
    }
    catch (std::invalid_argument &e) // command-line argument errors
    {
        std::cout << e.what() << std::endl;
        printUsage();
    }
    catch (std::ios_base::failure &e) // I/O failure (e.g. file not found)
    {
        std::cout << "I/O base exception caught\n";
        std::cout << e.what() << std::endl;
    }
    catch (std::exception &e) // All other exceptions
    {
        std::cout << "Exception caught\n";
        std::cout << e.what() << std::endl;
    }

    MPI_Finalize();
    return 0;
}
