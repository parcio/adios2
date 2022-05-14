/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 *  Created on: April 2022
 *      Author: Kira Duwe
 */

#include <chrono>
#include <ios>       //std::ios_base::failure
#include <iostream>  //std::cout
#include <stdexcept> //std::invalid_argument std::exception
#include <thread>
#include <vector>

#include <adios2.h>
#include <julea.h>
#include <julea-dai.h>

void TestDAISettings()
{
    /** Application variable */
    std::vector<float> myFloats = {12345.6, 1, 2, 3, 4, 5, 6, 7, 8, -42.333};
    std::vector<int> myInts = {333, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    const std::size_t Nx = myFloats.size();
    const std::size_t Nx2 = myInts.size();
    std::string fileName = "testFile.jb";
    std::string varName = "juleaFloats";
    std::string varName2 = "juleaInts";

    // JDAIOperator compare = J_DAI_OP_GT;
    // JDAIStatistic statistic = J_DAI_STAT_MAX;
    // JDAITagGranularity granularity = J_DAI_TGRAN_BLOCK;

    std::cout << "JuleaEngineTest Writing ... " << std::endl;
    /** ADIOS class factory of IO class objects, DebugON is recommended */
    adios2::ADIOS adios(adios2::DebugON);

    /*** IO class object: settings and factory of Settings: Variables,
     * Parameters, Transports, and Execution: Engines */
    adios2::IO juleaIO = adios.DeclareIO("juleaIO");

    // juleaIO.SetEngine("julea-kv");
    juleaIO.SetEngine("julea-db-dai");
    // juleaIO.SetEngine("julea-db");


    /** global array: name, { shape (total dimensions) }, { start (local) },
     * { count (local) }, all are constant dimensions */
    adios2::Variable<float> juleaFloats = juleaIO.DefineVariable<float>(
        varName, {Nx}, {0}, {Nx}, adios2::ConstantDims);
     
    adios2::Variable<int> juleaInts = juleaIO.DefineVariable<int>(
        varName2, {Nx2}, {0}, {Nx2}, adios2::ConstantDims);

    /** Engine derived class, spawned to start IO operations */
    adios2::Engine juleaWriter = juleaIO.Open(fileName, adios2::Mode::Write);

    // This is probably when the DAI call should happen at latest. Maybe even at earliest
    // j_dai_tag_feature_i(fileName, varName, "test_hot_days", J_DAI_STAT_MAX, 25, J_DAI_OP_GT, J_DAI_GRAN_BLOCK );
    // j_dai_tag_feature_i(fileName, varName, "test_hot_days", statistic, 25, compare, granularity );

    /** Write variable for buffering */
    juleaWriter.Put<float>(juleaFloats, myFloats.data(), adios2::Mode::Deferred);
    juleaWriter.Put<int>(juleaInts, myInts.data(), adios2::Mode::Deferred);


    /** Create bp file, engine becomes unreachable after this*/
    juleaWriter.Close();
}






int main(int argc, char *argv[])
{
    /** Application variable */
    std::vector<float> myFloats = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    const std::size_t Nx = myFloats.size();
    int err = -1;

    try
    {
        std::cout << "JuleaDAITest :)" << std::endl;
        TestDAISettings();
        std::cout << "\n JuleaDAITest :) Write variable finished \n"
                  << std::endl;
    }
    catch (std::invalid_argument &e)
    {
        std::cout << "Invalid argument exception, STOPPING PROGRAM\n";
        std::cout << e.what() << "\n";
    }
    catch (std::ios_base::failure &e)
    {
        std::cout << "IO System base failure exception, STOPPING PROGRAM\n";
        std::cout << e.what() << "\n";
    }
    catch (std::exception &e)
    {
        std::cout << "Exception, STOPPING PROGRAM\n";
        std::cout << e.what() << "\n";
    }

    return 0;
}
