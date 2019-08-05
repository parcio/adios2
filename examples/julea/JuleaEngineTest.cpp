/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 *  Created on: Jan 2018
 *      Author: Norbert Podhorszki
 */

#include <chrono>
#include <ios>       //std::ios_base::failure
#include <iostream>  //std::cout
#include <stdexcept> //std::invalid_argument std::exception
#include <thread>
#include <vector>

#include <adios2.h>

int write_test(){
 /** Application variable */
    std::vector<float> myFloats = {12345.6, 1, 2, 3, 4, 5, 6, 7, 8, -42.333};
    // std::vector<float> myFloats2 = {-6666.6, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<int> myInts = {555, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<int> myInts2 = {777, 42, 4242, 424242};
    const std::size_t Nx = myFloats.size();
    const std::size_t Nx2 = myInts.size();
    const std::size_t Nx3 = myInts2.size();

    std::cout << "JuleaEngineTest Writing ... " << std::endl;
    /** ADIOS class factory of IO class objects, DebugON is recommended */
    adios2::ADIOS adios(adios2::DebugON);

    /*** IO class object: settings and factory of Settings: Variables,
       * Parameters, Transports, and Execution: Engines */
    adios2::IO juleaIO = adios.DeclareIO("juleaIO");
    juleaIO.SetEngine("julea-kv");

    /** global array: name, { shape (total dimensions) }, { start (local) },
    * { count (local) }, all are constant dimensions */
    adios2::Variable<float> juleaFloats = juleaIO.DefineVariable<float>(
        "juleaFloats", {}, {}, {Nx}, adios2::ConstantDims);
    // adios2::Variable<float> juleaFloats2 = juleaIO.DefineVariable<float>(
        // "juleaFloats2", {}, {}, {Nx}, adios2::ConstantDims);
    adios2::Variable<int> juleaInts = juleaIO.DefineVariable<int>(
        "juleaInts", {}, {}, {Nx2}, adios2::ConstantDims);
    adios2::Variable<int> juleaInts2 = juleaIO.DefineVariable<int>(
        "juleaInts2", {}, {}, {Nx3}, adios2::ConstantDims);

    /** Engine derived class, spawned to start IO operations */
    adios2::Engine juleaWriter = juleaIO.Open("testFile", adios2::Mode::Write );

    /** Write variable for buffering */
    juleaWriter.Put<float>(juleaFloats, myFloats.data(),adios2::Mode::Sync);
    // juleaWriter.Put<float>(juleaFloats2, myFloats2.data(),adios2::Mode::Sync);
    juleaWriter.Put<int>(juleaInts, myInts.data(),adios2::Mode::Sync);
    juleaWriter.Put<int>(juleaInts2, myInts2.data(),adios2::Mode::Sync);

    /** Create bp file, engine becomes unreachable after this*/
    juleaWriter.Close();

    return 0;
}


int read_test(){
    /** Application variable */
    std::vector<float> myFloats = {-42, -42, -42, -42, -42, -42, -42, -42, -42, -42};
    // std::vector<float> myFloats2 = {-42, -42, -42, -42, -42, -42, -42, -42, -42, -42};
    std::vector<int> myInts = {-42, -42, -42, -42, -42, -42, -42, -42, -42, -42};
    // std::vector<int> myInts2 = {-42, -42, -42, -42, -42, -42, -42, -42, -42, -42};
    const std::size_t Nx = myFloats.size();
    const std::size_t Nx2 = myInts.size();


    /** ADIOS class factory of IO class objects, DebugON is recommended */
    adios2::ADIOS adios(adios2::DebugON);

    /*** IO class object: settings and factory of Settings: Variables,
    * Parameters, Transports, and Execution: Engines */
    adios2::IO juleaIO = adios.DeclareIO("juleaIO");
    juleaIO.SetEngine("julea-kv");

    /** Engine derived class, spawned to start IO operations */
    adios2::Engine juleaReader = juleaIO.Open("testFile", adios2::Mode::Read );

    // for(int i = 0; i <10; i++)
    // {
    //     std::cout << "juleaFloats contains: " << myFloats[i] << std::endl;
    // }
    /** global array: name, { shape (total dimensions) }, { start (local) },
    * { count (local) }, all are constant dimensions */
    adios2::Variable<float> juleaFloats = juleaIO.InquireVariable<float>(
      "juleaFloats");
    // adios2::Variable<float> juleaFloats2 = juleaIO.InquireVariable<float>(
      // "juleaFloats2");
    adios2::Variable<int> juleaInts = juleaIO.InquireVariable<int>(
      "juleaInts");
    // adios2::Variable<int> juleaInts2 = juleaIO.InquireVariable<int>(
      // "juleaInts2");

    if(juleaFloats)
    {
        juleaReader.Get<float>(juleaFloats, myFloats.data(),adios2::Mode::Sync);
        // juleaReader.Get<float>(juleaFloats2, myFloats2.data(),adios2::Mode::Sync);
        juleaReader.Get<int>(juleaInts, myInts.data(),adios2::Mode::Sync);
        // juleaReader.Get<int>(juleaInts2, myInts2.data(),adios2::Mode::Sync);
    }

    std::cout << std::endl;
    for(int i = 0; i <10; i++)
    {
        std::cout << "juleaFloats contains now: " << myFloats[i] << std::endl;
        // std::cout << "juleaFloats2 contains now: " << myFloats2[i] << std::endl;
        std::cout << "juleaInts contains now: " << myInts[i] << std::endl;
        // std::cout << "juleaInts2 contains now: " << myInts2[i] << std::endl;
    }
    std::cout << std::endl;
    /** Create bp file, engine becomes unreachable after this*/
    juleaReader.Close();

    return 0;
}


int main(int argc, char *argv[])
{
    /** Application variable */
    std::vector<float> myFloats = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    const std::size_t Nx = myFloats.size();
    int err = -1;

    try
    {
		std::cout << "JuleaEngineTest :)" << std::endl;
		err = write_test();
        std::cout << "\n JuleaEngineTest :) Write finished" << std::endl;
        err = read_test();
        std::cout << "\n JuleaEngineTest :) Read finished" << std::endl;

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
