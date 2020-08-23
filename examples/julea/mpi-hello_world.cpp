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

void TestWriteVariableSync(int rank)
{
    /** Application variable */
    std::vector<int> myInts = {rank, 42, 0, 1, 2, 3};
    const std::size_t Nx = myInts.size();

    std::cout << "JuleaEngineTest Writing ... " << std::endl;
    adios2::ADIOS adios(adios2::DebugON);

    adios2::IO juleaIO = adios.DeclareIO("juleaIO");
    juleaIO.SetEngine("bp3");

    adios2::Variable<int> juleaInts = juleaIO.DefineVariable<int>(
        "juleaInts", {}, {rank*Nx}, {Nx}, adios2::ConstantDims);
    
    adios2::Engine juleaWriter = juleaIO.Open("testFile", adios2::Mode::Write);

    juleaWriter.Put<int>(juleaInts, myInts.data(), adios2::Mode::Sync);

    /** Create bp file, engine becomes unreachable after this*/
    juleaWriter.Close();
}

int main(int argc, char *argv[])
{

    int rank = 0;
#if ADIOS2_USE_MPI
    int nproc = 1;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);
    std::cout << "nproc: " << nproc << std::endl;
#endif
#if ADIOS2_USE_MPI
    adios2::ADIOS adios(MPI_COMM_WORLD);
#else
    adios2::ADIOS adios;
#endif

    try
    {
        std::cout << "JuleaEngineTest :)" << std::endl;
        TestWriteVariableSync(rank);
        std::cout << "\n JuleaEngineTest :) Write variable finished \n"
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

#if ADIOS2_USE_MPI
    MPI_Finalize();
#endif

    return 0;
}
