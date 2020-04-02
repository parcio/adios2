/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * Write local arrays from multiple processors.
 *
 * If one cannot or does not want to organize arrays present on each process
 * as one global array, still one can write them out with the same name.
 * Reading, however, needs to be handled differently: each process' array has
 * to be read separately, using Writeblock selections. The size of each process
 * block should be discovered by the reading application by inquiring per-block
 * size information of the variable, and allocate memory for reading
 * accordingly.
 *
 * In this example we write v0, v1, v2 and v3, in 5 output steps, where
 * v0 has the same size on every process at every step
 * v1 has different size on each process but fixed over time
 * v2 has different size on each process and that is changing over time
 * v3 is like v2 but also the number of processes writing it changes over time
 *
 * bpls can show the size of each block of the variable:
 *
 * $ cd <adios build directory>
 * $ make
 * $ mpirun -n 4 ./bin/localArray
 * $ bpls -l localArray.bp
 * double   v0    5*[4]*{6} = 0 / 3.4
 * double   v1    5*[4]*{__} = 0 / 3.4
 * double   v2    5*[4]*{__} = 0 / 3.4
 * double   v3    5*[__]*{__} = 0 / 3.3
 *
 * Study the decomposition of each variable
 * $ bpls -l localArray.bp -D v0
 * and notice the progression in the changes.
 *
 * Created on: Jun 2, 2017
 *      Author: pnorbert
 */

#include <iostream>
#include <vector>

#include <adios2.h>
// #ifdef ADIOS2_HAVE_MPI
// #include <mpi.h>
// #endif

void write(void)
{

    // v0 has the same size on every process at every step
    const size_t Nglobal = 2;
    std::vector<double> v0(Nglobal);
    std::vector<double> v1(Nglobal);
    std::vector<double> v2(Nglobal);
    std::vector<double> v3(Nglobal);
    std::vector<double> v4(Nglobal);
    std::vector<double> v5(Nglobal);

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    // io.SetEngine("julea-kv");
    io.SetEngine("bp3");
    /*
     * Define local array: type, name, local size
     * Global dimension and starting offset must be an empty vector
     * Here the size of the local array is the same on every process
     */
    adios2::Variable<double> varV0 =
        io.DefineVariable<double>("v0", {}, {}, {Nglobal});
    adios2::Variable<double> varV1 =
        io.DefineVariable<double>("v1", {}, {}, {Nglobal});
    adios2::Variable<double> varV2 =
        io.DefineVariable<double>("v2", {}, {}, {Nglobal});
    adios2::Variable<double> varV4 =
        io.DefineVariable<double>("v4", {}, {}, {Nglobal});
    adios2::Variable<double> varV5 =
        io.DefineVariable<double>("v5", {}, {}, {Nglobal});

    // Open file. "w" means we overwrite any existing file on disk,
    // but Advance() will append steps to the same file.
    adios2::Engine writer =
        io.Open("JULEA-SimpleSteps.bp", adios2::Mode::Write);
    // adios2::Engine writer = io.Open("JULEAlocalArray.bp",
    // adios2::Mode::Append);


    v4[0] = -666;
    v4[1] = 666;

    v5[0] = -123;
    v5[1] = 123;
    writer.Put<double>(varV4, v4.data());
    writer.Put<double>(varV5, v5.data(), adios2::Mode::Sync);

    for (int step = 0; step < 3; step++)
    {
        std::cout
            << "\n-------------------------------------------------------------"
            << std::endl;
        std::cout << "---------- Application: for loop [" << step
                  << "]-------------------------" << std::endl;
        std::cout
            << "-------------------------------------------------------------\n"
            << std::endl;

        writer.BeginStep();

        // v0
        for (size_t i = 0; i < Nglobal; i++)
        {
            // v0[i] = rank * 1.0 + step * 0.1;
            // v0[i] = 42.0 + step * 0.1;
            v0[i] = 11 * (step + 1) + step * 0.1 + i * 100;
            v1[i] = 11 * (step + 1) + step * 0.1 + i * 100;
            v2[i] = 22 * (step + 1) + step * 0.1 + i * 100;
            v3[i] = 42;
            // v3[i] = 33 * (step + 1) + step * 0.1 + i * 100;
            std::cout << "v0[" << i << "]: " << v0[i] << std::endl;
            std::cout << "v1[" << i << "]: " << v1[i] << std::endl;
            std::cout << "v2[" << i << "]: " << v2[i] << std::endl;
            std::cout << "v3[" << i << "]: " << v3[i] << std::endl;
            std::cout << "v4[" << i << "]: " << v4[i] << std::endl;
            std::cout << "v5[" << i << "]: " << v5[i] << std::endl;
        }
        writer.Put<double>(varV0, v0.data());
        writer.Put<double>(varV2, v3.data()); //test what happens when writing v2 more than once
        writer.Put<double>(varV2, v2.data());
        writer.Put<double>(varV5, v3.data());


        if(step == 2)
        {

        writer.Put<double>(varV1, v1.data());
        writer.Put<double>(varV4, v1.data());
        writer.Put<double>(varV5, v1.data());
        }

        std::cout << "\n---------- Application: EndStep "
                     "-------------------------------------\n"
                  << std::endl;
        writer.EndStep();
    }

    std::cout << "\n---------- Application: left for loop "
                 "-------------------------------------\n"
              << std::endl;
    // io.FlushAll();
    writer.Close();
}

void read(void)
{

    // v0 has the same size on every process at every step
    const size_t Nglobal = 2;
    std::vector<double> v0(Nglobal);
    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Input");
    io.SetEngine("julea-kv");
    // io.SetEngine("bp3");


    // Open file. "w" means we overwrite any existing file on disk,
    // but Advance() will append steps to the same file.
    adios2::Engine reader =
        io.Open("JULEA-SimpleSteps.bp", adios2::Mode::Read);
    // adios2::Engine writer = io.Open("JULEAlocalArray.bp",
    // adios2::Mode::Append);

    adios2::Variable<double> varV0 = io.InquireVariable<double>("v0");

    size_t steps = varV0.Steps();

    std::cout << "SIMPLE_STEPS: steps: " << steps << std::endl;

    size_t stepsstart = varV0.StepsStart();

    std::cout << "stepsstart: " << stepsstart << std::endl;

    for (int step = 0; step < 2; step++)
    {
        double value[2];

        reader.BeginStep();
        reader.Get<double>(varV0, value, adios2::Mode::Deferred);
        reader.EndStep();

        std::cout << "step: " << step << std::endl << "v[0]: " << value[0]
                  << std::endl << "v[1]: " << value[1] << std::endl;
    }

    varV0.SetStepSelection(adios2::Box<std::size_t>(0, 3));

    double value[6];
    reader.Get<double>(varV0, value, adios2::Mode::Sync);

    for (size_t i = 0; i < Nglobal; i++)
    {
        std::cout << "v[" << i << "]: " << value[i] << std::endl;
    }

    std::cout << "\n---------- Application: left for loop "
                 "-------------------------------------\n"
              << std::endl;
    // io.FlushAll();
    reader.Close();
}

int main(int argc, char *argv[])
{
    int rank = 0;
    std::cout << "... SimpleStepTest ... " << std::endl;
    std::cout << "... Only one process ... " << std::endl;

    try
    {
        write();
        // read();
    }
    catch (std::invalid_argument &e)
    {
        if (rank == 0)
        {
            std::cout << "Invalid argument exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }
    catch (std::ios_base::failure &e)
    {
        if (rank == 0)
        {
            std::cout << "System exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }
    catch (std::exception &e)
    {
        if (rank == 0)
        {
            std::cout << "Exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }
    }

    // #ifdef ADIOS2_HAVE_MPI
    //     MPI_Finalize();
    // #endif

    return 0;
}
