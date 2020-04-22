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

// #include <algorithm> //std::for_each
// #include <array>
// #include <chrono>
// #include <ios>       //std::ios_base::failure
#include <iostream> //std::cout
// #include <stdexcept> //std::invalid_argument std::exception
// #include <string>
// #include <thread>
#include <vector>

#include <adios2.h>
// #ifdef ADIOS2_HAVE_MPI
// #include <mpi.h>
// #endif

std::string DimsToString(const adios2::Dims &dims)
{
    std::string s = "\"";
    for (int i = 0; i < dims.size(); i++)
    {
        if (i > 0)
        {
            s += ", ";
        }
        s += std::to_string(dims[i]);
    }
    s += "\"";
    return s;
}

void write_simple(std::string engine, std::string fileName)
{
    const size_t Nglobal = 2;
    std::vector<double> v0(Nglobal);
    std::vector<double> v1(Nglobal);
    std::vector<double> v2(Nglobal);
    std::vector<double> v3(Nglobal);
    std::vector<double> v4(Nglobal);
    std::vector<double> v5(Nglobal);
    std::vector<double> v6(Nglobal);

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    adios2::Variable<double> varV0 =
        io.DefineVariable<double>("v0", {}, {}, {Nglobal});
    adios2::Variable<double> varV1 =
        io.DefineVariable<double>("v1", {}, {}, {Nglobal});

    // Open file. "w" means we overwrite any existing file on disk,
    // but Advance() will append steps to the same file.
    adios2::Engine writer = io.Open(fileName, adios2::Mode::Write);
    // adios2::Engine writer = io.Open("SimpleSteps-APPEND-MODE",
    // adios2::Mode::Append); io.Open("JULEA-SimpleSteps.bp",
    // adios2::Mode::Write); adios2::Engine writer =
    // io.Open("JULEAlocalArray.bp", adios2::Mode::Append);

    // v1[0] = -42;
    // v1[1] = 42;
    // v2[0] = -222;
    // v2[1] = 222;
    v3[0] = -333;
    v3[1] = 333;
    v4[0] = -444;
    v4[1] = -444;
    // v5[0] = -555;
    // v5[1] = 555;
    v6[0] = -666;
    v6[1] = 666;

    // writer.Put<double>(varV0, v6.data());
    // writer.Put<double>(varV0, v2.data());

    // TODO: easier to test without these first
    writer.Put<double>(varV0, v3.data(), adios2::Mode::Sync);
    writer.Put<double>(varV0, v4.data(), adios2::Mode::Deferred);

    // writer.Put<double>(varV0, v1.data(), adios2::Mode::Sync);
    // writer.Put<double>(varV0, v5.data());

    for (int step = 0; step < 2; step++)
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
            // v1[i] = 22 * (step + 1) + step * 0.1 + i * 100;

            std::cout << "v0[" << i << "]: " << v0[i] << std::endl;
            std::cout << "v1[" << i << "]: " << v1[i] << std::endl;
        }
        writer.Put<double>(varV0, v6.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v0.data());
        // writer.Put<double>(varV0, v1.data(), adios2::Mode::Sync);

        if (step == 2)
        {
            // writer.Put<double>(varV1, v1.data());
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

void write_complex(std::string engine, std::string fileName)
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
    // io.SetEngine("bp3");
    io.SetEngine(engine);
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
    adios2::Engine writer = io.Open(fileName, adios2::Mode::Write);
    // io.Open("JULEA-SimpleSteps.bp", adios2::Mode::Write);
    // adios2::Engine writer = io.Open("JULEAlocalArray.bp",
    // adios2::Mode::Append);

    v4[0] = -666;
    v4[1] = 666;

    v5[0] = -123;
    v5[1] = 123;
    writer.Put<double>(varV4, v4.data());
    writer.Put<double>(varV5, v4.data());
    writer.Put<double>(varV5, v5.data(), adios2::Mode::Sync);
    writer.Put<double>(varV5, v4.data());

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
        writer.Put<double>(
            varV2,
            v3.data()); // test what happens when writing v2 more than once
        writer.Put<double>(varV2, v2.data());
        writer.Put<double>(varV5, v3.data());

        if (step == 2)
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

void read_simple(std::string engine, std::string fileName)
{
    std::cout << "\n---------- Application: Read "
                 "-------------------------------------\n"
              << std::endl;
    // v0 has the same size on every process at every step
    const size_t Nglobal = 2;
    std::vector<double> v0(Nglobal);
    std::vector<double> v1 = {-12345, -12345};
    std::vector<double> v2(Nglobal);
    std::vector<double> v3(Nglobal);
    v0.resize(2);
    v1.resize(2);
    v2.resize(2);
    v3.resize(2);

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Input");
    io.SetEngine(engine);

    // Open file. "w" means we overwrite any existing file on disk,
    // but Advance() will append steps to the same file.
    adios2::Engine reader = io.Open(fileName, adios2::Mode::Read);

    adios2::Variable<double> varV0 = io.InquireVariable<double>("v0");
    adios2::Variable<double> varV1 = io.InquireVariable<double>("v0");
    adios2::Variable<double> varV2 = io.InquireVariable<double>("v0");
    adios2::Variable<double> varV3 = io.InquireVariable<double>("v0");

    size_t steps = varV0.Steps();
    std::cout << "SIMPLE_STEPS: steps: " << steps << std::endl;

    size_t stepsstart = varV0.StepsStart();
    std::cout << "stepsstart: " << stepsstart << std::endl;

    for (int step = 0; step < 2; step++)
    {
        double value[2];

        reader.BeginStep();
        std::cout << "Step: " << step << std::endl;
        reader.Get<double>(varV0, v0.data(), adios2::Mode::Sync);
        // reader.Get<double>(varV0, v0.data(), adios2::Mode::Deferred);
        // reader.Get<double>(varV1, v1.data(), adios2::Mode::Sync);
        // reader.Get<double>(varV2, v2.data(), adios2::Mode::Sync);
        // reader.Get<double>(varV3, v3.data(), adios2::Mode::Sync);

        reader.EndStep();

        std::cout << "----- step ---- " << step << std::endl;
        // std::cout << "v[0]: " << value[0] << " v[1]: " << value[1] <<
        // std::endl;
        std::cout << "v0[0]: " << v0[0] << " v0[1]: " << v0[1] << std::endl;
        // std::cout << "v1[0]: " << v1[0] << " v1[1]: " << v1[1] << std::endl;
        // std::cout << "v2[0]: " << v2[0] << " v2[1]: " << v2[1] << std::endl;
        // std::cout << "v3[0]: " << v3[0] << " v3[1]: " << v3[1] << std::endl;
    }

    // varV0.SetStepSelection(adios2::Box<std::size_t>(0, 3));

    // double value[6];
    // reader.Get<double>(varV0, value, adios2::Mode::Sync);

    for (size_t i = 0; i < Nglobal; i++)
    {
        // std::cout << "v[" << i << "]: " << value[i] << std::endl;
    }

    std::cout << "\n---------- Application: left for loop "
                 "-------------------------------------\n"
              << std::endl;
    // io.FlushAll();
    reader.Close();
}

void read_selection(std::string engine, std::string fileName)
{
    std::cout << "\n---------- Application: Read "
                 "-------------------------------------\n"
              << std::endl;
    // v0 has the same size on every process at every step
    const size_t Nglobal = 2;
    std::vector<double> v0(Nglobal);
    std::vector<double> v1 = {-12345, -12345};

    v0.resize(2);
    v1.resize(2);

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Input");
    io.SetEngine(engine);

    adios2::Engine reader = io.Open(fileName, adios2::Mode::Read);

    adios2::Variable<double> varV0 = io.InquireVariable<double>("v0");
    adios2::Variable<double> varV1 = io.InquireVariable<double>("v0");

    size_t steps = varV0.Steps();
    std::cout << "\nSIMPLE_STEPS: steps: " << steps << std::endl;

    size_t stepsstart = varV0.StepsStart();
    std::cout << "stepsstart: " << stepsstart << std::endl;

    for (size_t step = 0; step < 2; step++)
    {
        reader.BeginStep(adios2::StepMode::Read);

        std::cout << "------ DEBUG -- " << std::endl;
        stepsstart = varV0.StepsStart();
        std::cout << "--- stepsstart: " << stepsstart << std::endl;

        auto blocksInfo = reader.BlocksInfo(varV0, step);

        std::cout << " v0 "
                  << " has " << blocksInfo.size() << " blocks in step " << step
                  << std::endl;

        // create a data vector for each block
        std::vector<std::vector<double>> dataSet;
        dataSet.resize(blocksInfo.size());

        // std::cout << "--DEBUG 2-- " << std::endl;
        /** to test stuff */
        // reader.Get<double>(varV1, v1.data(), adios2::Mode::Sync);
        // std::cout << "\nv1[0]: " << v1[0] << " v1[1]: " << v1[1] <<
        // std::endl;

        // schedule a read operation for each block separately
        int i = 0;
        for (auto &info : blocksInfo)
        {
            // std::cout << "------------------------ BLOCK LOOP: ------- i = "
            // << i << std::endl; std::cout << "test number blocksinfo: i= " <<
            // i << std::endl; std::cout << "info.BlockID = " << info.BlockID <<
            // std::endl; std::cout << "---- Application: SetBlockSelection ---
            // " << std::endl;
            varV0.SetBlockSelection(info.BlockID);
            // std::cout << "---- Application: Get --- " << std::endl;
            // reader.Get<double>(varV0, dataSet[i], adios2::Mode::Sync);
            reader.Get<double>(varV0, dataSet[i], adios2::Mode::Deferred);
            ++i;
        }

        // Read in all blocks at once now
        reader.PerformGets();
        // data vectors now are filled with data

        /** to test stuff */
        // reader.Get<double>(varV1, v1.data(), adios2::Mode::Sync);
        // std::cout << "\n v1[0]: " << v1[0] << " v1[1]: " << v1[1] <<
        // std::endl;

        std::cout
            << "\n--------------- APPLICATION loop over blocksInfo ------- "
            << std::endl;
        i = 0;
        for (const auto &info : blocksInfo)
        {
            std::cout << "        block " << info.BlockID
                      << " size = " << DimsToString(info.Count)
                      << " offset = " << DimsToString(info.Start) << " : ";

            for (const auto datum : dataSet[i])
            {
                std::cout << datum << " ";
            }
            std::cout << std::endl;
            ++i;
        }

        reader.EndStep();
    }

    // varV0.SetStepSelection(adios2::Box<std::size_t>(0, 3));

    // double value[6];
    // reader.Get<double>(varV0, value, adios2::Mode::Sync);

    for (size_t i = 0; i < Nglobal; i++)
    {
        // std::cout << "v[" << i << "]: " << value[i] << std::endl;
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
        // write_complex("julea-kv", "SimpleSteps.jv");
        // write_complex("bp3", "SimpleSteps.bp");
        write_simple("bp3", "SimpleSteps.bp");
        write_simple("julea-kv", "SimpleSteps.jv");
        // write_simple("hdf5", "SimpleSteps.h5");
        // write("julea-kv", "SimpleSteps.bp");
        // write();
        // read_simple("bp3", "SimpleSteps.bp");
        // read_simple("julea-kv", "SimpleSteps.jv");
        read_selection("bp3", "SimpleSteps.bp");
        read_selection("julea-kv", "SimpleSteps.jv");
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
