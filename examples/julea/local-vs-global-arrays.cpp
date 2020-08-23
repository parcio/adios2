/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * An application to write a simple bp file with steps and blocks which can then
 * be inspected using bpls.
 *
 * Created on: April 30, 2020
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */
#include <iostream>
#include <vector>

#include <adios2.h>


void writeSimpleLocalMINIMAL(std::string engine, std::string fileName,
                 bool printVectorContent, size_t vectorSize, size_t numberSteps)
{
    const size_t Nglobal = 4;

    std::vector<double> v1(Nglobal);
    std::vector<double> v2(Nglobal);
    std::vector<double> v3(Nglobal);
    std::vector<double> v4(Nglobal);

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine("bp4");

    adios2::Variable<double> varV0 =
        io.DefineVariable<double>("v0", {}, {}, {Nglobal});

    adios2::Engine writer = io.Open(fileName, adios2::Mode::Write);

    for (size_t i = 0; i < Nglobal; i++)
    {
        v1[i] = 11 + 1 * 0.1 + i * 100;
        v2[i] = 22 + 2 * 0.1 + i * 100;
        v3[i] = 33 + 3 * 0.1 + i * 100;
        v4[i] = 44 + 4 * 0.1 + i * 100;
    }

    for (int i = 0; i < numberSteps; i++)
    {
        writer.BeginStep();

        writer.Put<double>(varV0, v1.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v2.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v3.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v4.data(), adios2::Mode::Sync);

        writer.EndStep();
    }
    writer.Close();
}



void writeSimpleLocal(std::string engine, std::string fileName,
                 bool printVectorContent, size_t vectorSize, size_t numberSteps)
{
    const size_t Nglobal = vectorSize;

    std::vector<double> v1(Nglobal);
    std::vector<double> v2(Nglobal);
    std::vector<double> v3(Nglobal);
    std::vector<double> v4(Nglobal);


    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    adios2::Variable<double> varV0 =
        io.DefineVariable<double>("v0", {}, {}, {Nglobal});

    adios2::Engine writer = io.Open(fileName, adios2::Mode::Write);

    for (size_t i = 0; i < Nglobal; i++)
    {
        v1[i] = 11 + 1 * 0.1 + i * 100;
        v2[i] = 22 + 2 * 0.1 + i * 100;
        v3[i] = 33 + 3 * 0.1 + i * 100;
        v4[i] = 44 + 4 * 0.1 + i * 100;
    }

    for (int i = 0; i < numberSteps; i++)
    {
        writer.BeginStep();

        writer.Put<double>(varV0, v1.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v2.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v3.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v4.data(), adios2::Mode::Sync);

        writer.EndStep();
    }
    writer.Close();
}


void writeSimpleGlobal(std::string engine, std::string fileName,
                 bool printVectorContent, size_t vectorSize, size_t numberSteps)
{
    const size_t Nglobal = vectorSize;

    std::vector<double> v1(Nglobal);
    std::vector<double> v2(Nglobal);
    std::vector<double> v3(Nglobal);
    std::vector<double> v4(Nglobal);


    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engine);

    adios2::Variable<double> varV0 =
        io.DefineVariable<double>("v0", {Nglobal}, {0}, {Nglobal});

    adios2::Engine writer = io.Open(fileName, adios2::Mode::Write);

    for (size_t i = 0; i < Nglobal; i++)
    {
        v1[i] = 11 + 1 * 0.1 + i * 100;
        v2[i] = 22 + 2 * 0.1 + i * 100;
        v3[i] = 33 + 3 * 0.1 + i * 100;
        v4[i] = 44 + 4 * 0.1 + i * 100;
    }

    for (int i = 0; i < numberSteps; i++)
    {
        writer.BeginStep();

        writer.Put<double>(varV0, v1.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v2.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v3.data(), adios2::Mode::Sync);
        writer.Put<double>(varV0, v4.data(), adios2::Mode::Sync);

        writer.EndStep();
    }
    writer.Close();
}

int main(int argc, char *argv[])
{
    int rank = 0;

    try
    {
        // engine, fileName, printOutput, vectorSize, numberSteps
        writeSimpleLocal("bp4", "LocalArray.bp", true, 4, 3);
        writeSimpleGlobal("bp4", "GlobalArray.bp", true, 4, 3);
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
    return 0;
}
