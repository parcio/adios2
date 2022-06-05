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

void TestWriteVariableSync()
{
    /** Application variable */
    std::vector<float> myFloats = {12345.6, 1, 2, 3, 4, 5, 6, 7, 8, -42.3456};
    // std::vector<float> myFloats2 = {-6666.6, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<int> myInts = {333, 1, 2, 3, 4, 5, 6, 7, 8, 9};
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

    // juleaIO.SetEngine("julea-kv");
    juleaIO.SetEngine("julea-db-dai");
    // juleaIO.SetEngine("julea-db");
    // juleaIO.SetEngine("bp3");

    /** global array: name, { shape (total dimensions) }, { start (local) },
     * { count (local) }, all are constant dimensions */
    adios2::Variable<float> juleaFloats = juleaIO.DefineVariable<float>(
        "juleaFloats", {Nx}, {0}, {Nx}, adios2::ConstantDims);
        
    // adios2::Variable<float> juleaFloats2 = juleaIO.DefineVariable<float>(
    // "juleaFloats2", {}, {}, {Nx}, adios2::ConstantDims);
    adios2::Variable<int> juleaInts = juleaIO.DefineVariable<int>(
        "juleaInts", {Nx2}, {0}, {Nx2}, adios2::ConstantDims);
    adios2::Variable<int> juleaInts2 = juleaIO.DefineVariable<int>(
        "juleaInts2", {Nx3}, {0}, {Nx3}, adios2::ConstantDims);

    /** Engine derived class, spawned to start IO operations */
    adios2::Engine juleaWriter = juleaIO.Open("testFile.jb", adios2::Mode::Write);
    // adios2::Engine juleaWriter = juleaIO.Open("testFile.jv", adios2::Mode::Write);

    /** Write variable for buffering */
    juleaWriter.Put<float>(juleaFloats, myFloats.data(), adios2::Mode::Deferred);
    // juleaWriter.Put<float>(juleaFloats,
    // myFloats.data(),adios2::Mode::Deferred);
    // juleaWriter.Put<float>(juleaFloats2,
    // myFloats2.data(),adios2::Mode::Sync);
    juleaWriter.Put<int>(juleaInts, myInts.data(), adios2::Mode::Deferred);
    // juleaWriter.Put<int>(juleaInts, myInts.data(),adios2::Mode::Deferred);
    juleaWriter.Put<int>(juleaInts2, myInts2.data(), adios2::Mode::Deferred);
    // juleaWriter.Put<int>(juleaInts2, myInts2.data(),adios2::Mode::Deferred);

    /** Create bp file, engine becomes unreachable after this*/
    juleaWriter.Close();
}

void TestReadVariableSync()
{
    /** Application variable */
    std::vector<float> myFloats = {112, -42, -42, -42, -42,
                                   -42, -42, -42, -42, -42};
    // std::vector<float> myFloats2 = {-42, -42, -42, -42, -42, -42, -42, -42,
    // -42, -42};
    std::vector<int> myInts = {113, -42, -42, -42, -42,
                               -42, -42, -42, -42, -42};
    std::vector<int> myInts2 = {-42, -42, -42, -42, -42, -42, -42, -42, -42,
    -42};
    const std::size_t Nx = myFloats.size();
    const std::size_t Nx2 = myInts.size();

    /** ADIOS class factory of IO class objects, DebugON is recommended */
    adios2::ADIOS adios(adios2::DebugON);

    /*** IO class object: settings and factory of Settings: Variables,
     * Parameters, Transports, and Execution: Engines */
    adios2::IO juleaIO = adios.DeclareIO("juleaIO");

    // juleaIO.SetEngine("julea-kv");
    juleaIO.SetEngine("julea-db-dai");
    // juleaIO.SetEngine("bp3");

    /** Engine derived class, spawned to start IO operations */
    adios2::Engine juleaReader = juleaIO.Open("testFile.jb", adios2::Mode::Read);
    // adios2::Engine juleaReader = juleaIO.Open("testFile.jv", adios2::Mode::Read);

    // for(int i = 0; i <10; i++)
    // {
    //     std::cout << "juleaFloats contains: " << myFloats[i] << std::endl;
    // }
    /** global array: name, { shape (total dimensions) }, { start (local) },
     * { count (local) }, all are constant dimensions */
    adios2::Variable<float> juleaFloats =
        juleaIO.InquireVariable<float>("juleaFloats");
    std::cout << "juleaFloats: " << juleaFloats << std::endl;
    
    // adios2::Variable<float> juleaFloats2 = juleaIO.InquireVariable<float>(
    // "juleaFloats2");
    adios2::Variable<int> juleaInts = juleaIO.InquireVariable<int>("juleaInts");
    adios2::Variable<int> juleaInts2 = juleaIO.InquireVariable<int>(
    "juleaInts2");

    if (juleaFloats)
    {
        std::cout << "Right before reading" << std::endl;
        juleaReader.Get<float>(juleaFloats, myFloats.data(),
                               adios2::Mode::Sync);
        // juleaReader.Get<float>(juleaFloats2,
        // myFloats2.data(),adios2::Mode::Sync);
        juleaReader.Get<int>(juleaInts, myInts.data(), adios2::Mode::Deferred);
        // std::cout << "Data : " << juleaFloats.GetData() << std::endl;
        juleaReader.Get<int>(juleaInts2, myInts2.data(),adios2::Mode::Deferred);

        juleaReader.PerformGets();
    }

    // std::cout << "Data: " << juleaFloats.Name() << std::endl;
    std::cout
        << "\n__________ Test application: Read variable __________________"
        << std::endl;

    for (int i = 0; i < 10; i++)
    {
        std::cout << "juleaFloats contains now: " << myFloats[i] << std::endl;
        // std::cout << "juleaFloats2 contains now: " << myFloats2[i] <<
        // std::endl;
    }
    std::cout << "\n____________________________" << std::endl;

    for (int i = 0; i < 10; i++)
    {
        std::cout << "juleaInts contains now: " << myInts[i] << std::endl;
    }
    std::cout << "\n____________________________" << std::endl;

    for (int i = 0; i < 10; i++)
    {
        std::cout << "juleaInts2 contains now: " << myInts2[i] << std::endl;
    }
    std::cout << std::endl;
    /** Create bp file, engine becomes unreachable after this*/
    juleaReader.Close();
}

void TestWriteAttribute()
{
    std::vector<float> myFloats = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    const std::size_t Nx = myFloats.size();
    /** ADIOS class factory of IO class objects, DebugON is recommended */
    adios2::ADIOS adios(adios2::DebugON);

    /*** IO class object: settings and factory of Settings: Variables,
     * Parameters, Transports, and Execution: Engines */
    adios2::IO juleaIO = adios.DeclareIO("juleaIO");
    juleaIO.SetEngine("julea-db");
    // juleaIO.SetEngine("julea-kv");

    /** global array: name, { shape (total dimensions) }, { start (local) },
     * { count (local) }, all are constant dimensions */
    adios2::Variable<float> bpFloats = juleaIO.DefineVariable<float>(
        "bpFloats", {}, {}, {Nx}, adios2::ConstantDims);

    juleaIO.DefineAttribute<std::string>(
        "Single_String",
        "File generated with ADIOS2 without any variable put operation");

    std::vector<std::string> myStrings = {"one", "two", "three"};
    juleaIO.DefineAttribute<std::string>("Array_of_Strings", myStrings.data(),
                                         myStrings.size());

    juleaIO.DefineAttribute<double>("Attr_Double", 42.42);
    std::vector<double> myDoubles = {-111, 42, -333, 4, 5, 6, 7, 8, 9, 10};
    juleaIO.DefineAttribute<double>("Array_of_Doubles", myDoubles.data(),
                                    myDoubles.size());

    /** Engine derived class, spawned to start IO operations */
    // adios2::Engine juleaWriter = juleaIO.Open("myVector.bp",
    // adios2::Mode::Write);
    adios2::Engine juleaWriter = juleaIO.Open("testFile", adios2::Mode::Write);

    /** Write variable for buffering */
    // juleaWriter.Put<float>(bpFloats, myFloats.data());

    /** Create bp file, engine becomes unreachable after this*/
    juleaWriter.Close();
}

void TestReadAttribute()
{
    /** ADIOS class factory of IO class objects, DebugON is recommended */
    adios2::ADIOS adios(adios2::DebugON);

    /*** IO class object: settings and factory of Settings: Variables,
     * Parameters, Transports, and Execution: Engines */
    adios2::IO juleaIO = adios.DeclareIO("juleaIO");
    // juleaIO.SetEngine("julea-kv");
    juleaIO.SetEngine("julea-db");

    // adios2::Engine juleaReader = juleaIO.Open("myVector.bp",
    // adios2::Mode::Read);
    adios2::Engine juleaReader = juleaIO.Open("testFile", adios2::Mode::Read);

    std::cout
        << "\n__________ Test application: Read attribute __________________"
        << std::endl;

    adios2::Attribute<std::string> juleaAttrSingleString =
        juleaIO.InquireAttribute<std::string>("Single_String");
    if (juleaAttrSingleString)
    {
        std::cout << "Name: " << juleaAttrSingleString.Name() << std::endl;
        std::cout << "-- Data: " << juleaAttrSingleString.Data()[0]
                  << std::endl;
        // std::cout << "-- Attribute string read " << std::endl;
    }

    adios2::Attribute<std::string> juleaAttrStringArray =
        juleaIO.InquireAttribute<std::string>("Array_of_Strings");
    if (juleaAttrSingleString)
    {
        std::cout << "Name: " << juleaAttrStringArray.Name() << std::endl;
        std::cout << "-- Data: " << juleaAttrStringArray.Data()[0] << std::endl;
        std::cout << "-- Data: " << juleaAttrStringArray.Data()[1] << std::endl;
        std::cout << "-- Data: " << juleaAttrStringArray.Data()[2] << std::endl;
        // std::cout << "-- Attribute string read " << std::endl;
    }

    // adios2::Attribute<double> juleaAttrDouble =
    // juleaIO.InquireAttribute<double>("Attr_Double");
    auto juleaAttrDouble = juleaIO.InquireAttribute<double>("Attr_Double");
    if (juleaAttrDouble)
    {
        std::cout << "Name: " << juleaAttrDouble.Name() << std::endl;
        std::cout << "-- Data: " << juleaAttrDouble.Data()[0] << std::endl;
        // std::cout << "-- Attribute double read " << std::endl;
    }

    auto juleaAttrDoubleArray =
        juleaIO.InquireAttribute<double>("Array_of_Doubles");
    if (juleaAttrDouble)
    {
        std::cout << "Name: " << juleaAttrDoubleArray.Name() << std::endl;
        std::cout << "-- Data: " << juleaAttrDoubleArray.Data()[0] << std::endl;
        std::cout << "-- Data: " << juleaAttrDoubleArray.Data()[1] << std::endl;
        std::cout << "-- Data: " << juleaAttrDoubleArray.Data()[2] << std::endl;
        // std::cout << "-- Attribute double read " << std::endl;
    }
}

void TestWriteVariableDeferred()
{
    /** Application variable */
    std::vector<float> myFloats = {666, -111, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<int> myInts = {111, -1, -2, -3, -4, -5, -6, -7, -8, -9};
    std::vector<int> myInts2 = {777, 42, 4242, 424242};

    const std::size_t Nx = myFloats.size();
    const std::size_t Nx2 = myInts.size();
    const std::size_t Nx3 = myInts2.size();

    std::cout << "JuleaEngineTest Writing deferred ... " << std::endl;

    adios2::ADIOS adios(adios2::DebugON);

    adios2::IO juleaIO = adios.DeclareIO("juleaIO");
    // juleaIO.SetEngine("julea-kv");
    juleaIO.SetEngine("julea-db");

    adios2::Variable<float> bpFloats = juleaIO.DefineVariable<float>(
        "bpFloats", {}, {}, {Nx}, adios2::ConstantDims);

    adios2::Variable<int> bpInts = juleaIO.DefineVariable<int>(
        "bpInts", {}, {}, {Nx}, adios2::ConstantDims);
    adios2::Variable<int> bpInts2 = juleaIO.DefineVariable<int>(
        "bpInts2", {}, {}, {Nx3}, adios2::ConstantDims);

    adios2::Engine juleaWriter = juleaIO.Open("testFile", adios2::Mode::Write);

    juleaWriter.Put(bpFloats, myFloats.data());
    juleaWriter.Put(bpInts, myInts.data());
    juleaWriter.Put(bpInts2, myInts2.data());

    juleaWriter.PerformPuts();

    juleaWriter.Close();
}

void TestReadVariableDeferred()
{
    /** Application variable */
    std::vector<float> myFloats = {-42, -42, -42, -42, -42,
                                   -42, -42, -42, -42, -42};
    std::vector<int> myInts = {-42, -42, -42, -42, -42,
                               -42, -42, -42, -42, -42};
    std::vector<int> myInts2 = {-42, -42, -42, -42};
    const std::size_t Nx = myFloats.size();
    const std::size_t Nx2 = myInts.size();
    const std::size_t Nx3 = myInts2.size();

    std::cout << "JuleaEngineTest Writing deferred ... " << std::endl;

    adios2::ADIOS adios(adios2::DebugON);

    adios2::IO juleaIO = adios.DeclareIO("juleaIO");
    // juleaIO.SetEngine("julea-kv");
    juleaIO.SetEngine("julea-db");

    adios2::Engine juleaReader = juleaIO.Open("testFile", adios2::Mode::Read);

    adios2::Variable<float> bpFloats =
        juleaIO.InquireVariable<float>("bpFloats");
    adios2::Variable<int> bpInts = juleaIO.InquireVariable<int>("bpInts");
    adios2::Variable<int> bpInts2 = juleaIO.InquireVariable<int>("bpInts2");

    if (bpFloats)
    {
        std::cout << "bpFloats: Inquire was successfull " << std::endl;
        juleaReader.Get<float>(bpFloats, myFloats.data(), adios2::Mode::Sync);
    }
    if (bpInts)
    {
        std::cout << "bpInts: Inquire was successfull " << std::endl;
        juleaReader.Get<int>(bpInts, myInts.data(), adios2::Mode::Sync);
    }
    if (bpInts2)
    {
        std::cout << "bpInts2: Inquire was successfull " << std::endl;
        juleaReader.Get<int>(bpInts2, myInts2.data(), adios2::Mode::Sync);
    }

    // juleaReader.PerformPuts();
    for (int i = 0; i < 10; i++)
    {
        std::cout << "bpFloats contains now: " << myFloats[i] << std::endl;
        std::cout << "bpInts contains now: " << myInts[i] << std::endl;
        // std::cout << "juleaInts2 contains now: " << myInts2[i] << std::endl;
    }

    juleaReader.Close();
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
        TestWriteVariableSync();
        std::cout << "\n JuleaEngineTest :) Write variable finished \n"
                  << std::endl;
        TestReadVariableSync();
        std::cout << "\n JuleaEngineTest :) Read variable finished \n"
                  << std::endl;
                
        // TestWriteAttribute();
        // std::cout << "\n JuleaEngineTest :) Write attribute finished \n"
                  // << std::endl;
        // TestReadAttribute();
        // std::cout << "\n JuleaEngineTest :) Read attribute finished \n"
                  // << std::endl;

        // TestWriteVariableDeferred();
        // std::cout
            // << "\n JuleaEngineTest :) Write variable asynchronous finished \n"
            // << std::endl;
        // TestReadVariableDeferred();
        // std::cout
            // << "\n JuleaEngineTest :) Read variable asynchronous finished \n"
            // << std::endl;
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
