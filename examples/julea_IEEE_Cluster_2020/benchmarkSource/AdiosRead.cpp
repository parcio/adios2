/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * An application demonstrating some of the query possibilities enabled by the
 * JULEA database engine.
 *
 *  Created on: May 06, 2020
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */
#include <adios2.h>
#include <iomanip>
#include <iostream>
#include <vector>

// #include "AdiosQuery.h"


void AdiosReadMinMax(std::string fileName, std::string variableName)
{
    std::cout << "AdiosReadMinMax" << std::endl;
}

void AdiosRead(std::string engineName, std::string directory, size_t fileCount, uint32_t percentageVarsToRead)
{
    std::cout << "AdiosRead" << std::endl;

    //is directory? is file?
    //get all files
    //read first fileCount
    //read variables but only percentage
    // std::string fileName;
     std::string fileName = "sresa1b_ncar_ccsm3-example.bp";

    adios2::ADIOS adios(adios2::DebugON);
    adios2::IO io = adios.DeclareIO("Output");
    io.SetEngine(engineName);

    adios2::Engine reader = io.Open(fileName, adios2::Mode::Read);
    auto varMap = io.AvailableVariables();
    for (const auto &var : varMap)
    {
    	std::string name = var.first;
    	adios2::Params params = var.second;
    	std::cout << "name: " << name << std::endl;
    	auto type = io.VariableType(name);
    }

}