/**
 * @file peer/main.cpp
 * @brief Entry point of the p2place-peer executable.
 */
#include "peer/config.hpp"
#include "peer/peer_node.hpp"
#include <exception>
#include <iostream>

/**
 * @brief Run one peer: parse arguments, construct PeerNode, run().
 *
 * Any startup exception is reported on stderr and the process exits
 * with status 1.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on clean shutdown, 1 if startup threw.
 */
int main(int argc,char** argv){
   try {
      p2place::peer::PeerNode{
         p2place::peer::Config::parse_args(argc,argv)
      }.run(); return 0;
   }
   catch(const std::exception& e){std::cerr<<"p2place-peer: "<<e.what()<<'\n';return 1;}
}
