#include "peer/config.hpp"
#include "peer/peer_node.hpp"
#include <exception>
#include <iostream>
int main(int argc,char** argv){
   try { p2place::peer::PeerNode{p2place::peer::Config::parse_args(argc,argv)}.run(); return 0; }
   catch(const std::exception& e){std::cerr<<"p2place-peer: "<<e.what()<<'\n';return 1;}
}
