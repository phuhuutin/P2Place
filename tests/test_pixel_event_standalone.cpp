#include "peer/pixel_event.hpp"
#include "messages/message.hpp"
#include <stdexcept>
#include <iostream>
#include <array>
using namespace p2place;
void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
int main(){
 try {
  peer::PixelEvent e{2,42,0x1234567812345678ULL,163,127,{0xAA55CC,1000,2}};
  auto b=peer::encode_pixel_event(e);
  check(b.size()==42,"unexpected wire size");
  auto decoded=peer::decode_pixel_event(b);
  check(decoded.origin==2 && decoded.sequence==42 && decoded.placement_fingerprint==e.placement_fingerprint,"header roundtrip");
  check(decoded.key==163 && decoded.index==127 && decoded.pixel.color==0xAA55CC && decoded.pixel.lamport==1000 && decoded.pixel.node==2,"pixel roundtrip");
  messages::Message m; m.type=messages::Type::PIXEL_EVENT;m.payload=b;
  auto wire=messages::encode(m);auto dm=messages::decode(wire);
  check(dm.type==messages::Type::PIXEL_EVENT,"message envelope rejects PIXEL_EVENT");
  peer::PixelEventDedup cache(2);
  check(cache.accept(2,42),"first accepted");
  check(!cache.accept(2,42),"duplicate rejected");
  check(cache.accept(2,43),"next accepted");
  check(cache.accept(3,42),"other origin accepted");
  check(cache.accept(2,42),"old event evicted after bounded cache");
  bool threw=false;try{b.pop_back();(void)peer::decode_pixel_event(b);}catch(const std::invalid_argument&){threw=true;}
  check(threw,"truncated event rejected");
  std::cout<<"PASS pixel-event wire codec, dedup, malformed messages\n";
 }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}
}
