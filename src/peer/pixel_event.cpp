#include "peer/pixel_event.hpp"
#include "messages/payloads.hpp"
#include "core/region_store.hpp"
#include <stdexcept>
namespace p2place::peer {
namespace {
void u32(std::vector<std::uint8_t>& v,std::uint32_t x){for(int i=3;i>=0;--i)v.push_back(static_cast<std::uint8_t>(x>>(i*8)));}
void u64(std::vector<std::uint8_t>& v,std::uint64_t x){for(int i=7;i>=0;--i)v.push_back(static_cast<std::uint8_t>(x>>(i*8)));}
std::uint32_t r32(std::span<const std::uint8_t> b,std::size_t n){std::uint32_t x=0;for(int i=0;i<4;++i)x=(x<<8)|b[n+i];return x;}
std::uint64_t r64(std::span<const std::uint8_t> b,std::size_t n){std::uint64_t x=0;for(int i=0;i<8;++i)x=(x<<8)|b[n+i];return x;}
}
std::vector<std::uint8_t> encode_pixel_event(const PixelEvent& e) {
 if(!e.origin || !e.sequence || e.key>=core::RegionStore::REGION_COUNT || e.index>=core::Region::COUNT)
  throw std::invalid_argument("invalid pixel notification");
 auto body=messages::encode_put({e.key,e.index,e.pixel});
 std::vector<std::uint8_t> out;out.reserve(20+body.size());
 u32(out,e.origin);u64(out,e.sequence);u64(out,e.placement_fingerprint);
 out.insert(out.end(),body.begin(),body.end());return out;
}
PixelEvent decode_pixel_event(std::span<const std::uint8_t> bytes) {
 // Put payload is fixed-width. Reject malformed and oversized messages.
 if(bytes.size()!=20+22)throw std::invalid_argument("invalid pixel event length");
 PixelEvent e;e.origin=r32(bytes,0);e.sequence=r64(bytes,4);e.placement_fingerprint=r64(bytes,12);
 const auto put=messages::decode_put(bytes.subspan(20));
 e.key=put.key;e.index=put.pixel_index;e.pixel=put.pixel;
 if(!e.origin || !e.sequence || e.key>=core::RegionStore::REGION_COUNT || e.index>=core::Region::COUNT)
  throw std::invalid_argument("invalid pixel event fields");
 return e;
}
bool PixelEventDedup::accept(std::uint32_t origin,std::uint64_t sequence) {
 if(!origin || !sequence || !limit_)return false;
 const auto id=std::make_pair(origin,sequence);
 if(!seen_.insert(id).second)return false;
 order_.push_back(id);
 if(order_.size()>limit_) {seen_.erase(order_.front());order_.pop_front();}
 return true;
}
} // namespace p2place::peer
