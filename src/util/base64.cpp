#include "util/base64.hpp"
namespace p2place::util {
    std::string base64_encode(std::span<const std::uint8_t> data) {
        static constexpr char T[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string o;
        for(std::size_t i=0;i<data.size();i+=3){
        const unsigned a=data[i], b=i+1<data.size()?data[i+1]:0, c=i+2<data.size()?data[i+2]:0;
            o.push_back(T[a>>2]);o.push_back(T[((a&3)<<4)|(b>>4)]);
            o.push_back(i+1<data.size()?T[((b&15)<<2)|(c>>6)]:'=');
            o.push_back(i+2<data.size()?T[c&63]:'=');
        }return o;
    }
}
