#include "util/sha1.hpp"
#include "util/base64.hpp"
#include <bit>
#include <vector>
namespace p2place::util {
    std::array<std::uint8_t,20> sha1(std::string_view input) {
        std::vector<std::uint8_t> data(input.begin(),input.end());
        const std::uint64_t bits=static_cast<std::uint64_t>(data.size())*8;
        data.push_back(0x80);
        while((data.size()%64)!=56)data.push_back(0);
        for(int i=7;i>=0;--i)data.push_back(static_cast<std::uint8_t>(bits>>(8*i)));
        std::uint32_t h0=0x67452301,h1=0xefcdab89,h2=0x98badcfe,h3=0x10325476,h4=0xc3d2e1f0;
        for(std::size_t chunk=0;chunk<data.size();chunk+=64){
        std::array<std::uint32_t,80> w{};
        for(int i=0;i<16;++i){std::size_t pos=chunk+i*4;w[i]=(std::uint32_t(data[pos])<<24)|(std::uint32_t(data[pos+1])<<16)|(std::uint32_t(data[pos+2])<<8)|data[pos+3];}
        for(int i=16;i<80;++i)w[i]=std::rotl(w[i-3]^w[i-8]^w[i-14]^w[i-16],1);
        auto a=h0,b=h1,c=h2,d=h3,e=h4;
        for(int i=0;i<80;++i){
            std::uint32_t f,k;
            if(i<20){f=(b&c)|((~b)&d);k=0x5a827999;}
            else if(i<40){f=b^c^d;k=0x6ed9eba1;}
            else if(i<60){f=(b&c)|(b&d)|(c&d);k=0x8f1bbcdc;}
            else{f=b^c^d;k=0xca62c1d6;}
            auto temp=std::rotl(a,5)+f+e+k+w[i];e=d;d=c;c=std::rotl(b,30);b=a;a=temp;
        }
        h0+=a;h1+=b;h2+=c;h3+=d;h4+=e;
        }
        std::array<std::uint8_t,20> out{};int j=0;
        for(auto h:{h0,h1,h2,h3,h4})for(int i=3;i>=0;--i)out[j++]=static_cast<std::uint8_t>(h>>(8*i));
        return out;
    }
    std::string websocket_accept_key(std::string_view key){
    std::string combined(key);combined+="258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    auto digest=sha1(combined);return base64_encode(digest);
    }
}
