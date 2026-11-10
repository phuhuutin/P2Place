#pragma once
#include <cstdint>
#include <span>
#include <string>
namespace p2place::util { std::string base64_encode(std::span<const std::uint8_t> data); }
