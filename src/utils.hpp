#ifndef UTILS_HPP
#define UTILS_HPP


#include <chrono>
#include <string>
#include <sstream>
#include <iomanip>


inline std::string now_iso() {
using namespace std::chrono;
auto t = system_clock::now();
auto tt = system_clock::to_time_t(t);
std::tm tm = *std::localtime(&tt);
auto ms_part = duration_cast<milliseconds>(t.time_since_epoch()).count() % 1000;
std::ostringstream oss;
oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(3) << std::setfill('0') << ms_part;
return oss.str();
}


#endif // UTILS_HPP