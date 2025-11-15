CXX = g++
CXXFLAGS = -std=c++17 -O2 -pthread -Wall -Wextra
SRC = src
BUILD = build

all: dirs $(BUILD)/static_partition

dirs:
	mkdir -p $(BUILD)
	mkdir -p data/logs

$(BUILD)/static_partition: $(SRC)/static_partition.cpp $(SRC)/common.hpp $(SRC)/utils.hpp
	$(CXX) $(CXXFLAGS) $(SRC)/static_partition.cpp -o $(BUILD)/static_partition

clean:
	rm -rf $(BUILD)/* data/logs/*

.PHONY: all dirs clean