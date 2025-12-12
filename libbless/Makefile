CUDA_HOME ?= /usr/local/cuda
CXXFLAGS  := -fPIC -O2 -std=c++17 -I$(CUDA_HOME)/include
LDFLAGS   := -shared -L$(CUDA_HOME)/lib64 -lcuda -lcudart -ldl -lpthread


libbless.so: libbless.cpp context_manager.hpp routing.hpp
	$(CXX) $(CXXFLAGS) -o $@ libbless.cpp $(LDFLAGS)

clean:
	rm -f libbless.so 