PAPYRUS		= $(HOME)/work/papyrus/install
MPICXX		= mpic++
CXX_FLAGS	= -O2 -I$(PAPYRUS)/include
LD_FLAGS	= -L$(PAPYRUS)/lib -lpapyruskv

APP:basic workload cr

basic:basic.cpp
	$(MPICXX) $(CXX_FLAGS) -o $@ $^ $(LD_FLAGS)

workload:workload.cpp
	$(MPICXX) $(CXX_FLAGS) -o $@ $^ $(LD_FLAGS)

cr:cr.cpp
	$(MPICXX) $(CXX_FLAGS) -o $@ $^ $(LD_FLAGS)

clean:
	rm -f basic workload cr

