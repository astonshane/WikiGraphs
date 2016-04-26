x86: main.cpp
	mpic++ -I. -std=c++0x -Wall -O3 main.cpp -o a.out

ibm: main.cpp
	mpi++ -I. -O3 main.c -o project.xl
