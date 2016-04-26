x86: main.cpp
	mpic++ -I. -std=c++0x -Wall -O3 main.cpp -o a.out

ibm: main.cpp
	mpicxx -I. -qlanglvl=rightanglebracket -O3 main.cpp -o project.xl
