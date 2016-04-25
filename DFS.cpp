#include <stdio.h>
#include <mpi.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <algorithm>
#include <vector>
using namespace std;

int M[7][7] = 	//adjacency matrix
	{
		{0,1,1,1,0,0,0},
		{1,0,1,0,0,0,0},
		{1,1,0,1,1,0,0},
		{1,0,1,0,1,0,0},
		{0,0,1,1,0,0,0},
		{0,0,0,0,0,0,1},
		{0,0,0,0,0,1,0}
	};
int seen[7];
vector<vector<int> > connectedComponents;
int n = 7;

void dfs(int u, int ccCounter) {
	seen[u] = 1;
	connectedComponents[ccCounter].push_back(u);
	for(int v=0; v<n; v++) {
		if(!seen[v] && M[u][v]) {
			dfs(v, ccCounter);
		}
	}
}

int main(int argc, char** argv) {
	int ccCounter = -1;
	for(int i=0; i<n; i++) {
		if(!seen[i]) {
			ccCounter += 1;
			vector<int> temp;
			connectedComponents.push_back(temp);
			dfs(i, ccCounter);	
		}
	}
	for(int i=0; i<connectedComponents.size(); i++){
		cout<<"CC "<<i<<" - ";
		for(int j=0; j<connectedComponents[i].size(); j++) {
			cout<<connectedComponents[i][j]+1<<", ";
		}
		cout<<endl;
	}
	return 0;
}
