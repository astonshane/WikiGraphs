#include <stdio.h>
#include <mpi.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <algorithm>
#include <vector>
#include <list>
using namespace std;

int MTemp[2][7] = 	//adjacency matrix
	{
		// {0,1,1,1,0,0,0},
		// {1,0,1,0,0,0,0},
		{1,1,0,1,1,0,0},
		{1,0,1,0,1,0,0}//,
		// {0,0,1,1,0,0,0},
		// {0,0,0,0,0,0,1},
		// {0,0,0,0,0,1,0}
	};
vector<vector<int> > connectedComponents;

int vertices = 7;
int *visited = new int[vertices];
int minRankIndex = 2;
int maxRankIndex = 3;
int rows = maxRankIndex-minRankIndex+1;


void bfs(int u, int ccCounter) {
	list<int> queue;
	visited[u] = true;
	queue.push_back(u);
	while(!queue.empty()) {
		int s=queue.front();
		connectedComponents[ccCounter].push_back(s);
		queue.pop_front();
		for(int i=0;i<rows; i++) {
			if(MTemp[i][s] && !visited[i+minRankIndex]) {
				visited[i+minRankIndex]=1;
				queue.push_back(i+minRankIndex);
			}
		}
		for(int i=0;i<vertices;i++){
			if(s>=minRankIndex && s<=maxRankIndex && MTemp[s-minRankIndex][i] && !visited[i]) {
				visited[i]=1;
				queue.push_back(i);	
			}
		}
	}
	// for(int v=0; v<vertices; v++) {
	// 	cout<<"CC: "<<ccCounter<<" u: "<<u<<" v: "<<v<<" seen?: "<<seen[v]<<endl;
	// 	if(!seen[v] && u<split && MTemp[u][v]) {
	// 		dfs(v, ccCounter);
	// 	}
	// }
}

int main(int argc, char** argv) {
	int ccCounter = -1;
	for(int i=0; i<vertices; i++) {
	visited[i]=0;
	}
	for(int i=0; i<vertices; i++) {
		if(!visited[i]) {
			ccCounter += 1;
			vector<int> temp;
			connectedComponents.push_back(temp);
			bfs(i, ccCounter);	
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
