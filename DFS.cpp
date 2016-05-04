#include <stdio.h>
#include <mpi.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <algorithm>
#include <vector>
#include <list>

std::map<int, std::vector<int> > g_adj_list;
//sample testing adj_list
int foo1[] = {2,3,4};
int foo2[] = {1,3};
int foo3[] = {1,2,4,5};
int foo4[] = {1,3,5};
int foo5[] = {3,4};
int foo6[] = {7};
int foo7[] = {6};

std::vector<int> mySet1(foo1, foo1+3);
std::vector<int> mySet2(foo2, foo2+2);
std::vector<int> mySet3(foo3, foo3+4);
std::vector<int> mySet4(foo4, foo4+3);
std::vector<int> mySet5(foo5, foo5+2);
std::vector<int> mySet6(foo6, foo6+1);
std::vector<int> mySet7(foo7, foo7+1);

std::map<int, bool> visited;
std::vector<std::vector<int> > connectedComponents;
std::map<int, std::vector<int> > boundaryEdges;

void bfs(int u, int ccCounter) {
	std::list<int> queue;
	visited[u] = true;
	queue.push_back(u);
	int prev = u;
	while(!queue.empty()) {
		int s=queue.front();
		visited[s]=true;
		std::cout<<"Queue: ";
		connectedComponents[ccCounter].push_back(s);
		for(std::list<int>::iterator itr = queue.begin(); itr!=queue.end(); itr++) {
			std::cout<<*itr<<" ";
		}
		std::cout<<std::endl;
		queue.pop_front();
		std::map<int, std::vector<int> >::iterator adj_list_itr;
		adj_list_itr = g_adj_list.find(s);
		if(adj_list_itr == g_adj_list.end()) {
			std::map<int, std::vector<int> >::iterator it = boundaryEdges.find(prev);
			if(it != boundaryEdges.end()){
				it->second.push_back(s);
			}
			else{
				std::vector<int> temp;
				temp.push_back(s);
				boundaryEdges[prev] = temp;
			}
		}
		prev = s;
		if(adj_list_itr != g_adj_list.end()) {
			for(std::vector<int>::iterator itr = adj_list_itr->second.begin(); itr != adj_list_itr->second.end(); itr++) {
				if(!visited[*itr]){
					if(find(queue.begin(), queue.end(),*itr)!=queue.end()) {
						continue;
					}
					else{
						queue.push_back(*itr);	
					}
				}
			}
		}
	}
}

int main(int argc, char** argv) {
	//testing example
	// g_adj_list[1]=mySet1;
	// g_adj_list[2]=mySet2;
	// g_adj_list[3]=mySet3;
	// g_adj_list[4]=mySet4;
	// g_adj_list[5]=mySet5;
	// g_adj_list[6]=mySet6;
	// g_adj_list[7]=mySet7;
	int ccCounter = -1;
	for(std::map<int, std::vector<int> >::iterator itr = g_adj_list.begin(); itr!=g_adj_list.end(); itr++) {
		visited[itr->first]=false;
	}
	for(std::map<int, bool>::iterator itr =visited.begin(); itr!=visited.end(); itr++) {
		if(!visited[itr->first]) {
			ccCounter += 1;
			std::vector<int> temp;
			connectedComponents.push_back(temp);
			bfs(itr->first, ccCounter);	
		}
	}
	for(int i=0; i<connectedComponents.size(); i++){
		std::cout<<"CC "<<i<<" - ";
		for(int j=0; j<connectedComponents[i].size(); j++) {
			std::cout<<connectedComponents[i][j]<<", ";
		}
		std::cout<<std::endl;
	}
	return 0;
}
