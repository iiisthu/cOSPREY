#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <iostream>
using namespace std;
int N;

int main(int argv, char* argc[])
{
    if (argv != 2) return 0;
    printf("PROTEIN = %s\n", argc[1]);
    string inputFile = argc[1];
    inputFile = inputFile + "/" + argc[1] + ".txt";
    string outputFile = argc[1];
    outputFile = outputFile + "/" + argc[1] + "_input";
    freopen(inputFile.c_str(), "r", stdin);
    scanf("%d", &N);
    printf("%d\n", N);
    freopen(outputFile.c_str(), "w", stdout);
    printf("%lf %lf %lf", 1e9, -1e9, 1e9);
    for (int i = 0; i < N; i ++) {
        printf(" -1");
    }
    printf("\n");
    return 0;
}
