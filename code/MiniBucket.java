package bb;

import java.io.*;
import java.util.*;

public class MiniBucket
{
	public static int max_n;
	public static int limit_n;
	public HashSet<Integer> variables;
	public List<Function> functions;
	
	public MiniBucket()
	{
		variables = new HashSet<Integer>();
		functions = new ArrayList<Function>();
	}
	
	public boolean allowFunction(Function f)
	{
		int n = variables.size();
		for (int i = 0; i < f.n; i ++)
		if (!variables.contains(f.variable[i])) {
			n ++;
		}
		return n <= limit_n + 1;
	}
	
	public void addFunction(Function f)
	{
		functions.add(f);
		for (int i = 0; i < f.n; i ++) {
			variables.add(f.variable[i]);
		}
	}
	
	public void outputVariables()
	{
		Iterator<Integer> v = variables.iterator();
		while (v.hasNext()) {
			System.out.println("v = " + v.next());
		}
	}
	
	public Function bucketElimination(int v, int c[], boolean computeValues)
	{
		Function ret = new Function();
		Iterator<Integer> itr = variables.iterator();
		while (itr.hasNext()) {
			int now_v = itr.next();
			if (now_v != v) {
				ret.addVariable(now_v, c[now_v]);
			}
		}
		if (computeValues) {
			int assignment[] = new int[max_n];
			for (int i = 0; i < max_n; i ++) {
				assignment[i] = 0;
			}
			ret.createValues();
			int last_v = ret.variable[ret.n - 1];
			for (int i = 0; i < ret.m; i ++) {
				double minSum = 1e99;
				for (int j = 0; j < c[v]; j ++) {
					double sum = 0;
					assignment[v] = j;
					for (int k = 0; k < functions.size(); k ++) {
						sum += functions.get(k).getValue(assignment);
					}
					if (sum < minSum) {
						minSum = sum;
					}
				}
				ret.values[i] = minSum;
				assignment[last_v] ++;
				for (int j = ret.n - 1; j > 0; j --)
				if (assignment[ret.variable[j]] == ret.mVariable[j]) {
					assignment[ret.variable[j]] = 0;
					assignment[ret.variable[j - 1]] ++;
				} else {
					break;
				}
			}
		}
		return ret;
	}
}
