package bb;

import java.io.*;
import java.util.*;

public class MiniBucketHeuristic
{
	static int mem_limit;
	
	public List<Function> functions;
	
	public MiniBucketHeuristic()
	{
		functions = new ArrayList<Function>();
	}
	
	public void addFunction(Function f)
	{
		functions.add(f);
	}
	
	public int eliminateVariable(int var, int c[], boolean computeValues)
	{
		int size = 0;
		List<MiniBucket> m_bucket = new ArrayList<MiniBucket>();
		Iterator<Function> itr = functions.iterator();
		while (itr.hasNext()) {
			Function f = itr.next();
			if (f.hasVariable(var)) {
				itr.remove();
				boolean flag = false;
				for (int i = 0; i < m_bucket.size(); i ++)
				if (m_bucket.get(i).allowFunction(f)) {
					flag = true;
					m_bucket.get(i).addFunction(f);
					break;
				}
				if (!flag) {
					MiniBucket b = new MiniBucket();
					b.addFunction(f);
					m_bucket.add(b);
				}
			}
		}
		for (int i = 0; i < m_bucket.size(); i ++) {
			Function f = m_bucket.get(i).bucketElimination(var, c, computeValues);
			functions.add(f);
			size += f.m;
		}
		return size;
	}
	
	public double getHeuristic(int assignment[])
	{
		double h = 0;
		for (int i = 0; i < functions.size(); i ++) {
			h += functions.get(i).getValue(assignment);
		}
		return h;
	}
}
