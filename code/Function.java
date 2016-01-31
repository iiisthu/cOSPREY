package bb;

import java.io.*;
import java.util.*;

public class Function
{
	public static int limit_n;
	public static int limit_m;
	
	public int n, m;
	public int variable[];
	public int mVariable[];
	public double values[];
	
	public Function()
	{
		n = 0;
		m = 1;
		variable = new int[limit_n];
		mVariable = new int[limit_n];
	}
	
	public void addVariable(int x, int y)
	{
		variable[n] = x;
		mVariable[n] = y;
		n ++;
		if (m > limit_m / y) {
			m = limit_m + 1;
		} else {
			m = m * y;
		}
	}
	
	public void createValues()
	{
		values = new double[m];
	}
	
	public int getAssignment(int assignment[])
	{
		int id = 0;
		for (int i = 0; i < n; i ++) {
			id = id * mVariable[i] + assignment[variable[i]];
		}
		return id;
	}
	
	public void setValues(double value[])
	{
		for (int i = 0; i < mVariable[0]; i ++) {
			values[i] = value[i];
		}
	}
	
	public void setValues(double value[][])
	{
		int k = 0;
		for (int i = 0; i < mVariable[0]; i ++) {
			for (int j = 0; j < mVariable[1]; j ++) {
				values[k ++] = value[i][j];
			}
		}
	}
	
	public double getValue(int assignment[])
	{
		int id = getAssignment(assignment);
		return values[id];
	}
	
	public boolean hasVariable(int var)
	{
		for (int i = 0; i < n; i ++)
		if (variable[i] == var) {
			return true;
		}
		return false;
	}
	
	public void outputVariables()
	{
		System.out.println(Arrays.toString(variable));
	}
	
	public void outputValues()
	{
		System.out.println(Arrays.toString(values));
	}
}
