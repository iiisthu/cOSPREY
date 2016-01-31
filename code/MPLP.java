package bb;

import java.io.*;
import java.util.*;

public class MPLP
{
    static double delta = 1e-2;
	static int n;
	static int p[];
	static int q[];
	static int r[];
	static double b[][][][];
	static double c[][][];
	static double d[][];
	static double v1[][];
	static double v2[][][][];
	
	public MPLP(int nn, int assignment[], int cc[], double value1[][], double value2[][][][])
	{
		p = new int[nn];
		q = new int[nn];
		r = new int[nn];
		v1 = new double[nn][];
		n = 0;
		for (int i = 0; i < nn; i ++)
		if (assignment[i] == -1) {
			p[n] = cc[i];
			q[i] = n;
			r[n] = i;
			v1[n] = new double[p[n]];
			for (int j = 0; j < p[n]; j ++) {
				v1[n][j] = value1[i][j];
			}
			n ++;
		}
		d = new double[n][];
		for (int i = 0; i < n; i ++) {
			d[i] = new double[p[i]];
		}
		b = new double[n][n][][];
		for (int i = 0; i < n; i ++) {
			for (int j = 0; j < n; j ++) {
				b[i][j] = new double[p[i]][p[j]];
			}
		}
		c = new double[n][n][];
		for (int i = 0; i < n; i ++) {
			for (int j = 0; j < n; j ++) {
				c[j][i] = new double[p[i]];
			}
		}
		v2 = value2;
	}
	
	public double getSum()
	{
		double sum = 0;
		for (int i = 0; i < n; i ++) {
			double min_sum = 1e99;
			for (int j = 0; j < p[i]; j ++) {
				min_sum = Math.min(min_sum, d[i][j]);
			}
			sum += min_sum;
		}
		return sum;
	}
	
	public double getHeuristic(int nn, int assignment[], int maxIter, double minGlobalUpperBound)
	{
		for (int i = 0; i < n; i ++) {
			for (int x = 0; x < p[i]; x ++) {
				d[i][x] = v1[i][x];
				for (int j = 0; j < nn; j ++)
				if (assignment[j] != -1) {
					d[i][x] += v2[r[i]][j][x][assignment[j]];
				}
			}
		}
		for (int i = 0; i < n; i ++) {
			for (int j = i + 1; j < n; j ++) {
				for (int x = 0; x < p[i]; x ++) {
					for (int y = 0; y < p[j]; y ++) {
						b[i][j][x][y] = v2[r[i]][r[j]][x][y] / 2.0;
						b[j][i][y][x] = v2[r[j]][r[i]][y][x] / 2.0;
					}
				}
			}
		}
		for (int i = 0; i < n; i ++) {
			for (int j = 0; j < n; j ++)
			if (i != j) {
				for (int x = 0; x < p[i]; x ++) {
					c[j][i][x] = 1e99;
					for (int y = 0; y < p[j]; y ++) {
						c[j][i][x] = Math.min(c[j][i][x], b[j][i][y][x]);
					}
					d[i][x] += c[j][i][x];
				}
			}
		}
		double ret = getSum();
		for (int iter = 0; iter < maxIter && ret < minGlobalUpperBound; iter ++) {
            for (int i = 0; i < n; i ++) {
				for (int j = i + 1; j < n; j ++) {
					double c_i[] = new double[p[i]];
					double c_j[] = new double[p[j]];
					for (int x = 0; x < p[i]; x ++) {
						c_i[x] = 1e99;
						for (int y = 0; y < p[j]; y ++) {
							c_i[x] = Math.min(c_i[x], d[j][y] - c[i][j][y] + v2[r[i]][r[j]][x][y]);
						}
						c_i[x] -= d[i][x] - c[j][i][x];
						c_i[x] /= 2.0;
					}
					for (int y = 0; y < p[j]; y ++) {
						c_j[y] = 1e99;
						for (int x = 0; x < p[i]; x ++) {
							c_j[y] = Math.min(c_j[y], d[i][x] - c[j][i][x] + v2[r[i]][r[j]][x][y]);
						}
						c_j[y] -= d[j][y] - c[i][j][y];
						c_j[y] /= 2.0;
					}
					for (int x = 0; x < p[i]; x ++) {
						d[i][x] += c_i[x] - c[j][i][x];
						c[j][i][x] = c_i[x];
					}
					for (int y = 0; y < p[j]; y ++) {
						d[j][y] += c_j[y] - c[i][j][y];
						c[i][j][y] = c_j[y];
					}
				}
			}
            double prev_ret = ret;
			ret = getSum();
            if (Math.abs(ret - prev_ret) < delta) break;
		}
		return ret;
	}
	
}
