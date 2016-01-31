/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *	  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf ;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext ;
import org.apache.hadoop.mapreduce.Counter ;
import org.apache.hadoop.mapreduce.Counters ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BranchAndBound {
	static long numReduceTasks = 1;
	static long numOutput = 1;
	static long maxNumReduceTasks = 190;
	static double eps = 1e-7 ;
	static int mem_limit = 1000000;
    static final double Infinity = 1e99;
    static double minGlobalUpperBound = Infinity;
    static int iterRound = 0;
	static int MPLP_TIME_LIMIT = 1000000;
    static int MAX_MPLP_ITER = 1000;
	static int MIN_MPLP_ITER = 10;
	   
    
	public static class BBMapper1 extends Mapper <Object, Text, NullWritable, Text> {
		
		static int n = 0;
		static int c[];
		static int d[];
		static int p[];
		static double value1[][];
		static double value2[][][][];
		static double allMin[][];
		static double rowMin[][][];
		static double deeMin[][][][];
		static List<String> valueList;
		static final int maxListSize = 1000000;
		static final int SpaceLimit = 10000;
		static int numReduceTasks;
		static boolean hasBuildMiniBucket;
		static MiniBucketHeuristic miniBucketHeuristic;
		static boolean hasBuildMPLP;
		static MPLP myMPLP;
		static int MPLP_ITER;
		static double bSum, dSum;
		MyThread TH;
        
        public double MCMC(int b[]) {
            int d[] = new int[n];
            double T = 2000;
            for( int i = 0 ; i < n ; i++ )
                if (b[i] == -1) {
                    d[i] = (int)(Math.random()*c[i]);
                } else {
                    d[i] = b[i];
                }
            double now = 0 ;
            for( int i = 0 ; i < n ; i++ ) {
                now += value1[i][d[i]];
                for( int j = 0 ; j < n ; j++ )
                    if( i < j ) {
                        now += value2[i][j][d[i]][d[j]];
                    }
            }
            double best = now;
            while( T > 1e-5 ) {
                int t = (int)(Math.random()*n) ;
                while( b[t] != -1 ) {
                    t = (int)(Math.random()*n) ;
                }
                int x = (int)(Math.random()*c[t]);
                double change = -value1[t][d[t]] + value1[t][x];
                for( int i = 0 ; i < n ; i++ )
                    if( i != t ) {
                        change += -value2[t][i][d[t]][d[i]] + value2[t][i][x][d[i]];
                    }
                if( change < -1e-8 ) {
                    d[t] = x ;
                    now += change ;
                } else {
                    double p = Math.pow(Math.E, -change/(1.38046*1e-23*T));
                    if( p - Math.random() > 1e-8 ) {
                        d[t] = x ;
                        now += change ;
                    }
                }
                best = Math.min( best, now );
                T = T*0.999;
            }
            return best;
        }
        
        public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
            String jobName = context.getJobName();
            for(int i = jobName.length()-1; i >= 0; --i) {
                if(jobName.charAt(i) == '_') {
                    iterRound = Integer.parseInt(jobName.substring(i+1, jobName.length()));
                    break;
                }
            }
            // start thread to process http request
            TH = new MyThread();
            //TH.setDaemon(true);
            TH.start();
			// handle exception ?????
			Path inputdata = DistributedCache.getLocalCacheFiles(conf)[0];
			Scanner input = new Scanner(new BufferedReader(new FileReader(inputdata.toString())));
			n = input.nextInt();
			c = new int[n];
			d = new int[n];
			value1 = new double[n][];
			for( int i = 0 ; i < n ; i++ ) {
				c[i] = input.nextInt() ;
				value1[i] = new double[c[i]];
				for( int j = 0 ; j < c[i] ; j++ ) {
					value1[i][j] = input.nextDouble();
				}
			}
			value2 = new double[n][n][][] ;
			for( int i = 0 ; i < n ; i++ ) {
				for( int j = i + 1 ; j < n ; j++ ) {
					value2[i][j] = new double[c[i]][c[j]] ;
					for( int x = 0 ; x < c[i] ; x++ ) {
						for( int y = 0 ; y < c[j] ; y++ ) {
							value2[i][j][x][y] = input.nextDouble() ;
						}
					}
				}
			}
			for( int i = 0 ; i < n ; i++ ) {
				for( int j = 0 ; j < i ; j++ ) {
					value2[i][j] = new double[c[i]][c[j]] ;
					for( int x = 0 ; x < c[i] ; x++ ) {
						for( int y = 0 ; y < c[j] ; y++ ) {
							value2[i][j][x][y] = value2[j][i][y][x] ;
						}
					}
				}
			}
            // MCMC with original data
            if( iterRound == 0 ) {
                int b[] = new int[n];
                for(int i = 0; i < n; ++i) {
                    b[i] = -1;
                }
                for(int i = 0; i < 100; ++i) {
                    minGlobalUpperBound = Math.min(minGlobalUpperBound, MCMC(b));
                }
                // update minGlobalUpperBound
                //updateBound();
                //return;
            }
			//bSum = input.nextDouble();
			input.close() ;
			p = new int[n];
			for (int i = 0; i < n; i ++) {
				p[i] = 0;
				for (int j = 0; j < c[i]; j ++)
				if (value1[i][j] < value1[i][p[i]]) {
					p[i] = j;
				}
			}
			allMin = new double[n][n];
			rowMin = new double[n][n][];
			for (int i = 0; i < n; i ++) {
				for (int j = 0; j < n; j ++)
				if (i != j) {
					rowMin[i][j] = new double[c[i]];
					for (int k = 0; k < c[i]; k ++) {
						rowMin[i][j][k] = value2[i][j][k][0];
						for (int l = 0; l < c[j]; l ++)
						if (value2[i][j][k][l] < rowMin[i][j][k]) {
							rowMin[i][j][k] = value2[i][j][k][l];
						}
						if (rowMin[i][j][k] < allMin[i][j]) {
							allMin[i][j] = rowMin[i][j][k];
						}
					}
				}
			}
			deeMin = new double[n][][][];
			for (int i = 0; i < n; i ++) {
				deeMin[i] = new double[c[i]][c[i]][n];
				for (int x = 0; x < c[i]; x ++) {
					for (int y = 0; y < c[i]; y ++)
					if (x != y) {
						for (int j = 0; j < n; j ++)
						if (i != j) {
							deeMin[i][x][y][j] = Infinity;
							for (int k = 0; k < c[j]; k ++)
							if (value2[i][j][x][k] - value2[i][j][y][k] < deeMin[i][x][y][j]) {
								deeMin[i][x][y][j] = value2[i][j][x][k] - value2[i][j][y][k];
							}
						}
					}
				}
			}
			valueList = new ArrayList<String>();
			Counter cnt = context.getCounter("MyCounter", "Map Setup Counter");
			cnt.increment(1);
			JobConf jobConf = (JobConf) conf;
			numReduceTasks = (int)jobConf.getNumReduceTasks() ;
			hasBuildMiniBucket = true;
			hasBuildMPLP = false;
            // update|get minGlobalUpperBound from server
            synchronized(MyThread.minGlobalUpperBound) {
                double t = MyThread.minGlobalUpperBound.minGlobalUpperBound;
                if(minGlobalUpperBound - t >= 0) {
                    minGlobalUpperBound = t;
                } else {
                    MyThread.minGlobalUpperBound.minGlobalUpperBound = minGlobalUpperBound;
                }
            }
		}
		
		public static double getMinValue(int i, int x, int j, int y) {
			if ((x == -1) && (y == -1)) {
				return allMin[i][j];
			} else if (x == -1) {
				return rowMin[j][i][y];
			} else if (y == -1) {
				return rowMin[i][j][x];
			} else {
				return value2[i][j][x][y];
			}
		}
		
		public static boolean checkDEE(int b[]) {
            for (int i = 0; i < n; i ++)
            if (b[i] != -1) {
                int x = b[i];
                for (int y = 0; y < c[i]; y ++)
                    if (x != y) {
                        double sum = value1[i][x] - value1[i][y];
                        for (int j = 0; j < n; j ++)
                        if (i != j) {
                            if (b[j] == -1) {
                                sum += deeMin[i][x][y][j];
                            } else {
                                sum += value2[i][j][x][b[j]] - value2[i][j][y][b[j]];
                            }
                        }
                        if (sum > eps) return false;
                    }
            }
			return true;
		}
		
		public static double calcNaiveHeuristic(int b[])
		{
			double result = 0.0;
			for (int i = 0; i < n; i ++)
			if (b[i] == -1) {
				double minSum = Infinity;
				for (int v = 0; v < c[i]; v ++) {
					double sum = value1[i][v];
					for (int j = 0; j < n; j ++)
					if (i != j) {
						if (b[j] == -1) {
							sum += getMinValue(i, v, j, b[j]) / 2.0;
						} else {
							sum += value2[i][j][v][b[j]];
						}
					}
					if (sum < minSum) {
						minSum = sum;
						d[i] = v;
					}
				}
				result += minSum;
			}
			return result;
		}
		
		public static int buildMiniBucket(int b[], boolean computeValues)
		{
			miniBucketHeuristic = new MiniBucketHeuristic();
			int size = 0;
			for (int i = 0; i < n; i ++)
			if (b[i] < 0) {
				Function f = new Function();
				f.addVariable(i, c[i]);
				f.createValues();
				f.setValues(value1[i]);
				miniBucketHeuristic.addFunction(f);
				size += f.m;
			}
			for (int i = 0; i < n; i ++) {
				for (int j = i + 1; j < n; j ++)
				if (b[i] < 0 || b[j] < 0) {
					Function f = new Function();
					f.addVariable(i, c[i]);
					f.addVariable(j, c[j]);
					f.createValues();
					f.setValues(value2[i][j]);
					miniBucketHeuristic.addFunction(f);
					size += f.m;
				}
			}
			for (int i = 0; i < n; i ++)
			if (b[i] == -1) {
				size += miniBucketHeuristic.eliminateVariable(i, c, computeValues);
			}
			return size;
		}
		
		public static void buildMiniBucketHeuristic(int b[])
		{
			MiniBucket.max_n = n;
			Function.limit_m = mem_limit;
			MiniBucketHeuristic.mem_limit = mem_limit;
			int bucket_bound = n - 1;
			for (; bucket_bound >= 2; bucket_bound --) {
				Function.limit_n = bucket_bound;
				MiniBucket.limit_n = bucket_bound;
				int size = buildMiniBucket(b, false);
				if (size <= mem_limit) break;
			}
			buildMiniBucket(b, true);
		}
		
		public static void buildMPLP(int b[])
		{
			myMPLP = new MPLP(n, b, c, value1, value2);
			int SUM_2 = 0;
			for (int i = 0; i < n; i ++) {
				for (int j = i + 1; j < n; j ++)
				if (b[i] == -1 && b[j] == -1) {
					SUM_2 += c[i] * c[j];
				}
			}
			if (SUM_2 > 0) {
				MPLP_ITER = MPLP_TIME_LIMIT / SUM_2;
				MPLP_ITER = Math.min(MPLP_ITER, MAX_MPLP_ITER);
				MPLP_ITER = Math.max(MPLP_ITER, MIN_MPLP_ITER);
			} else {
				MPLP_ITER = MAX_MPLP_ITER;
			}
		}
		
		public static double calcMPLPHeuristic(int b[], double minGlobalUpperBound)
		{
			return myMPLP.getHeuristic(n, b, MPLP_ITER, minGlobalUpperBound + eps);
		}
		
		public static double calcDSUM(int b[])
		{
			double result = 0.0;
			for (int i = 0; i < n; i ++)
			if (b[i] != -1) {
				d[i] = b[i];
				result += value1[i][b[i]];
				for (int j = 0; j < n; j ++)
				if (i != j && b[j] != -1) {
					result += value2[i][j][b[i]][b[j]] / 2.0;
				}
			}
			return result;
		}
		
		public static double calcMiniBucketHeuristic(int b[])
		{
			return miniBucketHeuristic.getHeuristic(b);
		}
		
		public static double calcLowerBound(int k, int b[]) {
			double tempSUM = dSum + value1[k][b[k]];
			d[k] = b[k];
			for (int i = 0; i < n; i ++)
			if (b[i] != -1 && k != i) {
				tempSUM += value2[i][k][b[i]][b[k]];
			}
			double naiveHeuristic = calcNaiveHeuristic(b) + tempSUM;
			if (naiveHeuristic > minGlobalUpperBound + eps) return naiveHeuristic;
			// double result = Math.max(calcNaiveHeuristic(b), calcMPLPHeuristic(b)) + tempSUM;
			// result = Math.max(result, calcMiniBucketHeuristic(b));
			return calcMPLPHeuristic(b, minGlobalUpperBound - tempSUM) + tempSUM;
		}
		
		public static double calcUpperBound(int b[]) {
			double sum = 0;
			double count_c = 1;
            int count_n = 1;
			for (int i = 0; i < n; i ++) {
				sum += value1[i][d[i]];
				for (int j = 0; j < i; j ++) {
					sum += value2[i][j][d[i]][d[j]];
				}
				if (b[i] != -1) {
                    count_c *= c[i];
                    count_n ++;
                }
			}
			if (count_c <= 1e6 && count_n < n)
            for( int iter = 0; iter < 10; iter++ ) {
                double T = 2000;
                for( int i = 0 ; i < n ; i++ )
                if (b[i] == -1) {
                    d[i] = (int)(Math.random()*c[i]);
                } else {
                    d[i] = b[i];
                }
                double now = 0 ;
                for( int i = 0 ; i < n ; i++ ) {
                    now += value1[i][d[i]];
                    for( int j = 0 ; j < n ; j++ )
                        if( i < j ) {
                            now += value2[i][j][d[i]][d[j]];
                        }
                }
                double best = now;
                while( T > 1e-5 ) {
                    int t = (int)(Math.random()*n) ;
                    while( b[t] != -1 ) {
                        t = (int)(Math.random()*n) ;
                    }
                    int x = (int)(Math.random()*c[t]);
                    double change = -value1[t][d[t]] + value1[t][x];
                    for( int i = 0 ; i < n ; i++ )
                        if( i != t ) {
                            change += -value2[t][i][d[t]][d[i]] + value2[t][i][x][d[i]];
                        }
                    if( change < -1e-8 ) {
                        d[t] = x ;
                        now += change ;
                    } else {
                        double p = Math.pow(Math.E, -change/(1.38046*1e-23*T));
                        if( p - Math.random() > 1e-8 ) {
                            d[t] = x ;
                            now += change ;
                        }
                    }
                    best = Math.min( best, now );
                    T = T*0.999;
                }
                sum = best;
			}
			return sum;
		}
		
		public static int selectNode(int b[]) {
			for (int i = 0; i < n; i ++)
			if (b[i] == -1) {
				return i;
			}
			return -1;
			/*
			int k = -1;
			double min_sum = Infinity;
			for (int i = 0; i < n; i ++)
			if (b[i] == -1) {
				double sum = value1[i][d[i]];
				for (int j = 0; j < n; j ++)
				if (i != j) {
					sum += getMinValue(i, d[i], j, b[j]) / 2.0;
				}
				if (sum < min_sum) {
					min_sum = sum;
					k = i;
				}
			}
			return k;
			*/
		}
		
		public static String getInfo(double a[], int b[]) {
			String str = "" + a[0] + " " + a[1] + " " + a[2];
			for (int i = 0; i < n; i ++) {
				str = str + " " + b[i];
			}
			return str;
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Counter cntx = context.getCounter("MyCounter", "Map Input Counter");
			cntx.increment(1);
            if( iterRound == 0 ) {
                context.getCounter("MyCounter", "iteration round").increment(1);
            }
			if (itr.countTokens() != n + 3) return;
			double a[] = new double[3];
			int b[] = new int[n];
			for (int i = 0; i < 3; i ++) {
				a[i] = Double.parseDouble(itr.nextToken());
			}
			for (int i = 0; i < n; i ++) {
				b[i] = Integer.parseInt(itr.nextToken());
				if (c[i] == 1) b[i] = 0;
			}
            if( iterRound == 0 ) {
                if( Math.abs(a[0]) < 1e-8 ) {
                    a[0] = minGlobalUpperBound;
                    context.write(NullWritable.get() , new Text(getInfo(a, b)));
                }
                return;
            }
            // cut current node
            if( !(a[1] < minGlobalUpperBound + eps || a[2] < minGlobalUpperBound + eps) ) {
                context.getCounter("MyCounter", "Previous Cut By Bound").increment(1);
                return;
            }
            
			// calcLowerBound(a, b);
			int k = selectNode(b);
			if (k == -1) return;
			if (!hasBuildMiniBucket) {
				hasBuildMiniBucket = true;
				b[k] = -2;
				buildMiniBucketHeuristic(b);
				b[k] = -1;
			}
			if (!hasBuildMPLP) {
				hasBuildMPLP = true;
				b[k] = 0;
				buildMPLP(b);
				b[k] = -1;
			}
			dSum = calcDSUM(b);
			double globalUpperBound = a[0];
			double lowerBound[] = new double[c[k]];
			double upperBound[] = new double[c[k]];
			for (int i = 0; i < c[k]; i ++) {
                b[k] = i;
                if (checkDEE(b)) {
                    lowerBound[i] = calcLowerBound(k, b);
                    upperBound[i] = calcUpperBound(b);
                    globalUpperBound = Math.min(globalUpperBound, upperBound[i]);
                } else {
                    lowerBound[i] = Infinity;
                    upperBound[i] = Infinity;
                    Counter cnt = context.getCounter("MyCounter", "Cut By DEE");
                    cnt.increment(1);
                }
                b[k] = -1;
            }
            if( globalUpperBound < minGlobalUpperBound ) {
                minGlobalUpperBound = globalUpperBound;
                synchronized(MyThread.minGlobalUpperBound) {
                    MyThread.minGlobalUpperBound.minGlobalUpperBound = Math.min(MyThread.minGlobalUpperBound.minGlobalUpperBound, minGlobalUpperBound);
                    minGlobalUpperBound = MyThread.minGlobalUpperBound.minGlobalUpperBound;
                }
            }
			a[0] = minGlobalUpperBound;
			for (int i = 0; i < c[k]; i ++)
			if (lowerBound[i] < minGlobalUpperBound + eps) {
				b[k] = i;
				a[1] = lowerBound[i];
				a[2] = upperBound[i];
				String strInfo = getInfo(a, b);
				if( valueList.size() == maxListSize ) {
					Counter cnt = context.getCounter("MyCounter", "Map List Counter");
					cnt.increment(1);
					outputList(context) ;
				}
				valueList.add(strInfo) ;
				Counter cnt = context.getCounter("MyCounter", "Map Output Counter");
				cnt.increment(1);
			} else if (lowerBound[i] < Infinity) {
				Counter cnt = context.getCounter("MyCounter", "Cut By Bound");
				cnt.increment(1);
			}
		}
		
		public static void outputList(Context context) throws IOException, InterruptedException {
			for( String outputValue: valueList ) {
				StringTokenizer itr = new StringTokenizer(outputValue);
				double a[] = new double[3] ;
				for( int i = 0 ; i < 3 ; i++ ) {
					a[i] = Double.parseDouble(itr.nextToken()) ;
				}
				double lowerBound = a[1] , upperBound = a[2] ;
				if( lowerBound < minGlobalUpperBound + eps || upperBound < minGlobalUpperBound + eps ) {
					context.write(NullWritable.get(), new Text(outputValue) ) ;
				} else {
                    context.getCounter("MyCounter", "Cut By Bound").increment(1);
                }
			}
			valueList.clear();
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
            synchronized(MyThread.minGlobalUpperBound) {
                MyThread.minGlobalUpperBound.flag = true;
            }
			outputList(context) ;
			Counter cnt = context.getCounter("MyCounter", "cleanup");
			cnt.increment(1);
			TH.join();
		}
	}

	public static class BBReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		static final double Infinity = 1e99;
		static List<String> valueList = new ArrayList<String>() ;
		static final int maxListSize = 1000000 ;
		static double minGlobalUpperBound = Infinity ;
		
		public static void outputList(Context context) throws IOException, InterruptedException {
			for( String outputValue: valueList ) {
				StringTokenizer itr = new StringTokenizer(outputValue);
				double a[] = new double[3] ;
				for( int i = 0 ; i < 3 ; i++ ) {
					a[i] = Double.parseDouble(itr.nextToken()) ;
				}
				double lowerBound = a[1] , upperBound = a[2] ;
				if( lowerBound < minGlobalUpperBound + eps || upperBound < minGlobalUpperBound + eps ) {
					Counter cnt = context.getCounter("MyCounter", "Reduce Output Nodes Counter");
					cnt.increment(1);
					context.write( NullWritable.get() , new Text(outputValue) ) ;
				} else {
					Counter cnt = context.getCounter("MyCounter", "Cut By Bound");
					cnt.increment(1);
				}
			}
			valueList.clear();
		}
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				StringTokenizer itr = new StringTokenizer(value.toString());
				double globalUpperBound = Double.parseDouble(itr.nextToken());
				minGlobalUpperBound = Math.min(minGlobalUpperBound, globalUpperBound);
				double lowerBound = Double.parseDouble(itr.nextToken());
				double upperBound = Double.parseDouble(itr.nextToken());
				if (lowerBound < minGlobalUpperBound + eps || upperBound < minGlobalUpperBound + eps) {
					context.write(NullWritable.get(), new Text(value));
					context.getCounter("MyCounter", "Reduce Output Nodes Counter").increment(1);
				}
			}
/*
			for (Text value : values) {
				if( valueList.size() == maxListSize ) {
					outputList(context) ;
					Counter cnt = context.getCounter("MyCounter", "reduceListNum");
					cnt.increment(1);
				}
				valueList.add(value.toString()) ;
				StringTokenizer itr = new StringTokenizer(value.toString());
				double globalUpperBound = Double.parseDouble(itr.nextToken());
				minGlobalUpperBound = Math.min( minGlobalUpperBound , globalUpperBound ) ;
			}
			outputList(context) ;
*/
		}
	}

	static Job getJob(String input, String output, String dataDir, int iteration ) throws Exception{
		Configuration conf = new Configuration() ;
        
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] fileStatus = hdfs.listStatus(new Path(input));
        for(int i = 0; i < fileStatus.length; ++i) {
            if(fileStatus[i].getLen() == 0) {
                hdfs.delete(fileStatus[i].getPath());
            }
        }
		DistributedCache.addCacheFile(new URI(dataDir + "/data"), conf) ;
		Job ret = new Job(conf, dataDir + "_iteration_" + iteration ) ;
		ret.setJarByClass(BranchAndBound.class) ;
		ret.setMapperClass(BBMapper1.class) ;
		ret.setReducerClass(BBReducer.class) ;
        //ret.setReducerClass(MergeReducer.class);
		FileInputFormat.setInputPaths(ret, new Path(input)) ;
		//if( iteration > 7 ) FileInputFormat.setMinInputSplitSize(ret, 67108864);
		FileOutputFormat.setOutputPath(ret, new Path(output)) ;
		ret.setOutputKeyClass(NullWritable.class);
		ret.setOutputValueClass(Text.class);
		return ret ;
	}

    public static class MergeReducer extends Reducer<Object, Text, NullWritable, Text> {
		public void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
                context.write(NullWritable.get(), new Text(value));
			}
        }
    }
	
	public static void main(String[] args) throws Exception {
		/*Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: branchandbound <input> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "branch and bound");
		job.setJarByClass(BranchAndBound.class);
		job.setMapperClass(BBMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);*/
		int n ;
		String[] inputargs = new GenericOptionsParser(
					new Configuration(), args).getRemainingArgs();
		if( inputargs.length != 2 ) {
			System.err.println("Usage: branchandbound <data directory> <n>") ;
			System.exit(2) ;
		}
		n = Integer.parseInt(inputargs[1]);
        String dataDir = inputargs[0] ;
		String prev_output = dataDir + "/input" ;
/*		for( int i = 1 ; i <= n ; i++ ) {
			for( int j = 0 ; j < 2 ; j++ ) {
				String input = prev_output ;
				String output = inputargs[1] + "/iteration" + i + "_" + j ;
				Job job = getJob(input, output, i, j) ;
				job.waitForCompletion(true) ; // if failed ????
				prev_output = output;
			}
		}
*/
		//prev_output = dataDir + "/output" + "/iteration" + 17;
        long totalNodes = 0;
        long searchedNodes = 0;
        long cutbyDEE = 0;
        int mapTotal = 768;
		for( int i = 0 ; i <= n ; i++ ) {
            iterRound = i;
			String input = prev_output ;
			String output = dataDir + "/output" + "/iteration" + i ;
			Job job = getJob(input, output, dataDir, i) ;
            if( i == n ) {
				numReduceTasks = 1 ;
			}
			//job.setNumMapTasks(200);
            if(numOutput > mapTotal) {
                FileInputFormat.setMaxInputSplitSize(job, 10*(8*n+10) + numOutput*(8*n+10)/3000);
                FileInputFormat.setMinInputSplitSize(job, Math.max((8*n+10), numOutput*(8*n+10)/5000));
            } else {
                FileInputFormat.setMaxInputSplitSize(job, (8*n+10));
            }
            /*
			if( i == 0 ) {
                job.setNumReduceTasks(1);
            } else {
                job.setNumReduceTasks(0);
            }
             */
            job.setNumReduceTasks(0);
			job.waitForCompletion(true) ; // if failed ????
			prev_output = output;
			Counters counters = job.getCounters() ;
			Counter counter = counters.findCounter("MyCounter", "Map Output Counter") ;
			numOutput = counter.getValue();
            totalNodes += numOutput;
            cutbyDEE += counters.findCounter("MyCounter", "Cut By DEE").getValue();
            searchedNodes += totalNodes + cutbyDEE + counters.findCounter("MyCounter", "Cut By Bound").getValue();
			System.out.println(numOutput+" "+(8*n+10)+" "+(numOutput*(8*n+10)/768));
		}
        System.out.println("searchedNodes " + searchedNodes);
        System.out.println(totalNodes);
        System.out.println("cut by dee "+cutbyDEE);
	}
}