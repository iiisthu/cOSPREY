
Introduction to cOSPREY
=======================
cOSPREY is an extension to OSPREY to allow the original design framework to scale to the commercial cloud infrastructures.

Requirement
-----------

*  JDK1.7
*  A hadoop 1.2.1 cluster (also tested on v1.0.3, other 1.x versions would probably work)
* Some knowledge about the basic usage of hadoop
*  *(Optional)* An HTTP server which stores a float number, i.e. the minimum global upper bound. The server should be able to update the value upon a post request, and return the current minimum


Usage
------------
To get the code and compile it into a jar file, use the following commands:

        $ git clone https://github.com/iiisthu/cOSPREY.git
        $ cd cOSPREY/code
        $ sh make.sh
        
This will create `brachandbound.jar`. Next we create the input data and put it onto HDFS. With `data/small/1I27` as an example, first we create a file `input` containing lines below

    0.000000 -1000000000.000000 1000000000.000000 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1
    1000000000.000000 -1000000000.000000 1000000000.000000 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1
    1000000000.000000 -1000000000.000000 1000000000.000000 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1
    1000000000.000000 -1000000000.000000 1000000000.000000 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1

Lines except the first are exactly the same. These lines are used to parallel the Monte Carlo simulation and there can be many such lines, not just 3. Since we should have only one input, only the first line is different.

To put the data and input onto HDFS, use the following commands:

    $ hadoop fs -put data/small/1i27.txt /1i27/data
    $ hadoop fs -put input /1i27/input/input

To run this design case, modify the `NAME` and number of mutable residues in `run.sh` and use

    $ sh run.sh


Support or Contact
------------------
Feel free to contact authors: panyuchao@gmail.com, summerdaway@gmail.com

