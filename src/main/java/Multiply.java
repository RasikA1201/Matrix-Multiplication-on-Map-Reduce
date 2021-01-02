/** Name: Rasika Hedaoo
 ** Student ID: 1001770527
 ** Assignment No: 2
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;


//initialization for M matrix
class MatrixM implements Writable {
    public double m_val;
    public int m_i_row;
    public int m_j_col;

    MatrixM () {}

    MatrixM ( int i, int j, double v ) {
        m_val = v;
        m_i_row = i;
        m_j_col = j;
    }

    public void readFields ( DataInput in ) throws IOException {
        m_val = in.readDouble();
        m_j_col = in.readInt();
        m_i_row = in.readInt();
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeDouble(m_val);
        out.writeInt(m_j_col);
        out.writeInt(m_i_row);
    }

}

//initialization for N matrix
class MatrixN implements Writable {
    public double n_val;
    public int n_j_row;
    public int n_k_Column;

    MatrixN () {}

    MatrixN ( int j, int k, double v ) {
        n_val = v;
        n_j_row = j;
        n_k_Column = k;
    }

    public void readFields ( DataInput in ) throws IOException {
        n_val = in.readDouble();
        n_j_row = in.readInt();
        n_k_Column = in.readInt();
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeDouble(n_val);
        out.writeInt(n_j_row);
        out.writeInt(n_k_Column);
    }

}

//flag is 0 or 1 operation
class Multi_Matrix implements Writable {
    public MatrixM Matrix_m;
    public MatrixN Matrix_n;
    public short flag_m_n;

    Multi_Matrix () {}
    Multi_Matrix ( MatrixM m ) { flag_m_n = 0; Matrix_m = m; }
    Multi_Matrix ( MatrixN n ) { flag_m_n = 1; Matrix_n = n; }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(flag_m_n);
        if (flag_m_n==0) {
            Matrix_m.write(out);
        } else {
            Matrix_n.write(out);
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        flag_m_n = in.readShort();
        if (flag_m_n==0) {
            Matrix_m = new MatrixM();
            Matrix_m.readFields(in);
        } else {
            Matrix_n = new MatrixN();
            Matrix_n.readFields(in);
        }
    }
}

//flag is 0 or 1 result
class multi_result implements Writable {
    public double value;

    multi_result () {}

    multi_result (double v ) {
        value = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeDouble(value);
    }


    public void readFields ( DataInput in ) throws IOException {
        value = in.readDouble();
    }

}

//return the result in key, value pair
class Pair implements WritableComparable<Pair> {
    public int i;
    public int k;

    Pair () {}

    Pair (int i_j_Value,int j_k_Value ) {
        i = i_j_Value; k = j_k_Value;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(k);
    }


    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        k = in.readInt();
    }

    @Override
    //find whether the value is greater than, less than or equal to 0 and giving the output result
    public int compareTo(Pair pairVal) {
        int i_j_Value = this.i;
        int j_k_Value = this.k;
        int pair_i_Val = pairVal.i;
        int pair_k_Val = pairVal.k;
        int result = 0;
        if(i_j_Value == pair_i_Val){
            if(j_k_Value == pair_k_Val){
                result = 0;
            }else if(j_k_Value < pair_k_Val){
                result = -1;
            }else{
                result = 1;
            }
        }else if(i_j_Value < pair_i_Val){
            result = -1;
        }else if (i_j_Value > pair_i_Val){
            result = 1;
        }
        return result;
    }

    public String toString () { return i+" "+k; } //removing "," from the text output and output of key and value
}


public class Multiply {
    // mapper for matrix M Starts
    public static class M_Mapper extends Mapper<Object,Text,IntWritable,Multi_Matrix > {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            MatrixM m = new MatrixM(s.nextInt(),s.nextInt(),s.nextDouble());
            context.write(new IntWritable(m.m_j_col),new Multi_Matrix(m));
            s.close();
        }
    }
//mapper for matrix M Ends

    // mapper for matrix N Starts
    public static class N_Mapper extends Mapper<Object,Text,IntWritable,Multi_Matrix > {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            MatrixN n = new MatrixN(s.nextInt(),s.nextInt(),s.nextDouble());
            context.write(new IntWritable(n.n_j_row),new Multi_Matrix(n));
            s.close();
        }
    }
//mapper for matrix N Ends

    // Reducer for Multiplication of Mapper M and Mapper N Starts
    public static class mXnReducer extends Reducer<IntWritable,Multi_Matrix,Pair,multi_result> {
        static Vector<MatrixM> matrixm = new Vector<MatrixM>();
        static Vector<MatrixN> Matrixn = new Vector<MatrixN>();
        public void reduce ( IntWritable key, Iterable<Multi_Matrix> values, Context context )
                throws IOException, InterruptedException {
            matrixm.clear();
            Matrixn.clear();
            for (Multi_Matrix v: values)
                if (v.flag_m_n == 0)
                    matrixm.add(v.Matrix_m);
                else Matrixn.add(v.Matrix_n);
            for ( MatrixM m: matrixm )
            {
                for ( MatrixN n: Matrixn )
                {
                    context.write(new Pair(m.m_i_row,n.n_k_Column),new multi_result(m.m_val*n.n_val));
                }
            }
        }
    }
// Reducer for Multiplication of Mapper M and Mapper N Ends

    // Mapper of Mapper M and Mapper N Starts
    public static class mXnMapper extends Mapper<Object,multi_result,Object,multi_result > {
        @Override
        public void map ( Object key, multi_result value, Context context )
                throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
// Mapper of Mapper M and Mapper N Ends

    // Reducer of Summation of the pairs Start
    public static class SumReducer extends Reducer<Pair,multi_result,Pair,String> {
        @Override
        public void reduce ( Pair key, Iterable<multi_result> values, Context context )
                throws IOException, InterruptedException {
            double m = 0.0;
            for (multi_result v: values) {
                m += v.value;
            };
            context.write(key,""+m); //removing "," from the text output and output of key and value
        }
    }
// Reducer of Summation of the pairs Start

    //Exception Handler
    public static void main(String[] args) throws Exception{
        //Job for Mapper and Reducer M and N
        Job job1 = Job.getInstance();
        job1.setJobName("MultiplyJob");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(multi_result.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Multi_Matrix.class);
        job1.setReducerClass(mXnReducer.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(2);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,M_Mapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,N_Mapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);

        //Job for Summing the output of Mapper and Reducer
        Job job2 = Job.getInstance();
        job2.setJobName("SummingJob");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(String.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(multi_result.class);
        job2.setReducerClass(SumReducer.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job2,new Path(args[2]),SequenceFileInputFormat.class,mXnMapper.class);
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
    }

}
