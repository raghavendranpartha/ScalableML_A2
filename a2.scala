import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
 
object Assign2 {    

    def main(args: Array[String]) 
    {
        val conf = new SparkConf().setAppName("Assign2")
        val sc = new SparkContext(conf)
        val datafile = args(0)
        val missingfile = args(1)
        val outfile = args(2)
        /*
        val datafile = "small.csv"
        val missingfile = "small_missing.csv"
        val outfile = "small_out.csv"
        */
        val ofile = new File(outfile)
        val output = new BufferedWriter(new FileWriter(ofile))

        //val cmat = new CoordinateMatrix(sc.parallelize(Source.fromFile(datafile).getLines().map(line => line.split(",")).toList.map(x => MatrixEntry(x(0).toLong,x(1).toLong,x(2).toDouble))))
        println("findmeee start code")
        val dat = sc.textFile(datafile)
        dat.cache()
        println("findmeee convert to coordinate matrix")
        val cmat = new CoordinateMatrix(dat.map(line => line.split(",")).map(x => MatrixEntry(x(0).toLong,x(1).toLong,x(2).toDouble)))
        println("findmeee convert to indexed row matrix")
        val rmat = cmat.toIndexedRowMatrix()
        //val mat = cmat.toRowMatrix()


        var svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = rmat.computeSVD(20, computeU = true)
        var U: IndexedRowMatrix = svd.U
        var s: Vector = svd.s
        var V: Matrix = svd.V

        dat.unpersist()

        var iter = 0
        val numIterations = 5
        var smat: Matrix = Matrices.diag(s)
        var newmat = U.multiply(smat).multiply(V.transpose)      
        //newmat.cache()

        while(iter < numIterations){

            println("findmeee":+iter)
            svd = newmat.computeSVD(20, computeU = true)
            U = svd.U
            s = svd.s
            V = svd.V 
            smat = Matrices.diag(s)
            newmat = U.multiply(smat).multiply(V.transpose)    
            iter+=1  
        }

        //val outmat = newmat.rows.collect
        println("findmeee collecting rows")
        val newmatrows = newmat.rows.collect

        

        //outmat(x(0).toInt)(x(1).toInt)
        //newmat.rows.filter(r => r.index == x(0)).collect

        //Source.fromFile(missingfile).getLines.map(line => line.split(",")).toList.map(x => output.write(x(0)+","+x(1)+","+outmat(x(0).toInt)(x(1).toInt)+"\n"))
        //Source.fromFile(missingfile).getLines.map(line => line.split(",")).toList.map(x => output.write(x(0)+","+x(1)+","+newmatrows.filter(r => r.index == x(0).toInt)(0).vector(x(1).toInt)+"\n"))
        sc.textFile(missingfile).map(line => line.split(",")).collect.map(x => output.write(x(0)+","+x(1)+","+newmatrows.filter(r => r.index == x(0).toInt)(0).vector(x(1).toInt)+"\n"))
        //newmat.unpersist()
        output.close()

        /*
        for (line <- Source.fromFile(missingfile).getLines.map(line => line.split(",")).toList.map(x => (x(0),x(1),outmat(x(0).toInt)(x(1).toInt)))){
            println(line._1)
            output.write(line._1+","+line._2+","+line._3+"\n")
        }
        output.close()
        */  
        
        System.exit(0) 
    }
}
