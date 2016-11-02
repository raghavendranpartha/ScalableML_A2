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
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import java.util.Calendar
 
object Assign2 {    

    def main(args: Array[String]) 
    {
        val conf = new SparkConf().setAppName("Assign2")
        val sc = new SparkContext(conf)
        val datafile = args(0)
        val missingfile = args(1)
        val outfile = args(2)
        
        /*
        val datafile = "large.csv"
        val missingfile = "large_missing.csv"
        
        val datafile = "medium.csv"
        val missingfile = "medium_missing.csv"
        val outfile = "medium_out.csv"

        val datafile = "small.csv"
        val missingfile = "small_missing.csv"
        val outfile = "small_out.csv"
        */
        val ofile = new File(outfile)
        val output = new BufferedWriter(new FileWriter(ofile))

        //val missinginds = sc.textFile(missingfile).map(line => line.split(",")).map(x => (x(0).toLong,x(1).toLong))
        val missingindsdummy = sc.textFile(missingfile).map(line => line.split(",")).map(x => (x(0).toLong,x(1).toLong) -> 0.0)
        missingindsdummy.cache()
        //val missingindscoll = missinginds.collect

        //val cmat = new CoordinateMatrix(sc.parallelize(Source.fromFile(datafile).getLines().map(line => line.split(",")).toList.map(x => MatrixEntry(x(0).toLong,x(1).toLong,x(2).toDouble))))

        println("findmeee start code")
        println("filename "+datafile)
        //val dat = sc.textFile(datafile).map(line => line.split(",")).map(x => MatrixEntry(x(0).toLong,x(1).toLong,x(2).toDouble))                
        val dat = sc.textFile(datafile).map(line => line.split(",")).map(x => (x(0).toInt,x(1).toInt,x(2).toDouble))
        //dat.cache()
        
        println("findmeee convert to coordinate matrix")
        println(Calendar.getInstance.getTime())
        var cmat = new CoordinateMatrix(dat.map(x => MatrixEntry(x._1,x._2,x._3)))      
        println(Calendar.getInstance.getTime())

        println("findmeee convert to indexed row matrix")
        println(Calendar.getInstance.getTime())
        var rmat = cmat.toIndexedRowMatrix()
        println(Calendar.getInstance.getTime())
        rmat.rows.cache()

        println("findmeee svd run 1 start")
        println(Calendar.getInstance.getTime())
        var svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = rmat.computeSVD(20, computeU = true)
        var U: IndexedRowMatrix = svd.U
        var smat: Matrix = Matrices.diag(svd.s)
        var V: Matrix = svd.V
        println("findmeee svd run 1 end")
        println(Calendar.getInstance.getTime())

        rmat.rows.unpersist()

        //dat.unpersist()

        println(Calendar.getInstance.getTime())        
        var newmat = U.multiply(smat).multiply(V.transpose) 
        //var newmatc = newmat.toCoordinateMatrix.entries.cache()
        var newmatcRow = newmat.toCoordinateMatrix.entries.cache().map(r => (r.i,r.j) -> r.value)        
        var newmatcRowMiss = newmatcRow.join(missingindsdummy)

        



        //newmat.rows.cache()
        //var newmatrows = newmat.rows.collect     
        //println("collected reconstructed matrix")

        //var missingdat = missinginds.map(x => (x._1,x._2,newmatrows.filter(r => r.index == x._1)(0).vector(x._2)))
        /*
        var reconstructeddat = dat.union(missingdat)        
        reconstructeddat.cache()
        println(Calendar.getInstance.getTime())

        cmat = new CoordinateMatrix(reconstructeddat.map(x => MatrixEntry(x._1,x._2,x._3)))
        rmat = cmat.toIndexedRowMatrix()

        var iter = 0
        val numIterations = 10       
              

        while(iter < numIterations){
            println("findmeee"+iter)
            println(Calendar.getInstance.getTime())
            svd = rmat.computeSVD(20, computeU = true)
            U = svd.U
            smat = Matrices.diag(svd.s)
            V = svd.V             
            reconstructeddat.unpersist()
            newmat = U.multiply(smat).multiply(V.transpose)    
            newmatrows = newmat.rows.collect     
            missingdat = missinginds.map(x => (x._1,x._2,newmatrows.filter(r => r.index == x._1)(0).vector(x._2)))            
            reconstructeddat = dat.union(missingdat)                   
            reconstructeddat.cache()  
            cmat = new CoordinateMatrix(reconstructeddat.map(x => MatrixEntry(x._1,x._2,x._3)))
            rmat = cmat.toIndexedRowMatrix()                      
            iter+=1  
        }

        reconstructeddat.unpersist()            
        */
        //missingdat.collect.map(x => output.write(x._1+","+x._2+","+x._3+"\n"))        
        newmatcRowMiss.collect.map(x => output.write(x._1._1+","+x._1._2+","+x._2._1+"\n"))
        output.close()  
        missingindsdummy.unpersist()
        newmatcRowMiss.unpersist()
        
        //sc.textFile(missingfile).map(line => line.split(",")).collect.map(x => output.write(x(0)+","+x(1)+","+newmatrows.filter(r => r.index == x(0).toInt)(0).vector(x(1).toInt)+"\n"))
        
             
        System.exit(0) 
    }
}
