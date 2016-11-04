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
import breeze.linalg._
import breeze.numerics._

 
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

        //val missinginds = sc.textFile(missingfile).map(line => line.split(",")).map(x => (x(0).toInt,x(1).toInt, 0.0))
        //val missingindsdummy = sc.textFile(missingfile).map(line => line.split(",")).map(x => (x(0).toLong,x(1).toLong) -> 0.0)
        //missingindsdummy.cache()
        val missingindsdummy2 = sc.textFile(missingfile).mapPartitions(iter => {
            var res = collection.mutable.ArrayBuffer.empty[((Long, Long), Double)]
            iter.foreach(entry => {
                //println(entry)
                var tmp = entry.split(",")
                res +=  (((tmp(0).toLong,tmp(1).toLong),0.0))
                })
            res.iterator
            }).cache()

        //val md2 = sc.broadcast(missingindsdummy2)
        
        //val missingindscoll = missinginds.collect

        //val cmat = new CoordinateMatrix(sc.parallelize(Source.fromFile(datafile).getLines().map(line => line.split(",")).toList.map(x => MatrixEntry(x(0).toLong,x(1).toLong,x(2).toDouble))))

        println("findmeee start code")
        println("findmeee filename "+datafile)
        //val dat = sc.textFile(datafile).map(line => line.split(",")).map(x => MatrixEntry(x(0).toLong,x(1).toLong,x(2).toDouble))                
        //val dat = sc.textFile(datafile).map(line => line.split(",")).map(x => (x(0).toInt,x(1).toInt,x(2).toDouble))
        val dat2 = sc.textFile(datafile).mapPartitions(iter => {
            var res = collection.mutable.ArrayBuffer.empty[MatrixEntry]
            iter.foreach(entry => {
                //println(entry)
                var tmp = entry.split(",")
                res +=  (MatrixEntry(tmp(0).toInt,tmp(1).toInt,tmp(2).toDouble))
                })
            res.iterator
            })
        //val fulldat = dat.union(missinginds)
        //var fullcmat = new CoordinateMatrix(fulldat.map(x => MatrixEntry(x._1,x._2,x._3)))
        //var fullrowmat =  fullcmat.toRowMatrix
        //dat.cache()
        
        println("findmeee convert to coordinate matrix")
        println("findmeee"+Calendar.getInstance.getTime())
        //var cmat = new CoordinateMatrix(dat2.map(x => MatrixEntry(x._1,x._2,x._3)))      
        var cmat = new CoordinateMatrix(dat2)      
        println("findmeee"+Calendar.getInstance.getTime())

        println("findmeee convert to indexed row matrix")
        println("findmeee"+Calendar.getInstance.getTime())
        var rmat = cmat.toIndexedRowMatrix()        
        println("findmeee"+Calendar.getInstance.getTime())
        rmat.rows.cache()

        println("findmeee svd run 1 start")
        println("findmeee"+Calendar.getInstance.getTime())
        
        val ksvd = {
            if(datafile == "hdfs://noc-n063.csb.pitt.edu:9000/large.csv"){                
                25
            } else if(datafile == "hdfs://noc-n063.csb.pitt.edu:9000/medium.csv"){
                159
            }else{
                24
            }
        }
        val numIterations = {
            if(datafile == "hdfs://noc-n063.csb.pitt.edu:9000/large.csv"){                
                3
            } else if(datafile == "hdfs://noc-n063.csb.pitt.edu:9000/medium.csv"){
                6
            }else{
                10
            }
        }   

        println("findmeee ksvd "+ksvd)
        println("findmeee numIterations "+numIterations)

        var svd: SingularValueDecomposition[IndexedRowMatrix, org.apache.spark.mllib.linalg.Matrix] = rmat.computeSVD(ksvd, computeU = true)
        var U: IndexedRowMatrix = svd.U
        var smat: org.apache.spark.mllib.linalg.Matrix = Matrices.diag(svd.s)
        var V: org.apache.spark.mllib.linalg.Matrix = svd.V
        println("findmeee svd run 1 end")
        println("findmeee"+Calendar.getInstance.getTime())
        rmat.rows.unpersist()    

        //dat.unpersist()        
        var newmat = U.multiply(smat).multiply(V.transpose) 

        //var reconstructedmat = newmat.rows.map(r => r.index -> r.vector).join        

        var iter = 0
        //val numIterations = 10    
        

        var lsqerr = 0.0
        var lsqerrbr = sc.broadcast(lsqerr)
              

        while(iter < numIterations){
            println("findmeee"+iter)
            println("findmeee"+Calendar.getInstance.getTime())
            lsqerr = 0.0

            /*
            var recmatrows = newmat.rows.map(r => r.index -> r.vector).join(rmat.rows.map(x => x.index -> x.vector)).mapPartitions(iter => {
                var res = collection.mutable.ArrayBuffer.empty[IndexedRow]
                iter.foreach(entry => {
                    var v1 = breeze.linalg.DenseVector(entry._2._1.toArray)
                    var v2 = breeze.linalg.DenseVector(entry._2._2.toArray)                
                    res += IndexedRow(entry._1, Vectors.dense((v1:*(-I(v2)):+v1+v2).toArray))
                    var diff = v1:*(-I(v2)):+v2
                    lsqerr += sum(diff:*diff)
                    })
                res.iterator
            }).cache()*/

            var recmatrowslsqerr = newmat.rows.map(r => r.index -> r.vector).join(rmat.rows.map(x => x.index -> x.vector)).mapPartitions(iter => {
                var res2 = collection.mutable.ArrayBuffer.empty[(Double,Array[IndexedRow])]
                var res = collection.mutable.ArrayBuffer.empty[IndexedRow]
                iter.foreach(entry => {
                    var v1 = breeze.linalg.DenseVector(entry._2._1.toArray)
                    var v2 = breeze.linalg.DenseVector(entry._2._2.toArray)                
                    res += IndexedRow(entry._1, Vectors.dense((v1:*(-I(v2)):+v1+v2).toArray))
                    var diff = v1:*(-I(v2)):+v2
                    lsqerr += sum(diff:*diff)                    
                    })
                res2 += ((lsqerr,res.toArray))
                res2.iterator
            })

            var recmatrows = recmatrowslsqerr.flatMap(r => r._2).cache()
            lsqerr = recmatrowslsqerr.map(r => r._1).collect.sum
            if(lsqerr < 0.01){
                iter=100
            }
            println("findmeee lsqerr"+lsqerr)

            var recmat = new IndexedRowMatrix(recmatrows)
            svd = recmat.computeSVD(ksvd, computeU = true)
            U = svd.U
            smat = Matrices.diag(svd.s)
            V = svd.V             
            recmatrows.unpersist()
            newmat = U.multiply(smat).multiply(V.transpose)
            iter+=1
            
        }



        //var newmatc = newmat.toCoordinateMatrix.entries.cache()
        //var newmatcRow = newmat.toCoordinateMatrix.entries.cache().map(r => (r.i,r.j) -> r.value)        
        println("findmeee Final Missing start")
        println("findmeee"+Calendar.getInstance.getTime())   
        var newmatcRow = newmat.toCoordinateMatrix.entries.mapPartitions(iter => {
            var res = collection.mutable.ArrayBuffer.empty[((Long,Long),Double)]
            //val missinginds = md2.value
            iter.foreach(r => {
                res += (((r.i,r.j),r.value))
            })
            res.iterator
        }).cache()
        //var newmatcRowMiss = missingindsdummy2.keyBy(r => r).join(newmatcRow)
        var newmatcRowMiss = missingindsdummy2.join(newmatcRow)
        println("findmeee Final Missing end")
        println("findmeee"+Calendar.getInstance.getTime())                
        //dat2.unpersist()

        

        //newmat.rows.cache()
        //var newmatrows = newmat.rows.collect     
        //println("collected reconstructed matrix")

        //var missingdat = missinginds.map(x => (x._1,x._2,newmatrows.filter(r => r.index == x._1)(0).vector(x._2)))
        /*
        var reconstructeddat = dat.union(missingdat)        
        reconstructeddat.cache()
        println("findmeee"+Calendar.getInstance.getTime()())

        cmat = new CoordinateMatrix(reconstructeddat.map(x => MatrixEntry(x._1,x._2,x._3)))
        rmat = cmat.toIndexedRowMatrix()

        var iter = 0
        val numIterations = 10       
              

        while(iter < numIterations){
            println("findmeee"+iter)
            println("findmeee"+Calendar.getInstance.getTime()())
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
        println("findmeee outputting")
        println("findmeee"+Calendar.getInstance.getTime())  
        newmatcRowMiss.collect.foreach(x => output.write(x._1._1+","+x._1._2+","+x._2._2+"\n"))
        output.close()  
        missingindsdummy2.unpersist()
        newmatcRowMiss.unpersist()
        println("findmeee end code")
        println()
        println()
        println()
        //sc.textFile(missingfile).map(line => line.split(",")).collect.map(x => output.write(x(0)+","+x(1)+","+newmatrows.filter(r => r.index == x(0).toInt)(0).vector(x(1).toInt)+"\n"))
        
             
        System.exit(0) 
    }
}
