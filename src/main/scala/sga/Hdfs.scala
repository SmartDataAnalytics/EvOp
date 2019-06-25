//package ClassicalGA
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
// 
//object Hdfs extends App {
// 
//  def write(uri: String, filePath: String, data: Array[Byte]) = {
//    System.setProperty("HADOOP_USER_NAME", "hduser")
//    val path = new Path(filePath)
//    val conf = new Configuration()
//    conf.set("fs.defaultFS", uri)
//    val fs = FileSystem.get(conf)
//    val os = fs.create(path)
//    //os.write(data)
//    fs.close()
//  }
//}