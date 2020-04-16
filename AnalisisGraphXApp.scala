package AnalisisGraphX

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.StructType

import scala.reflect.io.File


/*Para esta práctica he decidido separar la parte de procesamiento de los mensajaes de la red social,
de la parte de análisis de relaciones de los usuarios. He creado por tanto dos paquetes diferentes
 con sus correspondientes clases objeto
 */
/**************************** GraphX ********************************************/

/*En este  objeto AnalisisGraphXApp voy a obtener las relaciones existentes entre los usuarios de la
red social.
Para ello tomo como entradas dos ficheros que tiene el gobierno y que están incluidos en el directorio
data creado para el proyecto:
- usuarios.csv: Contiene los datos de los usuarios de la red social y se esctructura así
                id,Nombre,Apellido,Edad,Sexo,Username
- relaciones.csv: Contiene datos sobre las relaciones conocidas y se estructura así
                Origen_Id,Destino_Id,Tipo_Conexion

Posteriormente en el código se verán los tipos de datos de cada estructura

Una vez tengo los datos de entrada utilizaré GraphX para crear un grafo sobre el que extraeré
la información de las relaciones usando los algoritmos Connected Components y  PageRank

Finalmente los resultados se guardarán en ficheros txt en el directorio Output.

 */


object AnalisisGraphXApp {
  def main(args: Array[String]): Unit = {


    /**************** Definición de esquema de datos **********/

    /*Defino el esquema de datos para los usuarios registrados. Con respecto al esquema propuesto en el enunciado de
      la práctica he añadido un campo UserName que considero que es único para cada usuario y que utilizaré
      para sacar la información de las relaciones*/

    val esquemaUsuariosRegistrados = new StructType()
      .add("User_Id","long")
      .add ("Nombre","string")
      .add ("Apellido","string")
      .add ("Edad","integer")
      .add("Sexo","string")
      .add("UserName", "String")

    /*Defino el esquema de las relaciones. Los camos Origen_Id y Destino_Id estarán relacionados con el campo
      User_Id del esquema de UsuariosRegistrados. De esta forma se relacionan ambas estructuras de datos*/

    val esquemaRelaciones = new StructType()
      .add("Origen_Id","long")
      .add("Destino_Id","long")
      .add ("Tipo_Conexion","string")

    Logger.getLogger("org").setLevel(Level.ERROR)  //Se utiliza para evitar que salga mucha información en consola.
                                                          //Solo salen los errores


    /************ Spark *************/
    //Levanto una SparkSession. Lo hago en local

    val spark = SparkSession
      .builder()
      .appName("Analisis relaciones")
      .master("local[*]")
      .getOrCreate()

    /************ Ingesta de datos ***********/

    //Ingesto los datos de usuario desde el fichero csv "usuarios.csv" y lo cargo en un DataFrame usando el esquema
    //que he definido antes esquemaUsuariosRegistrados

    val dataUsuarios = spark.read.format("csv")
      .option("header",true) //descarto la linea de cabecera
      .schema(esquemaUsuariosRegistrados)
      .load("data/usuarios.csv")

    //Ingesto los datos de las relaciones de usuarios desde el fichero csv "relaciones.csv" y lo cargo en un DataFrame
    //usando el esquema que he definido antes esquemaRelaciones

    val dataRelaciones  = spark.read.format("csv")
      .option("header",false)
      .schema(esquemaRelaciones)
      .load("data/relaciones.csv")


    /************ Creación de un grafo***********/

    val sc: SparkContext = spark.sparkContext

    //Creo el RDD de conexiones partiendo del dataframe anterior "dataRelaciones"

    val conexiones:RDD[Edge[String]] = dataRelaciones.rdd.map(row => {
      Edge(row.getAs[Long]("Origen_Id"), row.getAs[Long]("Destino_Id")
        , row.getAs[String]("Tipo_Conexion"))
    })

    //Creo el RDD de vértices partiendo del dataframe que he creado antes de usuarios "dataUsuarios"

    val vertices:RDD[(VertexId, String)] = dataUsuarios.rdd.map(row => {
      ( row.getAs[Long]("User_Id"), row.getAs[String]("UserName"))

    })

    //Creo un usuario por defecto para el caso de que haya relaciones sin vértices creados

    val defaultUser = "Missing"

    //Construyo el grafo
    val grafo = Graph (vertices, conexiones,defaultUser)

    //Obtengo ahora un subgrafo en el que voy a eliminar todos los vertices marcados como Missing
    //así como todas sus conexiones

    val grafovalido = grafo.subgraph(vpred = (id, attr)=> attr != "Missing")

    /***************** Analisis de las relaciones ************/

    //Para ver las relaciones en Celebram uso el algoritmo Connected Components

    //Busco los componentes conectados
    val cc = grafovalido.connectedComponents().vertices

    //Uno los componentes conectados con los UserName de los usuarios

    val users = dataUsuarios.rdd.map(row => {
      ( row.getAs[Long]("User_Id"), row.getAs[String]("UserName"))

    })

    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }

    //Para que no de error si se ejecuta varias veces este objeto, voy a comprobar si existe el directorio output
    //donde almaceno los resultados y si existe borro su contenido. Al intentar sobreescribir un fichero da un error
    //por fichero ya existente

    val directorio = File("output/")
    if (directorio.isDirectory) {
      directorio.deleteRecursively()
    }

    //Guardo el resultado en el fichero resultadoRelaciones.txt dentro de la carpeta output

    ccByUsername.saveAsTextFile("output/resultadoRelaciones.txt")


    //Voy también a aplicar el algoritmo PageRank
    val ranks = grafovalido.pageRank(0.0001).vertices
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    import spark.implicits._
    //Voy a ordenar el resultado obtenido en forma de tupla por el segundo campo. De esta forma tengo ordenados de mayor a menor
    // los usuarios con mas relaciones. Para ello lo paso a Dataframe y lo ordeno por la columna rank
    val rankPage = ranksByUsername.toDF ("usuario", "rank").coalesce(1).sort(desc("rank")).rdd

    //El resultado obtenido lo guardo en el fichero "PageRank" en la carpeta output
    rankPage.saveAsTextFile("output/PageRank.txt")

    /***************************************************************/

  }
}
