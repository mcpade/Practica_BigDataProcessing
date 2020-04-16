package ProcesoEspia2
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/*Para esta práctica he decidido separar la parte de procesamiento de los mensajaes de la red social,
de la parte de análisis de relaciones de los usuarios. He creado por tanto dos paquetes diferentes
 con sus correspondientes clases objeto
 */
/**************************** Procesamiento Palabras ********************************************/


object ProcesoEspia2App {

  /* 1.- En este objeto ProcesoEspia2App voy a ir analizando los mensajes de la red social Celebram
  que me llegan gracias a una red de IOTs. Para simular este envío de datos de los IOTs utilizo
  el fichero "mensajes.csv" que he dejado en la carpeta data del proyecto, pero que realmente
  no forma parte del proyecto, sino que está aquí para que se guarde en algún otro directorio y
  se utilice desde el cliente en linea de comandos Kafka para, mandarlo como mensajes hacia
  el cluster Kafka:

  >bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic celebram
  > bin/kafka-console-producer.sh --broker-list localhost:9092 --topic celebram < /Documents/mensajes.csv


  Otra opción hubiera sido tener un fichero de mensajes por cada dispositivo iot pero he decidido
  mejor tener un único fichero y meter el campo Iot_Id para identificar de donde viene el mensaje

  El análisis se realiza en ventanas de 1 hora, por lo que durante ese tiempo se puede simular
  la recepción de los diferentes mensajes haciendo un envío del fichero "mesajes.csv" varias veces
  durante esa hora.


  2.- La estructura del fichero de mensajes es:
  Mensaje_Id,Iot_Id,Usuario_Origen_Id,Usuario_Destiono_Id,Contenido

  He introducido el Usuario Origen y Destino por si se necesitara hacer algún procesamiento adicional
  en base a relaciones


  3.- Un segundo fichero que utilizaré será el fichero "iot.csv" que realmente para el procesamiento de palabras
  no hace falta, pero lo voy a unir a la fuente de datos de mensajes por si a futuro hiciera falta
  algún procesamiento adicional

  La estructura de ese fichero es:
  IoT_Id,Encendido,Zona_Id

  4.- El tercer fichero utilizado es "usuarios.csv" que tampoco se necesita para el procesamiento de palabras pero
  igualmente voy a unir a la fuente pricipal por si hicera falta para un futuro procesamiento
  La estrucutra de este fichero es:
  User_Id,Nombre,Apellido,Edad,Sexo,Username


  El objetivo es hallar por hora las 10 palabras mas usadas y, caso de que la palabra más repetida
  coincida con alguna de las palabras de una lista negra, se mande una notificación al ministro


  NOTA1: En el sbt he utilizado las versiones 2.4.0 debido a que he querido utilizar .foreachBath para el WriteStream
  y según leí en la documentación se añadió en la versión 2.4. Sé que se comentó que podía tener algún bug pero de
  momento para lo que he estado haciendo parece funcionar bien.

  NOTA2: Los mensajes del fichero mensajes.csv están puestos para que la palabra mas repetida sea tramaX que está dentro
  de la lista negra, por lo que se mandaría el aviso.

  NOTA 3: La ventana temporal de consulta es de 1 hora y así está configurado, pero el trigger está puesto
  cada 20 segundos, aunque también tendría que ser 1 hora. Así para la revisión de la práctica se puede ver el resultado antes.
  He dejado la línea de los 3600 segundos comentada y esa sería la que tendría que ir en la versión de producción.

  //.trigger(Trigger.ProcessingTime("30 second"))
  trigger(Trigger.ProcessingTime("3600 second"))

  NOTA4: Estoy mostrando como añadido la estructura de los mensajes capturados junto con los datos de los iots y de usuarios.
  Esto tiene puesto un trigger de 10 segundos. Simplemente lo he dejado para la revisión de la estructura. Es algo que se quitaría
  en la versión de producción

*/

  /***************************** Procesos adicionales ***********************************/

  def Desencriptar (mensaje:String) ={

    //Aquí suponemos que tendríamos el algoritmo de desencriptado de los mensajes de la red social.
    //En mi caso como es simulado no hago nada y devuelvo el mensaje tal cual
    mensaje

  }


  def pasearMensaje (mensaje:(String,Timestamp)) ={

    //Por claridad en el código me he traido fuera el proceso para pasear los mensajes recibidos de los iots.
    //La idea es separar por "," el mensaje y tener al final en formato tupla los diferentes campos junto con el
    //Timestamp (que lo recibimos también como entrada en este proceso)

    val campos = mensaje._1.split(",")
    val id_mensaje = campos (0)
    val id_iot = campos (1)
    val id_usuario_origen = campos(2)
    val id_usuario_destino = campos (3)
    val mensajeEncriptado = campos (4)
    //El mensaje está encriptado y tendríamos que tener una función para desencriptar que
    //es una proceso simulado
    val contenido = Desencriptar(mensajeEncriptado)
    val timestamp = mensaje._2

    (id_mensaje, id_iot, id_usuario_origen, id_usuario_destino, contenido, timestamp )


  }

  def ProcedimientoConLasDiezPalabras (lasdiezpalabras:DataFrame) = {

    //El enunciado no especifica que hacer con las 10 palabras que encontremos más repetidas.
    //Se podrían almacenar en ficheros, mandar a una base de datos, a otro kafka.... en mi caso simplemente
    //las enseñaré por consola, pero he decidido sacar este proceso fuera para dar la idea de que aquí trabajaríamos
    //con esas 10 palabras para enviarlas al sitio especifiado

    //Además de las palabras muestro tambíen la ventana temporal y el conteo

    lasdiezpalabras.show()

  }

  def AvisarMinistro (palabra:String) = {

    //Este sería el proceso para notificar al ministro de que la palabra más repetida en esta hora de análisis está
    //en la lista negra de palabras. Como será un sistema de notificación simulado simplemente muestro el siguiente
    //mensaje en la consola
    println (s"Atención: palabrá más repetida en la última hora dentro de la lista negra. Palabra: ${palabra}")
  }


/********************************** PRINCIPAL **************************************************************/

  def main(args: Array[String]): Unit = {



    /**************** Definición de esquema de datos **********/


   //Defino el esquema de los dispositivos IOT
    val esquemaDispositivosIOT = new StructType()
      .add("Id_Iot","string")
      .add("Encendido","boolean")
      .add ("Zona_Id","string")

    //El campo Id_Iot estará relacionado con el campo Iot_Id que venga con el mensaje

    /*Defino el esquema de datos para los usuarios registrados. Con respecto al esquema propuesto en el enunciado de
      la práctica he añadido un campo UserName que considero que es único para cada usuario y que utilizaré
      para sacar la información de las relaciones en la parte de GraphX*/

    val esquemaUsuariosRegistrados = new StructType()
      .add("User_Id","long")
      .add ("Nombre","string")
      .add ("Apellido","string")
      .add ("Edad","integer")
      .add("Sexo","string")
      .add("UserName", "String")

    //Con el User_Id enlaza con los mensajes que llevan un ID de Usuario Origen

    //Listado de palabras negras. Esto será una estructura de scala
    //Supongo que ABCD es el nombre del partido más importante de la oposición al gobierno
    // y que "tramaX" sea algún caso judicial que afecte al gobierno :)

    val listaNegraPalabras = List ("impuestos","pensiones","salario","precios","abcd", "tramax")

    //Aquí defino una lista /diccionario de palabras que no me interesa contar.
    //En el proceso voy a filtrar por palabras mayores de 3 letras y además aquí podría
    //añadir ese diccionario de palabras a no tener en cuenta.
    //Esta sería una primera aproximación donde quito las preposiciones, conjunciones, adverbios..

    val palabrasQueNoCuentan = List ("ante", "bajo", "cabe", "contra", "desde", "entre", "hacia"
      ,"hasta", "para", "segun", "unos", "unas", "sino", "tanto", "como", "igual", "mismo", "pero"
      , "mientras", "bien", "mal", "fuera", "porque", "aunque", "ahora",  "siempre",  "ahora", "dado"
      , "puesto", "visto", "cuando", "menos", "salvo", "cada", "luego", "conque", "tanto", "como"
      , "donde", "cuanto", "todo", "mejor", "peor", "nunca", "tienes", "eres", "soy", "solo", "tiene"
      , "antes", "despues", "tarde", "luego", "ayer", "todavia", "pronto", "aqui", "alli", "cerca"
      , "lejos", "dentro", "aparte", "alrededor", "encima", "debajo", "delante", "detras", "mucho"
      , "poco", "casi", "nada", "algo", "medio", "demasiado", "bastante", "menos", "ademas", "incluso"
      , "tambien", "tampoco", "jamas", "acaso", "quizas")



    Logger.getLogger("org").setLevel(Level.ERROR)  //Se utiliza para evitar que salga mucha información en consola.
                                                          //Solo salen los errores


    /*************************** Spark *****************************/

    //Levanto una SparkSession. Lo hago en local

    val spark = SparkSession
      .builder()
      .appName("Procesamiento espia")
      .master("local[*]")
      .getOrCreate()

    /*************************** Ingesta de datos ********************************/

    //Ingesto los datos de los iots desde el fichero csv "iot.csv" y lo cargo en un DataFrame usando
    // el esquema que he definido antes esquemaDispositivosIOT

    val dataIot = spark.read.format("csv")
      .option("header",true)  //descarto la linea de encabezado
      .schema(esquemaDispositivosIOT)
      .load("data/iot.csv")


    //Ingesto los datos de los usuarios desde el fichero csv "usuarios.csv" y lo cargo en un DataFrame usando
    // el esquema que he definido antes esquemaUsuariosRegistrados
    val dataUsuarios = spark.read.format("csv")
      .option("header",true) //descarto la linea de cabecera
      .schema(esquemaUsuariosRegistrados)
      .load("data/usuarios.csv")


    //Streaming KAFKA --> lectura de los mensajes que nos generan los iots en tiempo real
    //Kafka Source
    import spark.implicits._
    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","celebram")     //tipo de topic al que me suscribo. Este topic se tiene que estar creado en
                                          //Kafka
      .option("includeTimestamp", true)   //incluyo huella temporal
      .load()
      .selectExpr("CAST(value AS STRING)", "timestamp")
      .as[(String,Timestamp)]     //Le añado al mensaje, el timestamp

    //Aquí me creo el DF de los mensajes capturados. En mi procedimiento de parseo voy a añadir el timestamp
    //He sacado el parseo fuera porque como son bastantes campos se ve más claro en un proceso aparte


    val dataMensajes = data.map(pasearMensaje).toDF("Mensaje_Id", "Iot_Id", "Usuario_Origen_Id", "Usuario_Destino_ID","Contenido", "Timestamp")
    //Tengo ya mi dataMensajes como un Dataframe con sus columnas y se ha añadido la columna del timestamp

    /********************************** Unión de datos ***********************************/

    //A continuación voy a unir con la tabla de IOTs a través del campo Iot_Id
    //Realmente para el procesamiento posterior de palabras no nos van a hacer falta los campos relacionados con
    //los IOTs pero los tengo aquí añadidos por si en el futuro hiciera falta hacer algún procesamiento sobre
    //ellos


    val joinExpression = dataMensajes.col("Iot_Id")=== dataIot.col ("Id_Iot")
    val dataMensajesCapturados1 = dataMensajes.join(dataIot,joinExpression,"left_outer")


    //Voy a unir también con la tabla de Usuarios a través del capo User_Id y User_Origen_Id
    //Realmente para el procesamiento posterior de palabras no nos van a hacer falta los campos relacionados con
    //los usuarios pero los tengo aquí añadidos por si en el futuro hiciera falta hacer algún procesamiento sobre
    //ellos

    val joinExpression2 = dataMensajes.col("Usuario_Origen_Id")=== dataUsuarios.col ("User_Id")
    val dataMensajesCapturados2 = dataMensajesCapturados1.join(dataUsuarios,joinExpression2,"left_outer")

    //Me quedo con los campos mas representativos para luego mostrarlos
    val dataMensajesCapturados = dataMensajesCapturados2.select($"Mensaje_Id", $"Iot_Id", $"Usuario_Origen_Id", $"Contenido", $"Timestamp", $"Zona_Id", $"UserName" )

    /**********************************+ Procesamiento de palabras *************************/

    //Para el procesamiento de palabras me voy a quedar solo con la columna Contenido y la de Timestamp

    val dataSoloMensaje = dataMensajesCapturados.select("Contenido", "Timestamp")

    //Ahora tendría que separar el "Contenido" por palabras y a cada palabra añadirle su Timestamp

    val MensajeTime = dataSoloMensaje.map {
      case Row (val1:String, val2:Timestamp) =>
        (val1,val2)
    }

    val palabras = MensajeTime
        .flatMap(par => par._1.split("\\W+")  //Separo las palabras
        .map( palabra => palabra.toLowerCase )    //Paso todas las palabras a minúscula
        .filter(palabra => palabra.length >3)     //Me quedo con las palabras de más de 3 letras para descartar artículos,
                                                  //conjunciones...
        .filter(palabra => !palabrasQueNoCuentan.contains(palabra)) //Me quedo además con aquellas palabras no contenidas en la
                                                                    // lista de preposiciones, conjunciones, etc
        .map(palabra => (palabra, par._2)))   //Añado el timestamp
        .toDF("palabra", "timestamp") //Lo convierto de nuevo a DF


    //Agrupo por ventana temporal de 1 hora y por palabras,
    //cuento y ordeno en orden descendente

    val conteoPalabras = palabras
      .groupBy(window ($"timestamp", "3600 second", "3600 second"),$"palabra")
      .count ()
      .sort((desc("count")))


    /*********************************** Streaming Queries *******************************/


    //Lanzo ahora la query con un trigger de 1 hora y sobre el resultado necesito hacer un procesamiento para obtener
    //la palabra más repetida.
    //Utilizo como sink de salida el .foreachBath que permite realizar operaciones sobre el
    //Dataframe. Para poder usar ese método he tenido que subir a Spark 2.4.0 ya que según leí en la
    //doc fue algo que se añadió en esa versión. Utilizando la 2.3.4 no tenía ese método .foreachBatch
    //He subido todas las demás y parece funcionar


    val query = conteoPalabras.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("20 second")) //Para ver el resultado y no tener que esperar una hora
      //.trigger(Trigger.ProcessingTime("3600 second"))   //La linea que tendría que ir en la versión definitiva

      .foreachBatch {(conteoPalabras:DataFrame, bachID: Long) =>

        //Voy a comprobar que el dataframe no esté vacio ya que si hace el procesamiento sobre un
        //dataframe vacio da un error "next on empty iterator"
        if (!conteoPalabras.take(1).isEmpty) {

          //Voy a obtener las 10 palabras más repetidas. Como lo tengo ordenado por conteo, me quedo con las 10 primeras
          val diezpalabras = conteoPalabras.limit(10)
          //Me podría quedar solo con la columna "palabras" pero si quiero mostrarlo después queda mejor con
          // la ventana de tiempo y el conteo

          //El enunciado no dice que hacer con esas palabras pero simulo un proceso que se encargue
          ProcedimientoConLasDiezPalabras (diezpalabras)


          //Ahora voy a buscar la palabra más repetida

          val palabraMasRepetida = conteoPalabras.select("palabra").head.get(0) //Al estar ordenado es la primera que tengo


          //Compruebo si la palabra más repetida está en la lista negra de palabras
          if (listaNegraPalabras.contains(palabraMasRepetida)) {
            //Este sería el procedimiento de notificación que en mi caso es simulado y simplemente
            //imprimirá por consola el aviso
            AvisarMinistro(palabraMasRepetida.toString())
          }
        }

      }.start()

    //Aquí simplemente he añadido que se muestre también como queda la tabla compuesta por los mensajes y los datos
    //de los iots. No lo pide el enunciado pero por ver que queda bien la estructura. Solo lo muestro por consola
    //Se quitaría en la versión de producción
    val query2 = dataMensajesCapturados.writeStream
      //.outputMode("complete")
      .trigger(Trigger.ProcessingTime("10 second"))
      .format("console")
      .start()

    query.awaitTermination()
    query2.awaitTermination()


  }


}
