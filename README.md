# Práctica: Módulo Big Data Processing - Bootcamp KeepCoding - BIG DATA & MACHINE LEARNING

# Big Data Processing

En esta práctica se utilizará el lenguaje de programación **Scala**, el motor **Spark** y el entorno de desarrollo IDE **IntelliJ** 

El fichero practica_espia.zip contiene todo el proyecto generado desde el IDE IntelliJ.

Conceptos que se tratan en esta práctica

- SQL and Dataframes
- Streaming. **Kafka**
- GraphX - Gestión de datos almacenados como grafos (redes sociales)


# Enunciado de la práctica

### Contexto:
Siglo XXIII, los ciudadanos y sus comunicaciones son espiados por el Gobierno de un país un tanto particular.

### Introducción:

En secreto, instituciones ocultas del gobierno de Cloacalandia espían desde hace tiempo a los ciudadanos de las grandes ciudades del país. Estos usuarios utilizan una red social llamada Celebram enviando mensajes a sus conocidos y familiares. Los mensajes son cifrados por la red social, pero esto no es problema para los hackers del departamento del ministerio ya que han diseñado un algoritmo que es capaz de descifrar todos los mensajes. Una vez se envían, son ‘esnifados’ por dispositivos IOT ocultos y repartidos por diversas zonas de la ciudad generando información de forma ininterrumpida (lo que llamamos ‘streaming’). Todos los mensajes, al ser interceptados, son marcados por un huella temporal (timestamp), ademas de añadir la zona desde donde han sido ‘ingestados’ (zona del iot)

Hemos sido contratados para crear un algoritmo que ayude a este gobierno. Dejamos a un lado los escrúpulos y elegimos pensar en los honorarios. (quizás tengamos alma de mercenarios…)

De vosotros, como desarrolladores Big Data, se requiere…


1.- Crear el esquema de cada uno de los datasets (ver orientación)

2.- Rellenarlos con info dummy según esquema

3.- Con el ﬁn de procesar en tiempo real toda la información, tendréis que conseguir elaborar una única fuente de información ‘completa’ con la que trabajar, por lo previamente habréis tenido que preparar la info, quitar duplicados (si los hubiere), agrupar, ordenar, etc y todo aquello que creáis necesario para el correcto y posterior proceso.

**El ﬁn último es hallar por hora (ventana temporal) las 10 palabras más usadas en los mensajes de tal ventana.** Una vez realizado este proceso, en caso de que la palabra más repetida coincida con alguna de las palabras de la lista negra, el sistema (nuestra aplicación) deberá enviar una notiﬁcación al ministro avisando de tal situación.


### Partimos de la base de que…
- El sistema funciona 24/7
- Algunos IOT pueden dejar de funcionar, o bien por batería o bien porque se apaguen en remoto (estado apagado). Los IOT apagados no deberán contabilizar para la ingesta de datos.
- Los sistemas de notiﬁcación serán simulados
- El sistema de ‘desencriptado’ (función) será simulado
- La lista negra existirá realmente, y deberéis hacer la comprobación de pertenencia a dicha lista por parte de la palabra más repetida.
- La palabra repetida no podrá ser una preposición ni conjunción ni artículo.
- Para simular el envío de datos por parte de los IOT’s, enchufaremos ﬁcheros de texto para que los procese Kafka.

### Fuentes de datos y esquemas propuestos (orientativo):

 **MensajesCapturados:** 
 
        Mensaje_Id (String)
        
        Contenido (String)
        
        User_Id (String)
        
 **Usuarios registrados:**
 
        User_Id (String)
        
        Nombre (String)
        
        Apellido (String)
        
        Edad (Int)
        
        Sexo (String)

**Dispositivos IOT:** 

        IoT_Id
        
        Encendido(Bool)
        
        Zona_Id (String)
        
 **ListaNegraPalabras(*):** 
 
        Palabra (String)
        
        
### Parte opcional


Si queréis cobrar un plus por vuestro trabajo, deberéis extraer la información de cómo se ‘relacionan’ en la red social algunos de los usuarios (GraphX).

Fuente de datos y esquema: 

**Conexiones:** 
 
        Origen_Id (String)
        
        Destino_Id (String)
        
        Tipo_Conexion (String)

   
## Se valorará...

- Claridad en el código
- Favorecer la legibilidad por encima de un código compacto
- El correcto uso de las APIs sql
- Explicación (comentada en el propio código) el porqué de cada una de las decisiones tomadas
- Uso preferido de spark sql, aunque no obligatorio 
- Investigación y ensayo autodidacta de Scala

        
# Desarrollo

Para esta práctica he decidido separar la parte de procesamiento de los mensajaes de la red social, de la parte de análisis de relaciones de los usuarios. He creado por tanto dos paquetes diferentes con sus correspondientes clases objeto.

## Procesamiento Palabras

1.- En este objeto ProcesoEspia2App voy a ir analizando los mensajes de la red social Celebram que me llegan gracias a una red de IOTs. Para simular este envío de datos de los IOTs utilizo el fichero "mensajes.csv" que he dejado en la carpeta data del proyecto, pero que realmente no forma parte del proyecto, sino que está aquí para que se guarde en algún otro directorio y se utilice desde el cliente en linea de comandos Kafka para, mandarlo como mensajes hacia
el cluster Kafka:

  > bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic celebram
  
  > bin/kafka-console-producer.sh --broker-list localhost:9092 --topic celebram < /Documents/mensajes.csv

Otra opción hubiera sido tener un fichero de mensajes por cada dispositivo iot pero he decidido mejor tener un único fichero y meter el campo Iot_Id para identificar de donde viene el mensaje.

El análisis se realiza en ventanas de 1 hora, por lo que durante ese tiempo se puede simular.la recepción de los diferentes mensajes haciendo un envío del fichero "mesajes.csv" varias veces.durante esa hora.


  2.- La estructura del fichero de mensajes es:
  
  **Mensaje_Id,Iot_Id,Usuario_Origen_Id,Usuario_Destiono_Id,Contenido**

  He introducido el Usuario Origen y Destino por si se necesitara hacer algún procesamiento adicional en base a relaciones

  3.- Un segundo fichero que utilizaré será el fichero "iot.csv" que realmente para el procesamiento de palabras no hace falta, pero lo voy a unir a la fuente de datos de mensajes por si a futuro hiciera falta algún procesamiento adicional

La estructura de ese fichero es:

**IoT_Id,Encendido,Zona_Id**

  4.- El tercer fichero utilizado es "usuarios.csv" que tampoco se necesita para el procesamiento de palabras pero
  igualmente voy a unir a la fuente pricipal por si hicera falta para un futuro procesamiento
  
  La estrucutra de este fichero es:
  
**User_Id,Nombre,Apellido,Edad,Sexo,Username**


El objetivo es hallar por hora las 10 palabras mas usadas y, caso de que la palabra más repetida coincida con alguna de las palabras de una lista negra, se mande una notificación al ministro


**NOTA1**: En el sbt he utilizado las versiones 2.4.0 debido a que he querido utilizar .foreachBath para el WriteStream y según leí en la documentación se añadió en la versión 2.4. Sé que se comentó que podía tener algún bug pero de momento para lo que he estado haciendo parece funcionar bien.

**NOTA2**: Los mensajes del fichero mensajes.csv están puestos para que la palabra mas repetida sea tramaX que está dentro
de la lista negra, por lo que se mandaría el aviso.

**NOTA 3**: La ventana temporal de consulta es de 1 hora y así está configurado, pero el trigger está puesto cada 20 segundos, aunque también tendría que ser 1 hora. Así para la revisión de la práctica se puede ver el resultado antes. He dejado la línea de los 3600 segundos comentada y esa sería la que tendría que ir en la versión de producción.

  //.trigger(Trigger.ProcessingTime("30 second"))
  
  trigger(Trigger.ProcessingTime("3600 second"))

**NOTA4**: Estoy mostrando como añadido la estructura de los mensajes capturados junto con los datos de los iots y de usuarios. Esto tiene puesto un trigger de 10 segundos. Simplemente lo he dejado para la revisión de la estructura. Es algo que se quitaría en la versión de producción

