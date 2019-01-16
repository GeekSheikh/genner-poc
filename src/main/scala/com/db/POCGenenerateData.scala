package com.db

import java.io.{BufferedWriter, FileInputStream, FileWriter}

import com.google.gson._
import fabricator.{Alphanumeric, Contact, Fabricator}
import java.util
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import com.db.POCGenenerateData.InputJsonArray
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.concurrent.duration.TimeUnit
import scala.io.Source

object POCGenenerateData {

  val gson =  new Gson()
  val domainFactoryMap =  getDomainMap
  val numberRecordsPerFile = 10

  /**
    * Json input format
    * @param name
    * @param dataAttribute
    */
  case class InputJson(
                        name: String,
                        dataAttribute: String
                      )

  /**
    * List of fields
    * @param FieldArray
    */
  case class InputJsonArray(
                             FieldArray: Array[InputJson]
                           )


  class GenerateTestDataFileRunnableThread(totalRecords: Int,inputJsonArrayObj:InputJsonArray,outputFilePath:String) extends Runnable {

    override def run() {
      val list = new util.ArrayList[String]()
      for(count <- 1 to totalRecords){
        list.add(generateTestDataRecord(inputJsonArrayObj))
      }

      //TODO can write these lines to file n way parallel to different files or push to Kafka producer N way parallel

      val writer = new BufferedWriter(new FileWriter(outputFilePath))
      list.forEach(line => {
        writer.write(line)
        writer.newLine()
      })
      writer.flush()
      writer.close()
    }

  }


  class GenerateTestDataKafka(totalRecords: Int,inputJsonArrayObj:InputJsonArray) extends Runnable {

    val CLIENT_ID = "client1"
    val KAFKA_BROKERS = "35.166.21.7:9092,34.217.25.49:9092,54.202.128.221:9092";

    class ProducerCreator {
      def createProducer: Producer[Long, String] = {
        val props = new Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        new KafkaProducer[Long, String](props)
      }
    }
    override def run() {
      val list = new util.ArrayList[String]()
      val producer = (new ProducerCreator).createProducer

      for(count <- 1 to totalRecords){
        val msg = generateTestDataRecord(inputJsonArrayObj)
        val record = new ProducerRecord[Long, String]("nke-order", msg)
        producer.send(record)
      }
    }

  }

  /**
    *
    * @param gson
    * @return
    */
  private def getInputJson: String = {

    var list = Array(InputJson("UserId", "alphanumaric.randomGuid"), InputJson("FirstName", "contact.firstName"), InputJson("LastName", "contact.lastName"), InputJson("DOB", "contact.dob"), InputJson("Address", "contact.address"))

    gson.toJson(InputJsonArray(list))
  }

  private def readInputSchemaDefinition(filePath:String) :String={

    val fileStream = new FileInputStream(filePath)
    val lines = Source.fromInputStream(fileStream).getLines()
    lines.next()
  }
  /**
    *
    * @return
    */
  private def getDomainMap:java.util.LinkedHashMap[String,Any]={
    val domainFactoryMap = new java.util.LinkedHashMap[String,Any]()
    domainFactoryMap.put ("alphaNumeric" ,Fabricator.alphaNumeric())
    domainFactoryMap.put( "contact", Fabricator.contact())
    domainFactoryMap
  }

  def main(args: Array[String]): Unit = {
    //println(readInputSchemaDefinition("/Projects/RSA/sampleSchema.json"))
    //println(getInputJson)
    createTestData(100,"/Projects/RSA/sampleSchema.json","/Projects/RSA/outputTestData","file")
  }

  /**
    *
    * @param totalRecords
    * @param inputJsonFilePath
    * @param outputFilePath
    */
  def createTestData(totalRecords:Int,inputJsonFilePath:String,outputFilePath:String,mode:String):Unit ={

    val inputSchemaJson = readInputSchemaDefinition(inputJsonFilePath)//getInputJson // instead of this read json form inputJsonFilePath
    val inputJsonArrayObj = gson.fromJson(inputSchemaJson, classOf[InputJsonArray])

    if(mode.equals("file")) {
      val numberOfOutputFiles = totalRecords / numberRecordsPerFile
      val pool = Executors.newFixedThreadPool(numberOfOutputFiles)
      for (count <- 1 to numberOfOutputFiles) {
        pool.submit(new GenerateTestDataFileRunnableThread(numberRecordsPerFile, inputJsonArrayObj, outputFilePath + "/testdata_" + count + ".json"))
      }
    }else if(mode.equals("kafka")){
      val numberOfOutputFiles = totalRecords / numberRecordsPerFile
      val pool = Executors.newFixedThreadPool(numberOfOutputFiles)
      for (count <- 1 to numberOfOutputFiles) {
        pool.submit(new GenerateTestDataKafka(numberRecordsPerFile, inputJsonArrayObj))
      }
      //pool.awaitTermination(100000,TimeUnit.SECONDS)
    }

  }

  /**
    *
    * @param inputJsonArrayObj
    * @return
    */
  private def generateTestDataRecord(inputJsonArrayObj: InputJsonArray) = {
    val jsonMap = new java.util.LinkedHashMap[String, Any]()

    for (inputJson <- inputJsonArrayObj.FieldArray) {

      val domainStr = inputJson.dataAttribute.substring(0, inputJson.dataAttribute.indexOf("."))

      val attributeStr = inputJson.dataAttribute.substring(inputJson.dataAttribute.indexOf(".") + 1, inputJson.dataAttribute.length)

      if (domainStr.equals("alphanumaric")) {

        val domain = domainFactoryMap.get("alphaNumeric").asInstanceOf[Alphanumeric]

        if (attributeStr.equals("randomGuid")) {
          jsonMap.put(inputJson.name, domain.randomGuid)
        }else if(attributeStr.equals("randomDouble")){
          jsonMap.put(inputJson.name, domain.randomDouble(37.21,42.14))// need to provide additional Range attribute in input schema
        }
      } else if (domainStr.equals("contact")) {

        val domain = domainFactoryMap.get("contact").asInstanceOf[Contact]

        attributeStr match {
          case "firstName" => jsonMap.put(inputJson.name, domain.firstName)
          case "lastName" => jsonMap.put(inputJson.name, domain.lastName)
          case "dob" => jsonMap.put(inputJson.name, domain.birthday(domainFactoryMap.get("alphaNumeric").asInstanceOf[Alphanumeric].randomInt(10,100)))
          case "address" => jsonMap.put(inputJson.name, domain.address)
        }
      }
    }
    gson.toJson(jsonMap, classOf[util.LinkedHashMap[String, Any]])
  }
}
