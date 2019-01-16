package com.db

import com.google.gson.{Gson, GsonBuilder}
import fabricator.Fabricator
import org.apache.kafka.clients.producer.ProducerRecord

object FabricatorGenenerateDataSample{


  case class UserShopperInfo(
                             UserId : String,
                             FirstName: String,
                             LastName: String,
                             DOB: String,
                             Address : String
                             )


  def main(args: Array[String]): Unit = {

    // Using Fabricator core framework
    /**
      * Here Custom code needed to transform user schema : {UserId:String,FirstName:String,LastName:String,DOB:String,Address:String}
      */

    // Create/Instantiate domain entity object
    val fbContact = Fabricator.contact()
    val fbAlphanumaric = Fabricator.alphaNumeric()

    var count = 0
    val gson = new Gson()
    for(count <- 1 to 10  ) {
       println(gson.toJson(UserShopperInfo(fbAlphanumaric.randomGuid, fbContact.firstName, fbContact.lastName, fbContact.birthday(fbAlphanumaric.randomInt(18, 100)), fbContact.address)))
    }

  }

}
