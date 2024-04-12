trait TransformedModels extends DataIngestion with CleansedModels {

  val listings_with_host = listings_cleansed.join(hosts_cleansed,Seq("HOST_ID"),"Left")

}
