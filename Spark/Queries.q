spark-shell

val df = spark.read.format("csv").option("header", "true").load("s3://delay-flights-bucket/dataset/DelayedFlights-updated.csv");

df.createOrReplaceTempView("delay_data")

--Year wise carrier delay from 2003-2010--
spark.time {
  val result = spark.sql("SELECT Year, avg((CarrierDelay /ArrDelay)*100) from delay_data GROUP BY Year").show()
}

--Year wise NAS delay from 2003-2010--
spark.time {
  val result = spark.sql("SELECT Year, avg((NASDelay /ArrDelay)*100) from delay_data GROUP BY Year").show()
}

--Year wise Weather delay from 2003-2010--
spark.time {
  val result = spark.sql("SELECT Year, avg((WeatherDelay /ArrDelay)*100) from delay_data GROUP BY Year").show()
}

--Year wise late aircraft delay from 2003-2010--
spark.time {
  val result = spark.sql("SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) from delay_data GROUP BY Year").show()
}

--Year wise security delay from 2003-2010--
spark.time {
  val result = spark.sql("SELECT Year, avg((SecurityDelay /ArrDelay)*100) from delay_data GROUP BY Year").show()
}
