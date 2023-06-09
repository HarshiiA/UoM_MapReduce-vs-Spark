--Check to make sure the sample file is loaded and accessible on the S3 storage by issuing an AWS command

aws s3 ls s3://delay-flights-bucket/dataset/




CREATE EXTERNAL TABLE DelayedFlights 
(
    FlightId int,
    Year int,
    Month int,
    DayofMonth int,
    DayOfWeek int,
    DepTime int,
    CRSDepTime int,
    ArrTime int,
    CRSArrTime int,
    UniqueCarrier varchar(10),
    FlightNum int,
    TailNum varchar(10),
    ActualElapsedTime int,
    CRSElapsedTime int,
    AirTime int,
    ArrDelay int,
    DepDelay int,
    Origin varchar(10),
    Dest varchar(10),
    Distance int,
    TaxiIn int,
    TaxiOut int,
    Cancelled int,
    CancellationCode varchar(10),
    Diverted int,
    CarrierDelay varchar(10),
    WeatherDelay varchar(10),
    NASDelay varchar(10),
    SecurityDelay varchar(10),
    LateAircraftDelay varchar(10)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION "s3://delay-flights-bucket/dataset/";
