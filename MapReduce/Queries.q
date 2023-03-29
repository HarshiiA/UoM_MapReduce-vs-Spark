



--Year wise carrier delay from 2003-2010
--Year wise NAS delay from 2003-2010
--Year wise Weather delay from 2003-2010
--Year wise late aircraft delay from 2003-2010
--Year wise security delay from 2003-2010


--check if table is available--
SELECT * FROM DelayedFlights;

--Year wise carrier delay from 2003-2010--
SELECT Year, avg((CarrierDelay /ArrDelay)*100) from DelayedFlights GROUP BY Year;

--Year wise NAS delay from 2003-2010--
SELECT Year, avg((NASDelay /ArrDelay)*100) from DelayedFlights GROUP BY Year;

--Year wise Weather delay from 2003-2010--
SELECT Year, avg((WeatherDelay /ArrDelay)*100) from DelayedFlights GROUP BY Year;

--Year wise late aircraft delay from 2003-2010--
SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) from DelayedFlights GROUP BY Year;

--Year wise security delay from 2003-2010--
SELECT Year, avg((SecurityDelay /ArrDelay)*100) from DelayedFlights GROUP BY Year;


