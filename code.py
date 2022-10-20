from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *


def departure_delays(spark):

    # Set Files
    tripdelays_file = "Data/departuredelays.csv"
    airports_file = "Data/airport-codes-na.txt"

    # Obtain airports dataset
    airportsna = spark.read.csv(
        airports_file, header='true', inferSchema='true', sep='\t')
    airportsna.createOrReplaceTempView("airports_na")

    # Obtain departure Delays data
    departureDelays = spark.read.csv(tripdelays_file, header='true')
    departureDelays.createOrReplaceTempView("departureDelays")
    departureDelays.cache()

    # Available IATA codes from the departuredelays sample dataset
    tripIATA = spark.sql(
        "select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
    tripIATA.createOrReplaceTempView("tripIATA")

    # Only include airports with atleast one trip from the departureDelays dataset
    airports = spark.sql(
        "select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
    airports.createOrReplaceTempView("airports")
    airports.cache()

    # Build `departureDelays_geo` DataFrame
    # Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)
    departureDelays_geo = spark.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination")

    # Create Temporary View and cache
    departureDelays_geo.createOrReplaceTempView("departureDelays_geo")
    departureDelays_geo.cache()

    # Create Vertices (airports) and Edges (flights)
    tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
    tripEdges = departureDelays_geo.select(
        "tripid", "delay", "src", "dst", "city_dst", "state_dst")

    # Cache Vertices and Edges
    tripEdges.cache()
    tripVertices.cache()

    # Build GraphFrame
    tripGraph = GraphFrame(tripVertices, tripEdges)

    tripGraph = tripGraph.edges.filter("src = 'SEA' and delay > 100")

    tripGraph.show()


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .getOrCreate()

    try:
        departure_delays(spark)

    finally:
        spark.stop()
