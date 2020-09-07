import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        String mode, path, dbName;

        if(args.length >= 2){
            mode = args[0];
            path = args[1];
            dbName = args[2];
        }
        else {
            mode = "local";
            path = "C:/Users/sergi/Desktop/SparkProject/";
            dbName = "NYPD_Motor_Vehicle_Collisions.csv";
        }

        SparkSession sparkSession;
        if(mode.equalsIgnoreCase("aws")) {
            sparkSession = SparkSession
                    .builder()
                    .appName("Java Spark Middleware Project")
                    .getOrCreate();
        }
        else {
            sparkSession = SparkSession
                    .builder()
                    .appName("Java Spark Middleware Project")
                    .master("local[*]")
                    .getOrCreate();
        }

        sparkSession.sqlContext()
                .udf()
                .register( "computeYear", ( String s1, Integer week ) -> {
                    int year;
                    Date date = new SimpleDateFormat("MM/dd/yyyy").parse(s1);
                    LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                    int month = localDate.getMonthValue();
                    year = localDate.getYear();
                    if(month == 1 && week > 51) {
                        return year-1;
                    }
                    else if(month == 12 && week == 1) {
                        return year+1;
                    }
                    return year;

                }, DataTypes.IntegerType);

        final Dataset<Row> carAccidentsCsv = sparkSession.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path + dbName);

        carAccidentsCsv.persist();

        //PRIMA QUERY
        final Dataset<Row> accidents = carAccidentsCsv
                .select(
                        col("UNIQUE KEY").as("RowID"),
                        col("BOROUGH").as("Borough"),
                        col("DATE").as("Date"),
                        weekofyear(to_date(col("DATE"), "MM/dd/yyyy")).as("WeekNumber"),
                        col("NUMBER OF PERSONS KILLED").gt(0).as("WasLethal")
                );

        final Dataset<Row> accidentsWithReferenceYear = accidents
                .withColumn("ReferenceYear", functions.callUDF("computeYear", accidents.col("Date"), accidents.col("WeekNumber")));

        accidentsWithReferenceYear.persist();

        final Dataset<Row> lethalAccidents = accidentsWithReferenceYear
                .filter(col("WasLethal"))
                .groupBy(col("ReferenceYear"), col("WeekNumber"))
                .agg(
                        count(lit(1)).as("NumLethal")
                )
                .orderBy(col("ReferenceYear").desc(), col("WeekNumber").desc());

        lethalAccidents
                .coalesce(1)
                .write()
                .format("csv")
                .option("header","true")
                .mode("overwrite")
                .save(path + "query1.out");

        //SECONDA QUERY
        final Dataset<Row> contribFactorAccidents = carAccidentsCsv.select(
                col("UNIQUE KEY").as("RowID"),
                col("NUMBER OF PERSONS KILLED").gt(0).as("WasLethal"),
                col("NUMBER OF PERSONS KILLED").as("Deaths"),
                col("CONTRIBUTING FACTOR VEHICLE 1").as("CB1"),
                col("CONTRIBUTING FACTOR VEHICLE 2").as("CB2"),
                col("CONTRIBUTING FACTOR VEHICLE 3").as("CB3"),
                col("CONTRIBUTING FACTOR VEHICLE 4").as("CB4"),
                col("CONTRIBUTING FACTOR VEHICLE 5").as("CB5")
                )
                .orderBy(col("RowID").asc());

        carAccidentsCsv.unpersist();

        final Dataset<Row> allContribFactorAccidents = contribFactorAccidents
                .select(col("RowID"),
                        col("WasLethal"),
                        col("Deaths"),
                        col("CB1").as("CB"))
                .filter(col("CB").isNotNull())
                .union(contribFactorAccidents
                        .select(col("RowID"),
                                col("WasLethal"),
                                col("Deaths"),
                                col("CB2").as("CB"))
                        .filter(col("CB").isNotNull())
                        )
                .union(contribFactorAccidents
                        .select(col("RowID"),
                                col("WasLethal"),
                                col("Deaths"),
                                col("CB3").as("CB"))
                        .filter(col("CB").isNotNull())
                        )
                .union(contribFactorAccidents
                        .select(col("RowID"),
                                col("WasLethal"),
                                col("Deaths"),
                                col("CB4").as("CB"))
                        .filter(col("CB").isNotNull())
                        )
                .union(contribFactorAccidents
                        .select(col("RowID"),
                                col("WasLethal"),
                                col("Deaths"),
                                col("CB5").as("CB"))
                        .filter(col("CB").isNotNull())
                        )
                //Questa riga è stata commentata per poter fare test sulle prestazioni
                //.distinct()
                .groupBy(col("CB"))
                .agg(
                        //Questa riga è stata commentata per poter fare test sulle prestazioni
                        //count(col("RowID")).alias("NumAcc"),
                        count(lit(1)).alias("NumAcc"),
                        sum(when(col("WasLethal"), 1).otherwise(0)).as("NumLethal"),
                        sum(col("Deaths")).as("NumDeaths")
                )
                .withColumn("PercentageLethal", col("NumLethal").divide(col("NumAcc")).multiply(100))
                .withColumn("PercentageDeaths", col("NumDeaths").divide(col("NumAcc")).multiply(100))
                .orderBy(col("CB").desc());

        allContribFactorAccidents
                .coalesce(1)
                .write()
                .format("csv")
                .option("header","true")
                .mode("overwrite")
                .save(path + "query2.out");

        //TERZA QUERY
        final Dataset<Row> accidentsPerBoroughWeek = accidentsWithReferenceYear
                .filter(col("Borough").isNotNull())
                .groupBy(col("Borough"), col("ReferenceYear"), col("WeekNumber"))
                .agg(
                        //Questa riga è stata commentata per poter fare test sulle prestazioni
                        //count(col("RowID")).as("NumAcc"),
                        count(lit(1)).as("NumAcc"),
                        sum(when(col("WasLethal"), 1).otherwise(0)).as("NumLethal")
                )
                .orderBy(col("Borough").desc(), col("ReferenceYear").desc(), col("WeekNumber").desc());

        accidentsWithReferenceYear.unpersist();

        accidentsPerBoroughWeek
                .coalesce(1)
                .write()
                .format("csv")
                .option("header","true")
                .mode("overwrite")
                .save(path + "query3-1.out");

        final Dataset<Row> accidentsPerBorough = accidentsPerBoroughWeek
                .groupBy(col("Borough"))
                .agg(
                        sum(col("NumLethal")).divide(sum(col("NumAcc"))).as("AvgLethalAccPerWeek")
                )
                .orderBy(col("Borough").desc());

        accidentsPerBorough
                .coalesce(1)
                .write()
                .format("csv")
                .option("header","true")
                .mode("overwrite")
                .save(path + "query3-2.out");

        sparkSession.close();
    }
}