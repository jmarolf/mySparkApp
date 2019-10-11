using System;
using Microsoft.Spark.Sql;

namespace mySparkApp
{
    class Program
    {
        static void Main(string[] args)
        {
             // Create a Spark session
            var spark = SparkSession
                .Builder()
                .AppName("word_count_sample")
                .GetOrCreate();

            // Create initial DataFrame
            DataFrame dataFrame = spark.Read().Text(@"C:\source\jmarolf\mySparkApp\input.txt");

            // Count words
            var words = dataFrame
                .Select(Functions.Split(Functions.Col("value"), " ").Alias("words"))
                .Select(Functions.Explode(Functions.Col("words"))
                .Alias("word"))
                .GroupBy("word")
                .Count()
                .OrderBy(Functions.Col("count").Desc());

            // Show results
            words.Show();
        }
    }
}
