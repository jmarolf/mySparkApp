using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using StructType = Microsoft.Spark.Sql.Types.StructType;

using static Microsoft.Spark.Sql.Functions;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace mySparkApp
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create a Spark session
            var spark = SparkSession
                .Builder()
                .AppName("post_analysis")
                .GetOrCreate();


            // Create initial DataFrame
            var dataFrame = spark.Read()
                .Json(@"C:\data\3dprinting.meta.stackexchange.com\Posts.json");

            dataFrame.CreateOrReplaceTempView("posts");


            // TODO: Filter tags to only contain C# questions
            //dataFrame
            //    .Select(Split(Col("Body"), " ").As("words"))
            //    .Select(Explode(Col("words")).As("word"))
            //    .GroupBy("word").Count().OrderBy(Col("count").Desc())
            //    .Show();

            // TODO: process text to find code fences and extract C# code


            // TODO: parse remaining C# code
            //var parseCSharp =
            //    Udf<string, IDictionary<string, string[]>>(
            //        (str) => GetSyntaxKindsMap(str));
        }
    }
}
