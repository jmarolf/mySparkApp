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
                .AppName("post_analysis")
                .GetOrCreate();

            // Create initial DataFrame
            DataFrame dataFrame = spark.Read()
                .Schema("@Id INT, @PostTypeId INT, @CreationDate STRING, @Score INT, @ViewCount INT, @Body STRING, @OwnerUserId INT, @LastActivityDate STRING, @Title STRING, @Tags STRING, @AnswerCount INT, @CommentCount INT, @FavoriteCount INT")
                .Json(@"C:\data\3dprinting.meta.stackexchange.com\Posts.json");

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
