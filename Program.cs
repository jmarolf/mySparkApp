using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using StructType = Microsoft.Spark.Sql.Types.StructType;
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
                .Schema("`?xml` STRUCT<`@version`: STRING, `@encoding`: STRING>, `posts` STRUCT<`row`: ARRAY<STRUCT<`@Id`: INT, `@PostTypeId`: INT, `@CreationDate`: STRING, `@Score`: INT, `@ViewCount`: INT, `@Body`: STRING, `@OwnerUserId`: INT, `@LastActivityDate`: STRING, `@Title`: STRING, `@Tags`: STRING, `@AnswerCount`: INT, `@CommentCount`: INT, `@FavoriteCount`: INT>>>")
                .Json(@"C:\data\3dprinting.meta.stackexchange.com\Posts.json");

            var schema = dataFrame.Schema();

            var words = dataFrame
                 .Select(Functions.ArrayJoin(Functions.Col("posts.row.@Body"), " ", " ").Alias("body"))
                 .Select(Functions.Split(Functions.Col("body"), " ").Alias("words"))
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
