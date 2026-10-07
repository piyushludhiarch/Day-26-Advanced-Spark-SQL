import org.apache.spark.sql.SparkSession

object Day26App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Advanced Spark SQL")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val sales = Seq(
      (1,"Laptop","Electronics",60000),
      (2,"Phone","Electronics",30000),
      (3,"Mouse","Accessories",800),
      (4,"Keyboard","Accessories",1500),
      (5,"Monitor","Electronics",20000),
      (6,"Chair","Furniture",7000)
    ).toDF(
      "id",
      "product",
      "category",
      "amount"
    )

    sales.createOrReplaceTempView("sales")

    println("===== ADVANCED SPARK SQL =====")

    println("CTE:")

    spark.sql("""
      WITH category_stats AS (
        SELECT
          category,
          SUM(amount) AS total_sales,
          AVG(amount) AS avg_sales
        FROM sales
        GROUP BY category
      )
      SELECT *
      FROM category_stats
      ORDER BY total_sales DESC
    """).show(false)

    println("CASE:")

    spark.sql("""
      SELECT
        product,
        amount,
        CASE
          WHEN amount >= 30000 THEN 'High'
          WHEN amount >= 5000 THEN 'Medium'
          ELSE 'Low'
        END AS sales_level
      FROM sales
    """).show(false)

    println("SUBQUERY:")

    spark.sql("""
      SELECT product, amount
      FROM sales
      WHERE amount > (
        SELECT AVG(amount)
        FROM sales
      )
    """).show(false)

    println("RANKING:")

    spark.sql("""
      SELECT
        product,
        category,
        amount,
        RANK() OVER (
          PARTITION BY category
          ORDER BY amount DESC
        ) AS rank
      FROM sales
    """).show(false)

    spark.stop()
  }
}
