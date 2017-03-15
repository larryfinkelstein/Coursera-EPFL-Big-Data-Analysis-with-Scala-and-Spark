package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import java.io.File

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Week1-Wikipedia")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  //println(this.getClass.getClassLoader.getResource("wikipedia/wikipedia.dat"))
  //println(new File(this.getClass.getClassLoader.getResource("wikipedia/wikipedia.dat").toURI).getPath)
  //val wikiRdd: RDD[WikipediaArticle] = sc.textFile("D:/coursera/code/spark-epfl/week1/wikipedia/src/main/resources/wikipedia/wikipedia.dat").map(WikipediaData.parse)
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)
  
  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: should you count the "Java" language when you see "JavaScript"?
   *  Hint3: the only whitespaces are blanks " "
   *  Hint4: no need to search in the title :)
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter(_.text.split(" ").contains(lang)).count().toInt
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(lg => (lg, occurrencesOfLang(lg, rdd))).sortBy(-_._2).toList
    /*
    var list = scala.collection.mutable.ListBuffer.empty[(String, Int)]
    for(lang <- langs){
      val tmp: (String, Int) = (lang, occurrencesOfLang(lang, rdd)) 
      list += tmp
    }
    list.toList.sortBy(-_._2)
    * 
    */
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  //val cacheInverseIndex
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    val cacheRdd = rdd.map(wa => (wa.text.split(" ").filter(a => langs.contains(a)).distinct, wa))
       .flatMap(mp => {
         var list = scala.collection.mutable.ListBuffer.empty[(String, WikipediaArticle)]
         for(wd <- mp._1){
           val pair = (wd, mp._2)
           list += pair
         }
         list.toList
       }).groupByKey().cache()
    cacheRdd
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.mapValues(_.size).collect().toList.sortBy(-_._2)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap { article =>
      langs.filter(lang => article.text.toLowerCase.split(" ").contains(lang.toLowerCase)).map((_, 1))
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect().toList
    /*
    rdd.map(wa => (wa.text.split(" ").filter(langs.contains(_)), wa.title))
       .flatMap(data =>{
           val list = scala.collection.mutable.ListBuffer.empty[(String, Int)]
           for(wd <- data._1){
             val pair = (wd, 1)
             list += pair
           }
           list.toList
         }
       ).reduceByKey(_ + _).collect().toList.sortBy(-_._2)
     * 
     */
  }

  
  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    //println(langsRanked3)
    println(timing)
    sc.stop()
  }
	
  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
  //(conf, sc, wikiRdd)
}
