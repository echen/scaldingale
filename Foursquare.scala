package me.echen.scaldingale

import com.twitter.scalding._

import cascading.pipe.Pipe
import cascading.tuple.{Tuple, TupleEntryIterator, Fields}

/**
 * Calculate similarities between different attractions/locations,
 * using Foursquare check-ins on Twitter.
 *
 * @author Edwin Chen
 */
class Foursquare(args : Args) extends VectorSimilarities(args) {
  
  // Example tweets:
  // I'm at San Francisco Pizza (Seksyen 8, Bandar Baru Bangi) http://4sq.com/zyahAX
  // I'm at The Ambassador (673 Geary St, btw Leavenworth & Jones, San Francisco) w/ 2 others http://4sq.com/xok3rI
  val FOURSQUARE_REGEX = """I'm at (.+?) \(.*? New York""".r
  
  override val MIN_NUM_RATERS = 2
  override val MAX_NUM_RATERS = 1000
  override val MIN_INTERSECTION = 2

  /**
   * Searches Twitter for Foursquare checkins.
   *
   * Output is a pipe, where each row is of the form:
   *   (user = 235452312, item = "Statue of Liberty", rating = 1)
   */
  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val foursquareCheckins =
      // This is a Twitter-internal source that reads tweets off hdfs.
      // Luckily, we have an awesome API that you could use to scrape tweets yourself:
      // https://dev.twitter.com/docs
      TweetSource() 
        .mapTo('userId, 'text) { s => (s.getUserId.toLong, s.getText) }
        // .filter('text) { text : String => text.contains("4sq.com") }
        .flatMap('text -> ('location, 'rating)) {
          text : String =>         
          FOURSQUARE_REGEX.findFirstMatchIn(text).map { _.subgroups }.map { l => (l(0), 1) }
        }
        .rename(('userId, 'location, 'rating) -> (userField, itemField, ratingField))
        .unique(userField, itemField, ratingField)
        .write(Tsv(args("output1")))
    
    foursquareCheckins
  }
  
}