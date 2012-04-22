package me.echen.scaldingale

import com.twitter.scalding._

import cascading.pipe.Pipe
import cascading.tuple.{Tuple, TupleEntryIterator, Fields}

/**
 * Calculate song similarities, using iTunes tweets.
 *
 * @author Edwin Chen
 */
class ITunes(args : Args) extends VectorSimilarities(args) {
  
  // Example tweet:
  // rated New Kids On the Block: Super Hits by New Kids On the Block 5 stars http://itun.es/iSg3Fc #iTunes
  val ITUNES_REGEX = """rated (.+?) by (.+?) (\d) stars .*? #iTunes""".r
  
  override val MIN_NUM_RATERS = 2
  override val MAX_NUM_RATERS = 1000
  override val MIN_INTERSECTION = 5

  /**
   * Searches Twitter for iTunes ratings.
   *
   * Output is a pipe, where each row is of the form:
   *   (user = 124802, item = "Kumbaya by Britney Spears", rating = 4)
   */
  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val itunesRatings =
    // This is a Twitter-internal source that reads tweets off hdfs.
    // Luckily, we have an awesome API that you could use to scrape tweets yourself:
    // https://dev.twitter.com/docs
      TweetSource()
        .mapTo('userId, 'text) { s => (s.getUserId.toLong, s.getText) }
        .filter('text) { text : String => text.contains("#iTunes") }
        .flatMap('text -> ('song, 'artist, 'rating)) {
          text : String =>         
          ITUNES_REGEX.findFirstMatchIn(text).map { _.subgroups }.map { l => (l(0), l(1), l(2)) }
        }
        .discard('text)
        .map(('song, 'artist) -> 'song) { 
          songAndArtist : (String, String) => songAndArtist._1 + " by " + songAndArtist._2
        }    
    
    itunesRatings
      .rename(('userId, 'song, 'rating) -> (userField, itemField, ratingField))
      .project(userField, itemField, ratingField)
  }
  
}