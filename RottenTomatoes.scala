package com.twitter.ads.batch.job.targeting.recommendations

import com.twitter.ads.batch.source.StatusSource
import com.twitter.scalding._

import cascading.pipe.Pipe
import cascading.tuple.{Tuple, TupleEntryIterator, Fields}

/**
 * Calculate similarities between movies,
 * using RottenTomatoes tweets.
 *
 * @author Edwin Chen
 */
class RottenTomatoes(args : Args) extends VectorSimilarities(args) {

  /**
   * Example tweets:
   * My review for 'Hop' on Rotten Tomatoes: 1 star > http://bit.ly/AB7Tl4
   * My review for 'The Bothersome Man (Den Brysomme mannen)' on Rotten Tomatoes: 3 stars-A muddled Playtime in Paris,... http://tmto.es/AvPoO2
   */
  val ROTTENTOMATOES_REGEX = """My review for '(.+?)' on Rotten Tomatoes: (\d) star""".r

  override val MIN_NUM_RATERS = 2
  override val MAX_NUM_RATERS = 1000
  override val MIN_INTERSECTION = 2

  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val rottenTomatoesRatings =
      StatusSource()
        .mapTo('userId, 'text) { s => (s.getUserId.toLong, s.getText) }
        .flatMap('text -> ('movie, 'rating)) {
          text : String =>
          ROTTENTOMATOES_REGEX.findFirstMatchIn(text).map { _.subgroups }.map { x => (x(0), x(1).toInt) }
        }
        .rename(('userId, 'movie, 'rating) -> (userField, itemField, ratingField))
        .unique(userField, itemField, ratingField)

    rottenTomatoesRatings
  }
}
