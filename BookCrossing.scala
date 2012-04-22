package me.echen.scaldingale

import com.twitter.scalding._

import cascading.pipe.Pipe
import cascading.tuple.{Tuple, TupleEntryIterator, Fields}

/**
 * Calculate book similarities using the Book-Crossing dataset at
 *   http://www.informatik.uni-freiburg.de/~cziegler/BX/
 */
class BookCrossing(args : Args) extends VectorSimilarities(args) {
  
  override val MIN_NUM_RATERS = 2
  override val MAX_NUM_RATERS = 1000
  override val MIN_INTERSECTION = 2

  /**
   * Reads in the Book-Crossing dataset.
   */
  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val bookCrossingRatings =
      Tsv("book-ratings.tsv")
        .mapTo((0, 1, 2) -> (userField, itemField, ratingField)) { fields : (String, String, Double) => fields }
    
    bookCrossingRatings
  }
  
}