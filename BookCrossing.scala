package me.echen.scaldingale

import com.twitter.scalding._

import cascading.pipe.Pipe
import cascading.tuple.{Tuple, TupleEntryIterator, Fields}

class BookCrossing(args : Args) extends VectorSimilarities(args) {
  
  override val MIN_NUM_RATERS = 2
  override val MAX_NUM_RATERS = 1000
  override val MIN_INTERSECTION = 2

  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val bookCrossingRatings =
      Tsv("book-ratings.tsv")
        .read
        .mapTo((0, 1, 2) -> (userField, itemField, ratingField)) { fields : (String, String, Double) => fields }
    
    bookCrossingRatings
  }
  
}