package me.echen.scaldingale

import com.twitter.scalding._

/**
 * Given a dataset of movies and their ratings by different
 * users, how can we compute the similarity between pairs of
 * movies?
 *
 * This class computes similarities between movies
 * by representing each movie as a vector of ratings and
 * computing similarity scores over these vectors.
 *
 * Similarity measures include correlation, cosine similarity,
 * and Jaccard similarity.
 *
 * @author Edwin Chen
 */
class MovieSimilarities(args : Args) extends Job(args) {
  
  /**
   * Parameters to regularize correlation.
   */
  val PRIOR_COUNT = 10
  val PRIOR_CORRELATION = 0

  /**
   * The input is a TSV file with three columns: (user, movie, rating).
   */  
  val INPUT_FILENAME = "data/ratings.tsv"

// *************************
// * STEPS OF THE COMPUTATION
// *************************  

  /**
   * Read in the input and give each field a type and name.
   */
  val ratings = 
    Tsv(INPUT_FILENAME).read
      .mapTo((0, 1, 2) -> ('user, 'movie, 'rating)) {
        fields : (String, String, Double) => fields
        // In practice, the user and movie would probably be ids (and thus Ints or Longs),
        // but let's use Strings so we can easily print out human-readable names.
      }

  /**
   * Also keep track of the total number of people who rated a movie.
   */
  val ratingsWithSize =
    ratings
      // Put the size of each group in a field called "numRaters".  
      .groupBy('movie) { _.size('numRaters) }
      // Rename, since Scalding currently requires both sides of a join to have distinctly named fields.
      .rename('movie -> 'movieX)
      .joinWithLarger('movieX -> 'movie, ratings).discard('movieX)

  /**
   * Make a dummy copy of the ratings, so we can do a self-join.
   */
  val ratings2 = 
    ratingsWithSize
      .rename(('user, 'movie, 'rating, 'numRaters) -> ('user2, 'movie2, 'rating2, 'numRaters2))

  /**
   * Join the two rating streams on their user fields, 
   * in order to find all pairs of movies that a user has rated.  
   */
  val ratingPairs =
    ratingsWithSize
      .joinWithSmaller('user -> 'user2, ratings2)
      // De-dupe so that we don't calculate similarity of both (A, B) and (B, A).
      .filter('movie, 'movie2) { movies : (String, String) => movies._1 < movies._2 }
      .project('movie, 'rating, 'numRaters, 'movie2, 'rating2, 'numRaters2)

  /**
   * Compute dot products, norms, sums, and sizes of the rating vectors.
   */
  val vectorCalcs =
    ratingPairs
      // Compute (x*y, x^2, y^2), which we need for dot products and norms.
      .map(('rating, 'rating2) -> ('ratingProd, 'ratingSq, 'rating2Sq)) {
        ratings : (Double, Double) =>
        (ratings._1 * ratings._2, scala.math.pow(ratings._1, 2), scala.math.pow(ratings._2, 2))
      }
      .groupBy('movie, 'movie2) { 
        _
          .size // length of each vector
          .sum('ratingProd -> 'dotProduct)
          .sum('rating -> 'ratingSum)
          .sum('rating2 -> 'rating2Sum)
          .sum('ratingSq -> 'ratingNormSq)
          .sum('rating2Sq -> 'rating2NormSq)
          .max('numRaters) // Just an easy way to make sure the numRaters field stays.
          .max('numRaters2)
      }

  /**
   * Calculate similarity between rating vectors using similarity measures
   * like correlation, cosine similarity, and Jaccard similarity.
   */
  val similarities =
    vectorCalcs
      .map(('size, 'dotProduct, 'ratingSum, 'rating2Sum, 'ratingNormSq, 'rating2NormSq, 'numRaters, 'numRaters2) -> 
        ('correlation, 'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity)) {
          
        fields : (Double, Double, Double, Double, Double, Double, Double, Double) =>
                
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields
        
        val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))
        val jaccard = jaccardSimilarity(size, numRaters, numRaters2)
        
        (corr, regCorr, cosSim, jaccard)
      }

  /**
   * Output all similarities to a TSV file.
   */
  similarities
    .project('movie, 'movie2, 'corr, 'regCorr, 'cosSim, 'jaccard, 'size, 'numRaters, 'numRaters2)
    .write(Tsv("./output.tsv"))
  
// *************************
// * SIMILARITY MEASURES
// *************************
  
  /**
   * The correlation between two vectors A, B is
   *   cov(A, B) / (stdDev(A) * stdDev(B))
   *
   * This is equivalent to
   *   [n * dotProduct(A, B) - sum(A) * sum(B)] / 
   *     sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
   */
  def correlation(size : Double, dotProduct : Double, ratingSum : Double, 
    rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {
      
    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) * scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)
    
    numerator / denominator
  }
  
  /**
   * Regularize correlation by adding virtual pseudocounts over a prior:
   *   RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
   * where w = # actualPairs / (# actualPairs + # virtualPairs).
   */
  def regularizedCorrelation(size : Double, dotProduct : Double, ratingSum : Double, 
    rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double, 
    virtualCount : Double, priorCorrelation : Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }  

  /**
   * The cosine similarity between two vectors A, B is
   *   dotProduct(A, B) / (norm(A) * norm(B))
   */
  def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  /**
   * The Jaccard Similarity between two sets A, B is
   *   |Intersection(A, B)| / |Union(A, B)|
   */
  def jaccardSimilarity(usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }  
}
