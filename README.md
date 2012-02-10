# Movie Recommendations and More via MapReduce and Scalding

This is going to be an in-your-face introduction to [Scalding](https://github.com/twitter/scalding), the (Scala + Cascading) MapReduce framework that Twitter recently open-sourced.

In 140 characters: instead of forcing you to write raw *map* and *reduce* functions, Scalding allows you to write natural code like

``` scala
// Create a histogram of tweet lengths.
tweets.map('tweet -> 'length) { tweet : String => tweet.size }.groupBy('length) { _.size }
```

(Not much different from the Ruby code you'd write to compute tweet distributions over *small* data!)

Two notes before we begin:

* [This Github repository](https://github.com/echen/scaldingale) contains all the code used.
* For a gentler introduction to Scalding, see [this Getting Started guide](https://github.com/twitter/scalding/wiki/Getting-Started) on the Scalding wiki.

# Movie Similarities

Imagine you run an online movie business, and you want to generate movie recommendations. You have a rating system (people can rate movies with 1 to 5 stars), and we'll assume for simplicity that all of the ratings are stored in a TSV file somewhere.

Let's start by reading the ratings into a Scalding job.

``` scala
/**
 * The input is a TSV file with three columns: (user, movie, rating).
 */  
val INPUT_FILENAME = "data/ratings.tsv"

/**
 * Read in the input and give each field a type and name.
 */
val ratings = 
  Tsv(INPUT_FILENAME).read
    .mapTo((0, 1, 2) -> ('user, 'movie, 'rating)) { fields : (String, String, Double) => fields }
    
/**
 * Let's also keep track of the total number of people who rated each movie.
 */
val numRaters =
  ratings
    // Put the number of people who rated each movie into a field called "numRaters".    
    .groupBy('movie) { _.size }.rename('size -> 'numRaters) // Shortcut: .groupBy('movie) { _.size('numRaters) }
    // Rename, since when we join, Scalding currently requires both sides to have distinctly named fields.
    .rename('movie -> 'movieX)

// Merge `ratings` with `numRaters`, by joining on their movie fields.
val ratingsWithSize =
  ratings
    .joinWithSmaller('movie -> 'movieX, ratings)    
    .discard('movieX) // Remove the extra field.

// ratingsWithSize now contains the following fields: (user, movie, rating, numRaters).
```

You want to calculate how similar every two movies are, so that if someone watches *The Lion King*, you can recommend movies like *Toy Story*.

So how should you define the similarity between two movies?

One way is to use their **correlation**:

* For every pair of movies A and B, find all the people who rated both A and B.
* Use these ratings to form a Movie A vector and a Movie B vector.
* Calculate the correlation between these two vectors.
* Whenever someone watches a movie, you could then recommend the movies most correlated with it.

Let's start with the first two steps.

``` scala
/**
 * To get all pairs of co-rated movies, we'll join `ratings` against itself.
 * So first make a dummy copy of the ratings that we can join against.
 */
val ratings2 = 
  ratingsWithSize
    .rename(('user, 'movie, 'rating, 'numRaters) -> ('user2, 'movie2, 'rating2, 'numRaters2))

/**
 * Now find all pairs of co-rated movies (pairs of movies that a user has rated) by
 * joining the duplicate rating streams on their user fields, 
 */
val ratingPairs =
  ratingsWithSize
    .joinWithSmaller('user -> 'user2, ratings2)
    // De-dupe so that we don't calculate similarity of both (A, B) and (B, A).
    .filter('movie, 'movie2) { movies : (String, String) => movies._1 < movies._2 }
    .project('movie, 'rating, 'numRaters, 'movie2, 'rating2, 'numRaters2)

// By grouping on ('movie, 'movie2), we can now get all the people who rated any pair of movies.
```

Before using these rating pairs to calculate correlation, let's stop for a bit.

Since we're explicitly thinking of movies as **vectors** of ratings, it's natural to compute some very vector-y things like norms and dot products, as well as the length of each vector and the sum over all elements in each vector. So let's compute these:

``` scala
/**
 * Compute dot products, norms, sums, and sizes of the rating vectors.
 */
val vectorCalcs =
  ratingPairs
    // Compute (x*y, x^2, y^2), which we need for dot products and norms.
    .map(('rating, 'rating2) -> ('ratingProd, 'ratingSq, 'rating2Sq)) {
      ratings : (Double, Double) =>
      (ratings._1 * ratings._2, math.pow(ratings._1, 2), math.pow(ratings._2, 2))
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
        // Notice that all of these operations chain together like in a builder object.
    }
```

To summarize, each row in `vectorCalcs` now contains the following fields:

* **movie, movie2**
* **numRaters, numRaters2**: the total number of people who rated each movie
* **size**: the number of people who rated both movie and movie2
* **dotProduct**: dot product between the movie vector (a vector of ratings) and the movie2 vector (also a vector of ratings)
* **ratingSum, rating2sum**: sum over all elements in each ratings vector
* **ratingNormSq, rating2Normsq**: squared norm of each vector

So let's go back to calculating the correlation between movie and movie2. We could, of course, calculate correlation in the standard way: find the covariance between the movie and movie2 ratings, and divide by their standard deviations.

But recall that we can also write correlation in the following form:

$Corr(X, Y) = \frac{n \sum xy - \sum x \sum y}{\sqrt{n \sum x^2 - (\sum x)^2} \sqrt{n \sum y^2 - (\sum y)^2}}$

(See the [Wikipedia page](http://en.wikipedia.org/wiki/Correlation_and_dependence) on correlation.)

Notice that every one of the elements in this formula is a field in `vectorCalcs`! So instead of using the standard calculation, let's use this form instead:

``` scala
val correlations =
  vectorCalcs
    .map(('size, 'dotProduct, 'ratingSum, 'rating2Sum, 'ratingNormSq, 'rating2NormSq) -> 'correlation) {
      val fields : (Double, Double, Double, Double, Double, Double) =>
      correlation(fields._1, fields._2, fields._3, fields._4, fields._5, fields._6)
    }
    
def correlation(size : Double, dotProduct : Double, ratingSum : Double, 
  rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {

  val numerator = size * dotProduct - ratingSum * rating2Sum
  val denominator = math.sqrt(size * ratingNormSq - ratingSum * ratingSum) * math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

  numerator / denominator
}
```

And that's it! To see the full code, check out the Github repository [here](https://github.com/echen/scaldingale).

# Book Similarities

Let's run this code over some real data. Unfortunately, I didn't have a clean source of movie ratings available, so instead I used [this dataset of 1 million book ratings](http://www.informatik.uni-freiburg.de/~cziegler/BX/).

I ran a quick command, using the handy [scald.rb script](https://github.com/twitter/scalding/wiki/Scald.rb) that Scalding provides...

```
# Send the job off to a Hadoop cluster
scald.rb MovieSimilarities.scala --input book-crossing-ratings.tsv --output book-crossing-similarities.tsv
```

...and here's a sample of the top output I got:

[![Top Book-Crossing Pairs](http://dl.dropbox.com/u/10506/blog/scaldingale/top-book-crossing-sims-correlation.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/top-book-crossing-sims-correlation.png)

We see that 

* *Harry Potter* books are similar to other *Harry Potter* books
* *Lord of the Rings* books are similar to other *Lord of the Rings* books
* A Tom Clancy book is similar to a John Grisham book
* Chick lit (*Summer Sisters*, by Judy Blume) is similar to chick lit (*Bridget Jones*)

So our Scalding job is looking pretty awesome!

Just for fun, let's also look at books similar to *The Great Gatsby*:

[![Great Gatsby](http://dl.dropbox.com/u/10506/blog/scaldingale/great-gatsby-correlation.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/great-gatsby-correlation.png)

(Schoolboy memories, exactly.)

# More Similarity Measures

Of course, there are lots of other similarity measures we could use besides correlation.

## Cosine Similarity

For example, [cosine similarity](http://en.wikipedia.org/wiki/Cosine_similarity) is a another common vector-based similarity measure.

``` scala
def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
  dotProduct / (ratingNorm * rating2Norm)
}
```

## Correlation, Take II

We can also also add a *regularized* correlation, by (say) adding N virtual movie pairs that have zero correlation. This helps avoid noise if some movie pairs have very few raters in common (for example, *The Great Gatsby* had an unlikely raw correlation of 1 with many other books, due simply to the fact that those book pairs had very few ratings).

``` scala
def regularizedCorrelation(size : Double, dotProduct : Double, ratingSum : Double, 
  rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double, 
  virtualCount : Double, priorCorrelation : Double) = {

  val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
  val w = size / (size + virtualCount)

  w * unregularizedCorrelation + (1 - w) * priorCorrelation
}
```

## Jaccard Similarity

Recall that [one of the lessons of the Netflix prize](http://blog.echen.me/blog/2011/10/24/winning-the-netflix-prize-a-summary/) was that implicit data can be quite useful -- the mere fact that you rate a James Bond movie, even if you rate it quite horribly, suggests that you'd probably be interested in similar films. So we can also ignore the value itself of each rating and use a *set*-based similarity measure like [Jaccard similarity](http://en.wikipedia.org/wiki/Jaccard_index).

``` scala
def jaccardSimilarity(usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) = {
  val union = totalUsers1 + totalUsers2 - usersInCommon
  usersInCommon / union
}
```

## Incorporation

Finally, let's add all these similarity measures to our output.

``` scala
val PRIOR_COUNT = 50
val PRIOR_CORRELATION = 0

val similarities =
  vectorCalcs
    .map(('size, 'dotProduct, 'ratingSum, 'rating2Sum, 'ratingNormSq, 'rating2NormSq, 'numRaters, 'numRaters2) -> 
      ('correlation, 'regularizedCorrelation, 'cosineSimilarity, 'jaccardSimilarity)) {
        
      fields : (Double, Double, Double, Double, Double, Double, Double, Double) =>
              
      val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields
      
      val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
      val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
      val cosSim = cosineSimilarity(dotProduct, math.sqrt(ratingNormSq), math.sqrt(rating2NormSq))
      val jaccard = jaccardSimilarity(size, numRaters, numRaters2)
      
      (corr, regCorr, cosSim, jaccard)
    }
```

# Book Similarities Revisited

Let's take another look at the book similarities above, this time with these new fields. 

Here are some of the top Book-Crossing pairs, sorted by the shrunk correlation:

[![Top Book-Crossing Pairs](http://dl.dropbox.com/u/10506/blog/scaldingale/top-book-crossing-sims.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/top-book-crossing-sims.png)

Notice how regularization affects things: the *Dark Tower* pair has a pretty high raw correlation, but relatively few ratings (which makes us less confident in the raw correlation), so it ends up below the others.

And here are books similar to *The Great Gatsby*, this time ordered by cosine similarity:

[![Great Gatsby](http://dl.dropbox.com/u/10506/blog/scaldingale/great-gatsby.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/great-gatsby.png)
    
# Input Abstraction

Okay, so right now, our code is tied to our specific `ratings.tsv` input. But what if we change the way we store our ratings, or what if we want to generate similarities for something entirely different?

Let's abstract away our input. We'll create a [VectorSimilarities class](https://github.com/echen/scaldingale/blob/master/VectorSimilarities.scala) that represents input data in the following format:

``` scala
// This is an abstract method that returns a Pipe (aka, a stream of rating tuples).
// It takes in three symbols that name the user, item, and rating fields.
def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe

val ratings = input('user, 'item, 'rating)
// ...
// The rest of the code remains essentially the same.
```
  
Whenever we want to define a new input format, we simply subclass the abstract `VectorSimilarities` class and provide a concrete implementation of the `input` method.

## Book-Crossings

For example, here's a class I could have used to generate the book recommendations above:

``` scala
class BookCrossing(args : Args) extends VectorSimilarities(args) {  
  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val bookCrossingRatings =
      Tsv("book-crossing-ratings.tsv")
        .read
        .mapTo((0, 1, 2) -> (userField, itemField, ratingField)) { fields : (String, String, Double) => fields }
    
    bookCrossingRatings
  }  
}
```

The input method simply reads from a TSV file and lets the `VectorSimilarities` class do all the work.

## Song Similarities with Twitter + iTunes

But why limit ourselves to books? We do, after all, have Twitter at our fingertips...

<blockquote class="twitter-tweet"><p>rated Born This Way by Lady GaGa 5 stars <a href="http://t.co/wTYAwWqm" title="http://itun.es/iSg92N">itun.es/iSg92N</a> <a href="https://twitter.com/search/%2523iTunes">#iTunes</a></p>&mdash; gggf (@GalMusic92) <a href="https://twitter.com/GalMusic92/status/167267017865428996" data-datetime="2012-02-08T15:22:19+00:00">February 8, 2012</a></blockquote>
<script src="//platform.twitter.com/widgets.js" charset="utf-8"></script>

iTunes apparently lets you send a tweet whenever you rate a song, so let's make use of this data and generate music recommendations.

Again, we create a new class that overrides the abstract `input` defined in `VectorSimilarities`:

``` scala
class ITunes(args : Args) extends VectorSimilarities(args) {  
  // Example tweet:
  // rated New Kids On the Block: Super Hits by New Kids On the Block 5 stars http://itun.es/iSg3Fc #iTunes
  val ITUNES_REGEX = """rated (.+?) by (.+?) (\d) stars .*? #iTunes""".r

  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val itunesRatings =
      TweetSource()
        .mapTo('userId, 'text) { s => (s.getUserId, s.getText) }
        .filter('text) { text : String => text.contains("#iTunes") }
        .flatMap('text -> ('song, 'artist, 'rating)) {
          text : String =>         
          ITUNES_REGEX.findFirstMatchIn(text).map { _.subgroups }.map { l => (l(0), l(1), l(2)) }
        }  
        .rename(('userId, 'song, 'rating) -> (userField, itemField, ratingField))
        .project(userField, itemField, ratingField)
    
    itunesRatings
  }
}
```

(In this code, I used a Twitter-internal tweet source that provides data on tweets, but you could just as easily [scrape Twitter yourself](https://dev.twitter.com/docs) and provide your own source of tweets instead.)

And snap, here are some songs you might like if you recently listened to **Beyoncé**:

[![Jason Mraz](http://dl.dropbox.com/u/10506/blog/scaldingale/beyonce.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/beyonce.png)

(Unfortunately, the data was quite sparse, so the similarities might not be too great.)

And some recommended songs if you like **Lady Gaga**:

[![Lady Gaga](http://dl.dropbox.com/u/10506/blog/scaldingale/lady-gaga.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/lady-gaga.png)

## Location Similarities with Foursquare Check-ins

But what if we don't have explicit ratings? For example, we could be a news site that wants to generate article recommendations, and maybe we only have user *visits* on each story.

Or what if we want to generate restaurant or tourist recommendations, when all we know is who visits each location?

<blockquote class="twitter-tweet"><p>I'm at Empire State Building (350 5th Ave., btwn 33rd & 34th St., New York) <a href="http://t.co/q6tXzf3n" title="http://4sq.com/zZ5xGd">4sq.com/zZ5xGd</a></p>&mdash; Simon Ackerman (@SimonAckerman) <a href="https://twitter.com/SimonAckerman/status/167232054247956481" data-datetime="2012-02-08T13:03:23+00:00">February 8, 2012</a></blockquote>
<script src="//platform.twitter.com/widgets.js" charset="utf-8"></script>

Let's finally make Foursquare check-ins useful. (I kid, I kid.)

Instead of using an explicit rating given to us, we can simply generate a dummy rating of 1 for each check-in. Correlation doesn't make sense any more, but we can still pay attention to a measure like Jaccard simiilarity.

So again, we create a new class that scrapes Twitter for Foursquare check-ins and parses them into a ratings format:

``` scala
class Foursquare(args : Args) extends VectorSimilarities(args) {  
  // Example tweet: I'm at The Ambassador (673 Geary St, btw Leavenworth & Jones, San Francisco) w/ 2 others http://4sq.com/xok3rI
  // Let's limit to New York for simplicity.
  val FOURSQUARE_REGEX = """I'm at (.+?) \(.*? New York""".r

  override def input(userField : Symbol, itemField : Symbol, ratingField : Symbol) : Pipe = {
    val foursquareCheckins =
      TweetSource()
        .mapTo('userId, 'text) { s => (s.getUserId.toLong, s.getText) }
        .flatMap('text -> ('location, 'rating)) {
          text : String =>         
          FOURSQUARE_REGEX.findFirstMatchIn(text).map { _.subgroups }.map { l => (l(0), 1) }
        }
        .rename(('userId, 'location, 'rating) -> (userField, itemField, ratingField))
        .unique(userField, itemField, ratingField)
    
    foursquareCheckins
  }  
}
```

And bam! Here are locations similar to the **Empire State Building**:

[![Empire State Building](http://dl.dropbox.com/u/10506/blog/scaldingale/empire-state-building.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/empire-state-building.png)

Here are places you might want to check out, if you check-in at **Bergdorf Goodman**:

[![Bergdorf Goodman](http://dl.dropbox.com/u/10506/blog/scaldingale/bergdorf-goodman.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/bergdorf-goodman.png)

And here's where to go after the **Statue of Liberty**:

[![Statue of Liberty](http://dl.dropbox.com/u/10506/blog/scaldingale/statue-of-liberty.png)](http://dl.dropbox.com/u/10506/blog/scaldingale/statue-of-liberty.png)

Power of Twitter, yo.

# Another Abstraction

So we abstracted away our input, allowing us to easily plug in new data sources (tweets, TSVs, music ratings, check-ins, Yelp, Netflix, Amazon...).

But why stop there? We could just as easily also abstract away our output as well, allowing us to filter and format our output however we want.

Or, as hinted above, we could try to generalize our similarity computations! After all, what are all of these measures but simple vector and matrix manipulations? A dot product is just a matrix multiplication, the norms of our vectors are a diagonal, placing our matrix in a min-plus algebra would give us things like [Tetris and shortest-paths](http://www-rocq.inria.fr/metalau/quadrat/STACS-Gaubert.pdf) (and, hey, even [classical mechanics](http://mathoverflow.net/questions/406/how-is-tropicalization-like-taking-the-classical-limit))...

But that's the subject of another post.

# Next Steps

Hopefully I gave you a taste of the awesomeness of Scalding. If you want to learn even more, check out these links:

* The [official Scalding repository](https://github.com/twitter/scalding) on Github.
* [A Getting Started Guide](https://github.com/twitter/scalding/wiki/Getting-Started) on the Scalding wiki.
* [A code-based introduction](https://github.com/twitter/scalding/tree/master/tutorial), complete with Scalding jobs that you can run in local mode.
* [A bunch of code snippets](https://github.com/twitter/scalding/blob/master/tutorial/CodeSnippets.md) that show you examples of different Scalding functions (e.g., `map`, `filter`, `flatMap`, `groupBy`, `count`, `join`).
* All the code for this post is [here](https://github.com/echen/scaldingale).

Watch out for more documentation soon, and definitely [follow @Scalding on Twitter](https://twitter.com/#!/scalding) for updates or to ask any questions.

# Mad Props

And finally, a huge shoutout to [Argyris Zymnis](https://twitter.com/argyris), [Avi Bryant](https://twitter.com/avibryant), and [Oscar Boykin](https://twitter.com/posco), the mastermind hackers who have spent (and continue spending!) unimaginable hours making Scalding a joy to use. 

@argyris, @avibryant, @posco: Thanks for everything you've done. #awesomejobguys