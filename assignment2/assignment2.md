Assignment 2 - Josh Bradley
====================
<ul>
<li> Question 0 - For both my PairsPMI and StripesPMI solutions, I followed the same work flow. Both implementations require two MapReduce (MR) jobs chained together. The final output must be the PMI calculation log( P(X,Y) / (P(X)*P(Y) ), so I designed the first MR job to calculate the fraction P(X,Y)/P(X) for every bigram pair.

Since order of words is irrelevant in this assignment, it is important to note that sorting is required when processing each document. This way, the bigram (dog, he) is mapped to the same reducer as the bigram (he, dog). Although, we are able to determine the value P(x) and P(X,Y) in the same MR job, it is not efficiently feasible to obtain the P(Y) count, as this would require unnecessarily generating multiple copies of intermediate data. This is why we need the second MR job.

In the second MR job, it's sole purpose is to map the correct bigrams to the correct reducer. There is no counting involved since this information was computed in the first MR job. The reducer aggregates the bigrams together to find and multiply the "partial fraction" counts from the first MR job with the probability 1/P(Y) and the log is taken.
</li>

<li> Question 1 - PairsPMI ran for 187.782 seconds and StripesPMI ran for 125.538 seconds.
</li>

<li> Question 2 - PairsPMI ran for 191.337 seconds and StripesPMI ran for 127.06 seconds.
</li>

<li> Question 3 - 107511
</li>

<li> Question 4 - There are actually three bigram pairs with the same max pmi. They are (abednego, meshach) = 9.319931, (meshach, shadrach) = 9.319931, and (abednego, shadrach) = 9.319931

These three words have a high PMI with each other because they are the names of characters in the bible. The "story" of each character is quite interwoven with the other two so it is pretty reasonable to find the PMI value of their bigrams to be so high. For more information, I refer you to the wikipage at http://en.wikipedia.org/wiki/Shadrach,_Meshach,_and_Abednego
</li>

<li> Question 5 - The three words that have the highest PMI with the word "cloud" are (cloud, tabernacle) = 4.153025, (cloud, glory) = 3.3988752, and (cloud, fire) = 3.2354724

The three words that have the highest PMI with the word "love" are (hate, love) = 2.5755355, (hermia, love) = 2.0289917, and (commandments, love) = 1.9395468
</li>
</ul>

Grading
=======

Everything looks great: answer to question #3 (number of pairs) is a
bit off, but not a big deal. I was able to run your code without any
problems. Great work!

Score: 35/35

-Jimmy
