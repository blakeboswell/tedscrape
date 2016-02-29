# tedscrape

Python script for scraping the transcripts of [Ted Talks](https://www.ted.com/talks).

`Python 2.7.10`

Output is JSON dictionary :

```
{
    'speaker': <speaker name>,
    'title': <talk title>,
    'date': <talk date>,
    'url': <talk page url>,
    'categories': <list of category labels>,
    'transcript': <talk transcript>,
    'view_n': <count of views>,
    'topics': <list of topic labels>
}

```
