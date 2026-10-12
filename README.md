# p8ster

Proof-of-concept pastebin.com scraper

## Probability

The alphabet is 0-9a-zA-Z, that's 62 characters in total.

Number of unique IDs and time it'll take to probe them all (account for 1 second pause between requests):

```
62^6 =      56,800,235,584  ## 1,800 years
62^7 =   3,521,614,606,208  ## 111,593 years
62^8 = 218,340,105,584,896  ## 6,918,780 years
```

## Usage

    p8ster              guess random 6-char IDs forever
    p8ster -l 8         guess random IDs of length 6, 7 or 8
    p8ster ID [ID...]   scrape just these IDs (6-8 chars), then exit
