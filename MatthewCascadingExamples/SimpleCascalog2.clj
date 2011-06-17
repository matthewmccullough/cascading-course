(comment http://nathanmarz.com/blog/new-cascalog-features-outer-joins-combiners-sorting-and-more.html)
(use 'cascalog.playground) (bootstrap)
(?<- (stdout) [?person ?age ?gender]
          (age ?person ?age) (gender ?person ?gender))