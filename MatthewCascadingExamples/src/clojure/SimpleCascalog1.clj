(comment "http://nathanmarz.com/blog/introducing-cascalog-a-clojure-based-query-language-for-hado.html")
(use 'cascalog.playground) (bootstrap)

(comment "Print all people from the AGE set")
(?<- (stdout) [?person]     (age ?person _))

(comment "Print all people and ages from the AGE set")
(?<- (stdout) [?person ?years] (age ?person ?years))

(comment "Print all people from the AGE set equal to 25")
(?<- (stdout) [?person] (age ?person 25))

(comment "Print all ages from the AGE set and show which names containing g")
(defn containsg [str] (.contains str "g")) 
(?<- (stdout) [?name ?years ?doescontaing]
    (age ?name ?years)
    (containsg ?name :> ?doescontaing))

(comment "Print all ages from the AGE set with a name containing g")
(defn containsg [str] (.contains str "g")) 
(?<- (stdout) [?name ?years ?doescontaing](age ?name ?years)(containsg ?name :> ?doescontaing)(true? ?doescontaing))

(comment "Print all ages from the AGE set with a name containing a given letter")
(defn containsstr [strcontents pattern] (.contains strcontents pattern)) 
(?<- (stdout) [?name ?years ?doescontaing]
    (age ?name ?years)
    (containsstr ?name "m" :> ?doescontaing)
    (true? ?doescontaing))



(comment "Print all names from the AGE set with years < 30")
(?<- (stdout) [?person ?age]
    (age ?person ?age)
    (< ?age 30))

(?<- (stdout) [?person]
    (follows "emily" ?person)
    (gender ?person "m"))

(?<- (stdout) [?person ?a2]
    (age ?person ?age)
    (< ?age 30)
    (* 2 ?age :> ?a2))

(?<- (stdout) [?person ?age ?young]
    (age ?person ?age)
    (< ?age 30)
    (< ?age 28 :> ?young))
    
(?<- (stdout) [?person1 ?person2] 
    (age ?person1 ?age1) (follows ?person1 ?person2)
    (age ?person2 ?age2) (< ?age2 ?age1))

(comment "emit the age difference as well")
(?<- (stdout) [?person1 ?person2 ?delta] 
    (age ?person1 ?age1) (follows ?person1 ?person2)
    (age ?person2 ?age2) (- ?age2 ?age1 :> ?delta)
    (< ?delta 0))

(?<- (stdout) [?person ?count]
          (person ?person) (follows ?person !!p2) (c/!count !!p2 :> ?count))