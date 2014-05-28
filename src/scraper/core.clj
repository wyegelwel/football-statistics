(ns scraper.core
  (:require [net.cgrand.enlive-html :as html]
             [clojure.data.csv :as csv]
             [clojure.java.io :as io]
             )
  (:use [clojure.pprint])
  )

(defn fetch-url [url]
  (html/html-resource (java.net.URL. url)))

;; Reads the dom of a table-node and extracts relevant text
(defn scrap-table [table-node]
  (let [headers (map #(map html/text (html/select % [:th])) (html/select table-node [:thead]))
        body (map #(map html/text (html/select % [:td])) (html/select table-node [:tr]))
        ]
  (concat headers body)))

;; Takes in a table that has already had (scrap-table ...) called on it and then applies functions to each element based on the header and body parsers. The header and body parsers are expected to a sequence of functions, one for each column of the data, such that applying function i of the sequence to an element in column i will output reasonable data 
(defn parse-table [scraped-table header-parsers body-parsers]
  (if (seq header-parsers)
    (concat 
       [(map #(%1 %2) header-parsers (first scraped-table))]
       (map (fn [row] (map #(%1 %2) body-parsers row)) (rest scraped-table)))
    (map (fn [row] (map #(%1 %2) body-parsers row)) scraped-table)  
   ))


(defn nil-parse [a] nil)

(defn const-parse [s] 
  (fn [a] s))

;; Unfortunate result of how data is laid out. The player's name and team are in the same column so we need to separate them
(defn player-parse [s]
  (map #(apply str %)  
    (if (seq (re-matches #"[A-Z]{3,}" (apply str (take-last 3 s))))
      [(drop-last 3 s) (take-last 3 s)]
      [(drop-last 2 s) (take-last 2 s)]
    ))
  )

;; Returns a sequence of sequence, where the first sub seq is the headers of the table, the remaining sequences are stats for each player
;; Have to do a bit of cleaning to get the data to look nice
(defn get-stats [n table]
  (partition (+ n 2) 
            (remove nil? 
                     (flatten 
                       (parse-table table 
                                    (concat [nil-parse (const-parse ["Player" "Team"])] (repeat n str))
                                    (concat [nil-parse player-parse] (repeat n read-string)))
                       )))
  )

(defn scrap-page [url process-table] 
  (let [table (html/select (fetch-url url) [:table.wisfb_statsTable])]
   (process-table (scrap-table table))))

(defn write-csv [file data]
 (with-open  [out-file  (io/writer file)]
    (csv/write-csv out-file data)))
 
(def weeks (range 1 18))

(def groups {"PASSING" 15,
 "RUSHING" 9,
 "RECEIVING" 10,
 ;"RETURNING" 11, ;headers get trashed
 "KICKING" 10,
 "PUNTING" 10,
 ;"DEFENSE" 17 ;headers get trashed 
})

;; Top level function. Called to fetch a seasons worth of urls for a specific group, parse out the stats and write the data to a csv file.
(defn download-data [group year num-stats base-dir]
  (let [base-url (str "http://msn.foxsports.com/nfl/stats?season=" year "&seasonType=1&group="  group "&team=0&opp=0&sort=4&week=" )
        urls (map #(str base-url %) weeks)
        stats (pmap #(scrap-page % (partial get-stats num-stats)) urls)
        ]
    (map #(write-csv (str base-dir "/" group "/week_" %1 "_year_" year ".csv") %2) weeks stats)))

#_(pmap #(download-data % 2013 (groups %) "data/2013") (keys groups))

;; Scratch work

(def base-passing-url "http://msn.foxsports.com/nfl/stats?season=2013&seasonType=1&group=PASSING&team=0&opp=0&sort=4&week=")

(def passing-urls 
  (map #(str base-passing-url %) weeks))

(def passing-stats (pmap #(scrap-page % (partial get-stats 15)) passing-urls))

#_(map #(write-csv (str "data/passing/passing_week_" %1 ".csv") %2)  weeks passing-stats)
 
