create or replace view flatten_word
as
select id as OwnerUserId, word 
from posts 
lateral view explode(split(concat(title, " ", body), ' |,')) lateralTable as word 
where word != "";

---------- Term frequent table 
--- 
--create or replace view tf
--as 
--select a.OwnerUserId, a.word, termCount, wordCount, termCount/wordCount as termFreq
--from 
--(select OwnerUserId, word, count(*) as termCount 
--from flatten_word
--group by OwnerUserId, word) as a 
--join (
--select OwnerUserId, count(*) as wordCount
--from flatten_word
--group by OwnerUserId) as b 
--on a.OwnerUserId = b.OwnerUserId;

-- using log normalization in term frequent. It reduces computation operation 
create or replace view tf
as 
select OwnerUserId, word, log(10, count(*) + 1) as termFreq
from flatten_word
group by OwnerUserId, word;

------create inverse view 
create or replace view idf 
as 
select word, numberAppearInUser, numberUser, log(10, numberUser/(numberAppearInUser + 1)) + 1 as idf 
from (
select word, count(distinct(OwnerUserId)) as numberAppearInUser
from flatten_word
group by word) as a 
cross join (
select count(distinct (OwnerUserId)) as numberUser
from flatten_word) as b;

----create tfidf table (save in hdfs) 
create external table if not exists tfidf (
 OwnerUserId int, 
 word String,
 tfidf double
)
location '/user/hive/warehouse/tfidf';

--- insert data to tfidf table
INSERT OVERWRITE table tfidf
select OwnerUserId, tf.word, tf.termFreq * idf.idf as tfidf 
from tf 
join idf 
on tf.word = idf.word;


