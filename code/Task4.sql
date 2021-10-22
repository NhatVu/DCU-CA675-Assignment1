create or replace view flatten_word
as
select cast(OwnerUserId as int) as OwnerUserId, trim(word) as word 
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
);
--location '/user/hive/warehouse/tfidf';

--- insert data to tfidf table
INSERT OVERWRITE table tfidf
select OwnerUserId, tf.word, tf.termFreq * idf.idf as tfidf 
from tf 
join idf 
on tf.word = idf.word;


------- select top 10 words for each top 10 userId (sorted by score)
select * from (
select OwnerUserId, word, tfidf, rank() over(partition by OwnerUserId order by tfidf desc) as rn 
from tfidf as T
where T.OwnerUserId in (
 select cast(OwnerUserId as int) from (
 select OwnerUserId, sum(cast(score as int)) as s 
from posts 
where OwnerUserId != "" 
group by OwnerUserId 
order by s desc 
limit 10
) as B 
)
) as A
where A.rn <= 10;



--- top 10 OwnerUserId
-- 87234
-- 4883
-- 9951
-- 6068
-- 89904.0
-- 51816.0
-- 49153.0
-- 179736.0
-- 95592.0
-- 63051.0
