---------- create posts table 
create external table if not exists posts (Id int,
       PostTypeId int,
      AcceptedAnswerId int,
      ParentID int, 
       CreationDate String,
      DeletionDate String,
       Score int,
       ViewCount int,
       Body String,
       OwnerUserId int,
      OwnerDisplayName String,
       LastEditorUserId int,
       LastEditorDisplayName String,
       LastEditDate String,
       LastActivityDate String,
       Title String,
       Tags String,
       AnswerCount int,
       CommentCount int,
       FavoriteCount int,
      ClosedDate String, 
     CommunityOwnedDate String, 
     ContentLicense String
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

-------- Load data from local to hive table 
---- This file will be upload to hdfs at /user/hive/warehouse/posts
LOAD DATA LOCAL INPATH '/home/minhnhat/Downloads/Master_In_Ireland/Semester_1/CA675_Cloud_Technologies/clean-data/QueryResults-128k-49479-clean.csv'
INTO TABLE posts;


-- Task 2.2.1. The top 10 posts by score
select id, cast(score as int), viewCount, title 
from posts 
order by cast(score as int) desc 
limit 10;

--- 2.2.2. The top 10 users by total post score 
select OwnerUserId, sum(cast(score as int)) as s 
from posts 
where OwnerUserId != "" 
group by OwnerUserId order by s desc 
limit 10;

-- 2.2.3. The number of distinct users, who used the word “cloud” in one of their Posts 
select count(distinct(OwnerUserId))
from posts 
lateral view explode(split(concat(title," ", body), ' |,')) lateralTable as word 
where word = "cloud"
group by word;

