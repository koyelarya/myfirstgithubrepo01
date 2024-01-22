import pyspark
from pyspark.sql import SparkSession, DataFrame
import os
import shutil
from re import search

spark= SparkSession.builder \
    .master("local") \
    .appName("ReadFlatFile") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def create_custom_csv(df: DataFrame, path, fname):
   print('\nCreating csv in Temporary path..')
   df.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{path}/temp/{fname}")
   print('\nFile created in Temporary path!!\nShifting it to given path..')

   flist=os.listdir(f"{path}/temp/{fname}")
   for f in flist:
       if search("part-",f):
           part_filename=f

   if os.path.isfile(f"{path}/{fname}.csv"):
       os.remove(f"{path}/{fname}.csv")

   shutil.copy(f"{path}/temp/{fname}/{part_filename}",f"{path}/{fname}.csv")
   print("\nFinal file created!!\nRemoving temporary file..")

   shutil.rmtree(f"{path}/temp/{fname}")
   print("\nTemporary file removed!!\n\n")


df_users=spark.read.csv('./data/users.dat', sep="::", schema="userID int, Gender string, Age int, Occupation int, ZipCode string")
df_users.createOrReplaceTempView("temp_users_vw")

df_ratings=spark.read.csv('./data/ratings.dat', sep="::", schema="UserID int, MovieID int, Rating int, Timestamp long")
df_ratings.createOrReplaceTempView("temp_ratings_vw")

df_movies=spark.read.csv('./data/movies.dat', sep="::", schema="MovieID int, Title string, Genres string")
df_movies.createOrReplaceTempView("temp_movies_vw")

v_users_count=df_users.count()

print('\nUsers data...',
      '\nNo of total users: ',v_users_count,
      '\nMinimum userID (should be 1): ',spark.sql("select min(userID) id from temp_users_vw").take(1)[0][0],
      '\nmaximum userID (should be 6040): ',spark.sql("select max(userID) id from temp_users_vw""").take(1)[0][0],
      '\nNo of User records with incorrect Gender code (not F or M): ',spark.sql("select count(*) from temp_users_vw where nvl(gender,'') not in ('F','M')").take(1)[0][0],
      '\nNo of User records with incorrect Age groups (anything else than 1,18,25,35,45,50,56): ',spark.sql("select count(*) from temp_users_vw where nvl(Age,-999) not in (1,18,25,35,45,50,56)").take(1)[0][0],
      '\nNo of User records with incorrect Occupation Codes (outside of range 0 to 20): ',spark.sql("select count(*) from temp_users_vw where nvl(Occupation,-999) < 0 or  nvl(Occupation,-999)>20").take(1)[0][0],
      '\nRedundant entries (not present in ratings file): ',spark.sql("select u.* from temp_users_vw u left join temp_ratings_vw r on u.userid=r.userid where r.userid is null").count(),'\n')

print('\nSaving User data with headers as CSV in outbound directory..')
create_custom_csv(df_users,"./outbound","Users")

v_redundant_movies=spark.sql("""select m.* from temp_movies_vw m left join temp_ratings_vw r on m.MovieID=r.MovieID where r.movieID is null""").count()
v_movies_count=df_movies.count()

#Movies data sanity check - Check total count is 3952
print('\nMovies data...\nNo of total movies: ',v_movies_count,
      '\nMinimum movieID(should be 1): ',spark.sql("""select min(movieID) from temp_movies_vw""").take(1)[0][0],
      '\nMaximum movieID(should be 3952): ',spark.sql("""select max(movieID) from temp_movies_vw""").take(1)[0][0],
      '\nDuplicate entries (based on MovieID): ',spark.sql("""select movieID from temp_movies_vw group by movieID having count(*)>1""").count(),
      '\nDuplicate entries (based on Title): ',spark.sql("""select lower(trim(title)) from temp_movies_vw group by lower(trim(title)) having count(*)>1""").count(),
      '\nRedundant entries (not present in Ratings file): ',v_redundant_movies,'\n')

print('\nSaving Movies data with headers as CSV in outbound directory..')
create_custom_csv(df_movies,"./outbound","Movies")

#Ratings data sanity check
print('\nRating data..',
      '\nTotal records: ',df_ratings.count(),
      '\nNo of users with less than 20 ratings (should be 0): ',spark.sql("""select userid from temp_ratings_vw group by userid having count(*)<20 """).count(),
      '\nNo of distinct users in the file: ',spark.sql("""select count(distinct userid) from temp_ratings_vw""").take(1)[0][0],
      '\nNo of distinct Movies in the file: ',spark.sql("""select count(distinct movieID) from temp_ratings_vw""").take(1)[0][0],
      '\nNo of incorrect(other than 0 to 5) or blank ratings (should be 0): ',spark.sql("""select * from temp_ratings_vw where rating not in (0,1,2,3,4,5) or rating is null""").count(),
      '\n')

print('\nSaving Ratings data with headers as CSV in outbound directory..')
create_custom_csv(df_ratings,"./outbound","Ratings")

print('\nCreating new dataframe df_movies_rating for Movies & Rating..\n')
df_movies_rating=spark.sql("""select m.movieID, trim(m.title) Title, trim(genres) Genres, max(rating) maxRating, min(rating) minRating, avg(rating) avgRating from temp_movies_vw m 
                        inner join temp_ratings_vw r on m.MovieID=r.MovieID /*inner join to remove redundant movie entries without rating*/
                        group by m.movieID, m.title, m.genres""")

df_movies_rating.printSchema()

print('\n df_movies_rating unit tests..')

if v_movies_count-v_redundant_movies==df_movies_rating.count():
    print('\nUT1: Check total count is: ',v_movies_count-v_redundant_movies,' --- PASS')
else:
    print('\nUT1: Check total count is: ',v_movies_count-v_redundant_movies,' --- FAIL')

if (df_movies_rating.filter(df_movies_rating["maxRating"].isNull() | df_movies_rating["minRating"].isNull() | df_movies_rating["avgRating"].isNull() | df_movies_rating["Title"].isNull() | df_movies_rating["Genres"].isNull()).count())==0:
    print('\nUT2: 0 records with any NULL column: --- PASS','\n')
else:
    print('\nUT2: 0 records with any NULL column: --- FAIL','\n')

print('\nwriting df_movies_rating into csv file ..')
create_custom_csv(df_movies_rating,"./outbound","Movies_with_Rating")

print('\nCreating new dataframe df_user_rating_movies for User wise top 3 movies(based on rating).. \n')
df_user_rating_movies=spark.sql("""select userID,movieID, rating from
                        (select r.userid,r.movieid,r.rating, row_number() over(partition by userid order by rating desc) rnk 
                        from temp_ratings_vw r)
                        where rnk<=3""")
df_user_rating_movies.printSchema()
df_user_rating_movies.createOrReplaceTempView("userRating_vw")

print('\ndf_user_rating_movies unit tests..')
if v_users_count*3==df_user_rating_movies.count():
    print('\nUT1: check total count is : ',v_users_count*3,' --- PASS')
else:
    print('\nUT1: check total count is : ',v_users_count*3,' --- FAIL')

if spark.sql("select userid from userRating_vw group by userid having count(*)<>3").count()==0:
    print('UT2: check all user entries have 3 entries each --- PASS')
else:
    print('UT2: check all user entries have 3 entries each --- FAIL')

if spark.sql("select userid from userRating_vw group by userid having count(distinct movieid)<>3").count()==0:
    print('UT3: check all user entries have 3 distinct movies each --- PASS\n')
else:
    print('UT3: check all user entries have 3 distinct movies each --- FAIL\n')

print('\nwriting df_user_rating_movies into csv file ..')
create_custom_csv(df_user_rating_movies,"./outbound","Userwise_Top_3_Movies")

shutil.rmtree("./outbound/temp")
print('\nTemporary folder removed!!\n\nAll old and new dataframes are saved in "outbound" directory!!\nScript execution Completed!!')