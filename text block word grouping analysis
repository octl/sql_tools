/* 
This code was originally used to analyze comments on customer accounts but can be adapted for any text field.
The code indexes each word in a block of text and ranks which words are used most often in conjunction with eachother.
This can help summarize and find patterns across a large set of text blocks and can be grouped by any attribute by which the text blocks are organized.
It utilizes cross joins as opposed to loops to self compare text blocks all within SQL. 
Code has been adapted from working code to pseudocode protect original confidential data architecture.
*/ 

Define customerlevelfilters = --populate with customer attributes of interest 
Define commentlevelfilters = —populate with comment attributes of interest

/* Select customers' comments of interest*/

WITH one AS 
( 
         SELECT   * 
         FROM     fakedatawarehouse.customer_comment_table 
         WHERE 
                  &&customerlevelfilters 
         AND   &&commentlevelfilters
         ORDER BY seq_nbr ) 

/*Aggregate broken lines into one field*/
, two AS 

( 
         SELECT   customer_attribute, 
                  comment_attribute, 
                  Listagg(comment_line, ' ') within GROUP (ORDER BY seq_nbr) comments 
         FROM     one 
         GROUP BY comment_attribute, 
                  customer_attribute ) , 

/*Format out problematic special characters*/
three AS 
( 
       SELECT customer_attribute, 
              comment_attribute, 
              Replace(Replace(Replace(Replace(Replace(comments,',',''),'.',''),'/',' '),':',''),Chr(39),'') comments
       FROM   two ) , 


/*Split out each word into a separate row and index*/
four AS 
( 
       SELECT *
       FROM   ( 
                     SELECT customer_attribute, 
                            comment_attribute, 
                            Regexp_substr (comments, '[^[:space:]]+', 1, ROWNUM) split, 
                            ROWNUM                                               w_idx 
                     FROM   three 
                            CONNECT BY LEVEL <= Length (Regexp_replace (comments, '[^[:space:]]+')) + 1 )
       WHERE  split IS NOT NULL 
          
) , 

/*Self join indexed words for cartesian product twice, excluding the same word position with itself. Ends up with all permutations of 3 words*/

five AS 
( 
          SELECT    f.customer_attribute, 
                    f.comment_attribute, 
                    f.split   AS word1, 
                    f.w_idx   AS index1, 
                    ff.split  AS word2, 
                    ff.w_idx  AS index2, 
                    fff.split AS word3, 
                    fff.w_idx AS index3 
          FROM      four f 
          left join four ff 
          ON        f.customer_attribute = ff.customer_attribute 
          AND       f.comment_attribute= ff.comment_attribute 
          AND       f.w_idx <> ff.w_idx 
          left join four fff 
          ON        f.customer_attribute = fff.customer_attribute 
          AND       f.comment_attribute= fff.comment_attribute 
          AND       ff.customer_attribute = fff.customer_attribute 
          AND       ff.comment_attribute= fff.comment_attribute 
          AND       f.w_idx <> fff.w_idx 
          AND       ff.w_idx <> fff.w_idx ) , 


/* Uses Greatest and Least to aggregate permutations into unique word combinations in alphabetical order.
Uses Greatest and Least on word position to add the absolute distance between the first and second and second and third words to calculate total proximity of word combinations.
Create a new index from all word positions.
*/

six AS 
( 
         SELECT   customer_attribute, 
                  comment_attribute, 
                  Greatest(word1,word2,word3) 
                           ||' ' 
                           ||Greatest ( Least (word1, word2) , Least (word2, word3) , Least (word1, word3) )
                           ||' ' 
                           ||Least(word1,word2,word3)                                                                                                                                                                                                        AS Word_Group,
                  Abs(Greatest ( Least (index1, index2) , Least (index2, index3) , Least (index1, index3) ) - Greatest(index1,index2,index3) ) + Abs(Greatest ( Least (index1, index2) , Least (index2, index3) , Least (index1, index3) ) - Least(index1,index2,index3) ) AS Proximity,
                  index1+index2+index3                                                                                                                                                                                                        AS indexindex
         FROM     five 
         GROUP BY customer_attribute, 
                  comment_attribute, 
                  Greatest(word1,word2,word3) 
                           ||' ' 
                           ||Greatest ( Least (word1, word2) , Least (word2, word3) , Least (word1, word3) )
                           ||' ' 
                           ||Least(word1,word2,word3) , 
                  Abs(Greatest ( Least (index1, index2) , Least (index2, index3) , Least (index1, index3) ) - Greatest(index1,index2,index3) ) + Abs(Greatest ( Least (index1, index2) , Least (index2, index3) , Least (index1, index3) ) - Least(index1,index2,index3) ) ,
                  index1+index2+index3 ) 


/* 
Aggregate proximity by word combinations. Calculate strength as number of instances / (average proximity/ highest proximity within analytical grouping). 
Since average is of each combination and the max is of each analytical grouping, the avg/max would give a sense of where a particular word combination stands within an analytical grouping. 
The count of number of combinations gives a sense of how often a combination occurs, so strength weights it by avg/max, so applies the distribution of proximities. 
*/ 
,seven AS 
( 
         SELECT   customer_attribute, 
                  comment_attribute, 
                  word_group, 
                  Max(Max(proximity)) over (PARTITION BY customer_attribute, comment_attribute) AS maxi,
                  Avg(proximity), 
                  Count(DISTINCT indexindex)                                                                                                 AS instance_count,
                  Count(DISTINCT indexindex)/(Avg(proximity)/Max(Max(proximity)) over (PARTITION BY customer_attribute, comment_attribute))    strength
         FROM     six 
         GROUP BY customer_attribute, 
                  comment_attribute, 
                  word_group) 
SELECT * 
FROM   seven
